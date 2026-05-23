package info.elect.spark.catalog;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.connector.catalog.Column;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.V1Table;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Extends PatchedUCSingleCatalog to honor DELTA as an explicit table format
 * and to bootstrap the Delta transaction log so saveAsTable's partition check
 * sees a real Snapshot (not Delta's DummySnapshot for empty paths).
 *
 * PatchedUCSingleCatalog forces all tables to the session default (parquet)
 * regardless of the requested provider. This subclass intercepts createTable
 * calls where provider=delta and temporarily sets spark.sql.sources.default
 * to "delta" before delegating to super, so the parent's getDefaultProvider()
 * returns "delta" and the UC REST API registers the table as DELTA format.
 *
 * After super.createTable succeeds, this subclass also writes an initial
 * _delta_log/00000000000000000000.json at the storage location via Delta's
 * DeltaTable.createIfNotExists() API. Without this bootstrap, Delta's
 * DeltaLog.update() finds an empty directory and returns a DummySnapshot
 * with empty partitionColumns. Spark's DataFrameWriter.checkPartitioningMatchesV2Table
 * then reads partitions from the DummySnapshot (not from PatchedUCSingleCatalog's
 * TruncatableV1Table.overridePartitioning injection) and fails with:
 *   requirement failed: The provided partitioning or clustering columns
 *   do not match the existing table's.
 *    - provided: identity(cycle), identity(year_month)
 *    - table:
 * because the table-side partitioning is empty. See electinfo/spark-on-kubernetes#19
 * for the originating arc (cycles 8, 10, 11 of electinfo/enterprise#2094).
 *
 * Use this catalog for pipelines that require Delta tables:
 *   spark.sql.catalog.spark_catalog = info.elect.spark.catalog.PatchedUCSingleCatalogDelta
 *
 * Pipelines that do not need Delta should continue using PatchedUCSingleCatalog
 * to avoid any risk of regression on Steve's production pipelines.
 */
public class PatchedUCSingleCatalogDelta extends PatchedUCSingleCatalog {

    private static final String PROVIDER_KEY = "provider";
    private static final String DELTA = "delta";
    private static final String SOURCES_DEFAULT = "spark.sql.sources.default";

    @Override
    @SuppressWarnings("deprecation")
    public Table createTable(Identifier ident, Column[] columns, Transform[] partitions,
                             Map<String, String> properties) {
        if (isDeltaRequested(properties)) {
            System.out.println("PatchedUCSingleCatalogDelta: createTable " + ident
                + " honoring provider delta");
            return withDeltaDefault(() -> {
                Table created = super.createTable(ident, columns, partitions, properties);
                bootstrapDeltaLog(created, columnsToStructType(columns), partitions);
                return created;
            });
        }
        return super.createTable(ident, columns, partitions, properties);
    }

    @Override
    @SuppressWarnings("deprecation")
    public Table createTable(Identifier ident, StructType schema, Transform[] partitions,
                             Map<String, String> properties) {
        if (isDeltaRequested(properties)) {
            System.out.println("PatchedUCSingleCatalogDelta: createTable " + ident
                + " honoring provider delta");
            return withDeltaDefault(() -> {
                Table created = super.createTable(ident, schema, partitions, properties);
                bootstrapDeltaLog(created, schema, partitions);
                return created;
            });
        }
        return super.createTable(ident, schema, partitions, properties);
    }

    private boolean isDeltaRequested(Map<String, String> properties) {
        return properties != null && DELTA.equalsIgnoreCase(properties.get(PROVIDER_KEY));
    }

    /**
     * Temporarily sets spark.sql.sources.default=delta so the parent's
     * getDefaultProvider() returns "delta", then restores the original value.
     * This is the only way to influence the parent's provider selection without
     * duplicating private helper logic (createExternalTable, columnsToJson, etc.).
     */
    private Table withDeltaDefault(Supplier<Table> action) {
        SparkSession spark = SparkSession.active();
        String original;
        try {
            original = spark.conf().get(SOURCES_DEFAULT);
        } catch (Exception e) {
            original = "parquet";
        }
        try {
            spark.conf().set(SOURCES_DEFAULT, DELTA);
            return action.get();
        } finally {
            spark.conf().set(SOURCES_DEFAULT, original);
        }
    }

    /**
     * Bootstrap the Delta transaction log at the storage location with the
     * requested schema + partition columns. Must run AFTER super.createTable
     * because PatchedUCSingleCatalog.createExternalTable does fs.delete(tablePath, true)
     * before posting to the UC REST API; any pre-existing _delta_log would be
     * wiped. By running after, we initialize a fresh _delta_log alongside the
     * fresh UC entry.
     *
     * Uses DeltaTable.createIfNotExists semantics: if the path somehow already
     * has a Delta table (race condition, recovery scenario), no-op. Otherwise
     * write _delta_log/00000000000000000000.json with the schema and
     * partitionColumns. Spark's checkPartitioningMatchesV2Table will then read
     * the real Snapshot's partitionColumns instead of DummySnapshot's empty list.
     */
    private void bootstrapDeltaLog(Table created, StructType schema, Transform[] partitions) {
        if (!(created instanceof V1Table)) {
            System.err.println("WARN: PatchedUCSingleCatalogDelta.bootstrapDeltaLog: "
                + "created table is not V1Table (got " + created.getClass().getName()
                + "); skipping _delta_log init");
            return;
        }
        V1Table v1 = (V1Table) created;
        CatalogTable ct = v1.v1Table();
        if (ct.storage().locationUri().isEmpty()) {
            System.err.println("WARN: PatchedUCSingleCatalogDelta.bootstrapDeltaLog: "
                + "CatalogTable has no locationUri; skipping _delta_log init");
            return;
        }
        String location = ct.storage().locationUri().get().toString();
        List<String> partCols = extractPartitionColNames(partitions);

        try {
            SparkSession spark = SparkSession.active();
            io.delta.tables.DeltaTable.Builder builder = io.delta.tables.DeltaTable
                .createIfNotExists(spark)
                .location(location)
                .addColumns(schema);
            if (!partCols.isEmpty()) {
                builder = builder.partitionedBy(partCols.toArray(new String[0]));
            }
            builder.execute();
            System.out.println("PatchedUCSingleCatalogDelta: bootstrapped _delta_log at "
                + location + " (partitionColumns=" + partCols + ")");
        } catch (Exception e) {
            // Non-fatal: the subsequent saveAsTable will fail with a clearer error
            // if the bootstrap was actually required and didn't run. Logging here
            // surfaces the failure mode without masking the downstream symptom.
            System.err.println("WARN: PatchedUCSingleCatalogDelta.bootstrapDeltaLog: "
                + "failed to init _delta_log at " + location
                + " (" + e.getClass().getSimpleName() + ": " + e.getMessage() + ")");
        }
    }

    /**
     * Convert Spark V2 Column[] to a StructType for DeltaTableBuilder.addColumns().
     * Mirrors PatchedUCSingleCatalog's column conversion shape; uses empty Metadata
     * (UC connector v0.4.0 doesn't surface field-level metadata through Column).
     */
    private StructType columnsToStructType(Column[] columns) {
        StructField[] fields = new StructField[columns.length];
        for (int i = 0; i < columns.length; i++) {
            Column c = columns[i];
            fields[i] = new StructField(c.name(), c.dataType(), c.nullable(), Metadata.empty());
        }
        return new StructType(fields);
    }

    /**
     * Extract partition column names from DLP's Transform[] array. Mirrors
     * PatchedUCSingleCatalog's private extractPartitionColNames (cannot reuse
     * directly because the super method is private; the logic is small enough
     * to duplicate without abstraction debt).
     */
    private static List<String> extractPartitionColNames(Transform[] partitions) {
        List<String> names = new ArrayList<>();
        if (partitions != null) {
            for (Transform t : partitions) {
                try {
                    NamedReference[] refs = t.references();
                    if (refs != null && refs.length > 0) {
                        String[] fieldNames = refs[0].fieldNames();
                        if (fieldNames != null && fieldNames.length > 0) {
                            names.add(fieldNames[0]);
                        }
                    }
                } catch (Exception e) {
                    // ignore malformed transforms
                }
            }
        }
        return names;
    }
}
