package info.elect.spark.catalog;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.connector.catalog.Column;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.StagedTable;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.V1Table;
import org.apache.spark.sql.connector.expressions.NamedReference;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
        boolean explicitDelta = isDeltaRequested(properties);
        boolean effectiveDelta = explicitDelta || isSessionDeltaDefault();
        System.out.println("PatchedUCSingleCatalogDelta: createTable " + ident
            + " explicitDelta=" + explicitDelta + " effectiveDelta=" + effectiveDelta);
        Table created;
        if (explicitDelta) {
            created = withDeltaDefault(() -> super.createTable(ident, columns, partitions, properties));
        } else {
            created = super.createTable(ident, columns, partitions, properties);
        }
        if (effectiveDelta) {
            bootstrapDeltaLog(created, columnsToStructType(columns), partitions);
        }
        return created;
    }

    @Override
    @SuppressWarnings("deprecation")
    public Table createTable(Identifier ident, StructType schema, Transform[] partitions,
                             Map<String, String> properties) {
        boolean explicitDelta = isDeltaRequested(properties);
        boolean effectiveDelta = explicitDelta || isSessionDeltaDefault();
        System.out.println("PatchedUCSingleCatalogDelta: createTable " + ident
            + " explicitDelta=" + explicitDelta + " effectiveDelta=" + effectiveDelta);
        Table created;
        if (explicitDelta) {
            created = withDeltaDefault(() -> super.createTable(ident, schema, partitions, properties));
        } else {
            created = super.createTable(ident, schema, partitions, properties);
        }
        if (effectiveDelta) {
            bootstrapDeltaLog(created, schema, partitions);
        }
        return created;
    }

    private boolean isDeltaRequested(Map<String, String> properties) {
        return properties != null && DELTA.equalsIgnoreCase(properties.get(PROVIDER_KEY));
    }

    /**
     * Returns true if spark.sql.sources.default is "delta" in the active session.
     * DLT does not pass provider=delta in createTable properties; it relies on
     * sources.default (set in the pipeline yaml). We must check both surfaces.
     */
    private boolean isSessionDeltaDefault() {
        try {
            return DELTA.equalsIgnoreCase(SparkSession.active().conf().get(SOURCES_DEFAULT));
        } catch (Exception e) {
            return false;
        }
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
        String location = extractLocation(created);
        if (location == null) {
            System.err.println("WARN: PatchedUCSingleCatalogDelta.bootstrapDeltaLog: "
                + "could not extract storage location from " + created.getClass().getName()
                + "; skipping _delta_log init");
            return;
        }
        List<String> partCols = extractPartitionColNames(partitions);

        try {
            SparkSession spark = SparkSession.active();
            io.delta.tables.DeltaTableBuilder builder = io.delta.tables.DeltaTable
                .createIfNotExists(spark)
                .location(location)
                .addColumns(schema);
            if (!partCols.isEmpty()) {
                builder = builder.partitionedBy(partCols.toArray(new String[0]));
            }
            builder.execute();
            System.out.println("PatchedUCSingleCatalogDelta: bootstrapped _delta_log at "
                + location + " (partitionColumns=" + partCols + ")");
            // Invalidate any cached DeltaLog snapshot for this path. Without this,
            // a DeltaTableV2 instance returned earlier from super.createTable would
            // continue to expose its cached DummySnapshot (empty partitionColumns)
            // when Spark's saveAsTable checks .partitioning(). Calling
            // DeltaLog.forTable(...).update() forces a fresh read from the
            // _delta_log we just wrote.
            try {
                io.delta.tables.DeltaTable.forPath(spark, location).toDF();
            } catch (Exception ignored) {
                // best-effort cache refresh; if forPath fails, the next read will re-init
            }
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
     * Extract storage location from a Table returned by super.createTable.
     *
     * Two cases:
     * - V1Table (default): location is in CatalogTable.storage.locationUri.
     * - DeltaTableV2 (Delta extension intercepts when sources.default=delta): expose
     *   the storage path via a public `path()` method. Use reflection to avoid a
     *   compile-time dependency on org.apache.spark.sql.delta.catalog.DeltaTableV2
     *   (which is in delta-spark, present at runtime but pulled via a separate
     *   classpath in the pom).
     */
    private String extractLocation(Table created) {
        if (created instanceof V1Table) {
            V1Table v1 = (V1Table) created;
            if (!v1.v1Table().storage().locationUri().isEmpty()) {
                return v1.v1Table().storage().locationUri().get().toString();
            }
        }
        try {
            java.lang.reflect.Method pathMethod = created.getClass().getMethod("path");
            Object pathObj = pathMethod.invoke(created);
            if (pathObj != null) {
                return pathObj.toString();
            }
        } catch (NoSuchMethodException nsme) {
            // not a Delta table; ignore
        } catch (Exception e) {
            System.err.println("WARN: PatchedUCSingleCatalogDelta.extractLocation: "
                + "reflection on " + created.getClass().getName() + ".path() failed: "
                + e.getMessage());
        }
        return null;
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

    // --- Staging overrides (Delta-aware) ---
    //
    // PatchedUCSingleCatalog.stageCreate / stageReplace / stageCreateOrReplace all
    // funnel into wrapAsStagedTable, which throws UnsupportedOperationException for
    // anything that isn't a V1Table. When this subclass's createTable returns a
    // DeltaTableV2 (the Delta SQL extension intercepts UCSingleCatalog.loadTable
    // when sources.default=delta), the parent's wrapAsStagedTable rejects it and
    // V2 CTAS paths like df.write.format("delta").saveAsTable(...) fail with:
    //     Cannot stage non-V1 table: org.apache.spark.sql.delta.catalog.DeltaTableV2
    //
    // Override all six staging entry points so they call this subclass's
    // createTable (which bootstraps _delta_log) and wrap the returned Table in a
    // Delta-aware StagedTable. The wrapper uses Java dynamic proxy so it picks up
    // every interface the wrapped table implements (SupportsWrite, SupportsRead,
    // and Delta-specific extension interfaces) without a compile-time dep on
    // DeltaTableV2. Spark's V2 write code dispatches via these interfaces, so
    // instanceof checks still succeed. See electinfo/spark-on-kubernetes#19.

    @Override
    public StagedTable stageCreate(Identifier ident, Column[] columns,
            Transform[] partitions, Map<String, String> properties) {
        Table table = createTable(ident, columns, partitions, properties);
        return wrapAsDeltaAwareStagedTable(table, ident);
    }

    @Override
    @SuppressWarnings("deprecation")
    public StagedTable stageCreate(Identifier ident, StructType schema,
            Transform[] partitions, Map<String, String> properties) {
        Table table = createTable(ident, schema, partitions, properties);
        return wrapAsDeltaAwareStagedTable(table, ident);
    }

    @Override
    public StagedTable stageReplace(Identifier ident, Column[] columns,
            Transform[] partitions, Map<String, String> properties) {
        try { dropTable(ident); } catch (Exception ignored) { }
        Table table = createTable(ident, columns, partitions, properties);
        return wrapAsDeltaAwareStagedTable(table, ident);
    }

    @Override
    @SuppressWarnings("deprecation")
    public StagedTable stageReplace(Identifier ident, StructType schema,
            Transform[] partitions, Map<String, String> properties) {
        try { dropTable(ident); } catch (Exception ignored) { }
        Table table = createTable(ident, schema, partitions, properties);
        return wrapAsDeltaAwareStagedTable(table, ident);
    }

    @Override
    public StagedTable stageCreateOrReplace(Identifier ident, Column[] columns,
            Transform[] partitions, Map<String, String> properties) {
        try { dropTable(ident); } catch (Exception ignored) { }
        Table table = createTable(ident, columns, partitions, properties);
        return wrapAsDeltaAwareStagedTable(table, ident);
    }

    @Override
    @SuppressWarnings("deprecation")
    public StagedTable stageCreateOrReplace(Identifier ident, StructType schema,
            Transform[] partitions, Map<String, String> properties) {
        try { dropTable(ident); } catch (Exception ignored) { }
        Table table = createTable(ident, schema, partitions, properties);
        return wrapAsDeltaAwareStagedTable(table, ident);
    }

    /**
     * Build a StagedTable proxy that delegates Table-interface methods to the
     * wrapped table (DeltaTableV2 or anything else returned by createTable) and
     * implements commitStagedChanges as a no-op + abortStagedChanges as a UC
     * drop.
     *
     * Uses a dynamic proxy over the union of {StagedTable} and every interface
     * the wrapped table implements (walked transitively up the class hierarchy).
     * This means SupportsWrite, SupportsRead, and any Delta-specific interfaces
     * remain visible to Spark's V2 write code without a compile-time dep on
     * DeltaTableV2. The proxy is NOT an instance of the wrapped table's concrete
     * class; that's acceptable because Spark's StagingTableCatalog contract only
     * promises a StagedTable, and Spark's V2 write path dispatches via interface
     * methods rather than concrete-class casts.
     */
    private StagedTable wrapAsDeltaAwareStagedTable(Table table, Identifier ident) {
        Set<Class<?>> interfaces = new LinkedHashSet<>();
        collectInterfaces(table.getClass(), interfaces);
        interfaces.add(StagedTable.class);
        Class<?>[] proxyInterfaces = interfaces.toArray(new Class<?>[0]);

        InvocationHandler handler = new StagedTableInvocationHandler(table, ident);
        ClassLoader loader = table.getClass().getClassLoader();
        return (StagedTable) Proxy.newProxyInstance(loader, proxyInterfaces, handler);
    }

    /**
     * Walk the class + superclass + interface chain of cls and add every
     * interface encountered to out. Required because table.getClass().getInterfaces()
     * only returns DIRECT interfaces; Delta's DeltaTableV2 extends V2TableWithV1Fallback
     * which implements many interfaces transitively.
     */
    private static void collectInterfaces(Class<?> cls, Set<Class<?>> out) {
        while (cls != null) {
            for (Class<?> iface : cls.getInterfaces()) {
                if (out.add(iface)) {
                    collectInterfaces(iface, out);
                }
            }
            cls = cls.getSuperclass();
        }
    }

    /**
     * Inner handler so the InvocationHandler isn't a closure-style lambda; gives
     * abortStagedChanges access to the catalog via the enclosing instance for
     * dropTable, and a clean stack trace if anything in the proxy chain throws.
     */
    private class StagedTableInvocationHandler implements InvocationHandler {
        private final Table wrapped;
        private final Identifier ident;

        StagedTableInvocationHandler(Table wrapped, Identifier ident) {
            this.wrapped = wrapped;
            this.ident = ident;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            String name = method.getName();
            if ("commitStagedChanges".equals(name)) {
                // Data is already written to S3 (PatchedUCSingleCatalog uses the
                // eager-create-then-write idiom) and UC metadata is in place from
                // createTable. No-op matches the SimpleStagedTable semantics for
                // V1Table.
                return null;
            }
            if ("abortStagedChanges".equals(name)) {
                // On failure, drop the UC entry we just created. dropTable is the
                // public CatalogPlugin method; package-private deleteTable in the
                // parent is inaccessible from this subclass.
                try {
                    PatchedUCSingleCatalogDelta.this.dropTable(ident);
                } catch (Exception ignored) {
                    // best-effort cleanup
                }
                return null;
            }
            try {
                return method.invoke(wrapped, args);
            } catch (java.lang.reflect.InvocationTargetException ite) {
                throw ite.getCause();
            }
        }
    }
}
