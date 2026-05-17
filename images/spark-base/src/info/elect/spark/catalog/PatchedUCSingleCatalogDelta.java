package info.elect.spark.catalog;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Column;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;

import java.util.Map;
import java.util.function.Supplier;

/**
 * Extends PatchedUCSingleCatalog to honor DELTA as an explicit table format.
 *
 * PatchedUCSingleCatalog forces all tables to the session default (parquet)
 * regardless of the requested provider. This subclass intercepts createTable
 * calls where provider=delta and temporarily sets spark.sql.sources.default
 * to "delta" before delegating to super, so the parent's getDefaultProvider()
 * returns "delta" and the UC REST API registers the table as DELTA format.
 * All other providers continue through the parent's override logic unchanged.
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
            return withDeltaDefault(
                () -> super.createTable(ident, columns, partitions, properties));
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
            return withDeltaDefault(
                () -> super.createTable(ident, schema, partitions, properties));
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
}
