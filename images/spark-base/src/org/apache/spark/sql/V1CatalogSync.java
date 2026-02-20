package org.apache.spark.sql;

import org.apache.spark.sql.catalyst.catalog.CatalogDatabase;
import org.apache.spark.sql.catalyst.catalog.SessionCatalog;

import java.net.URI;

/**
 * Syncs V2 catalog namespaces to the V1 SessionCatalog.
 *
 * When spark_catalog is mapped to a V2 CatalogPlugin (e.g. PatchedUCSingleCatalog),
 * SQL commands like CREATE DATABASE route through V2, bypassing the V1 SessionCatalog.
 * However, CreateDataSourceTableAsSelectCommand (used for parquet/csv DataSource tables)
 * validates databases via V1's SessionCatalog.requireDbExists(). This causes
 * SCHEMA_NOT_FOUND errors for schemas that exist in V2 but not V1.
 *
 * This class bridges the gap by creating databases in the V1 InMemoryCatalog
 * to match what exists in the V2 catalog. It must be in the org.apache.spark.sql
 * package to access the package-private sessionState() method.
 */
public class V1CatalogSync {

    /**
     * Ensure a schema exists in the V1 SessionCatalog (InMemoryCatalog).
     * Creates it if it doesn't exist. Safe to call multiple times.
     *
     * @param schemaName the schema name (e.g. "bronze")
     */
    public static void ensureSchemaExists(String schemaName) {
        try {
            SparkSession spark = SparkSession.active();
            SessionCatalog v1Catalog = spark.sessionState().catalog();
            if (!v1Catalog.databaseExists(schemaName)) {
                URI dbPath;
                try {
                    dbPath = v1Catalog.getDefaultDBPath(schemaName);
                } catch (Exception e) {
                    dbPath = new URI("file:///tmp/spark-warehouse/" + schemaName);
                }
                CatalogDatabase db = new CatalogDatabase(
                    schemaName,
                    "Auto-synced from V2 catalog",
                    dbPath,
                    scala.collection.immutable.Map$.MODULE$.<String, String>empty()
                );
                v1Catalog.createDatabase(db, true);
                System.out.println("V1CatalogSync: Created V1 schema '" + schemaName + "'");
            }
        } catch (Exception e) {
            System.err.println("WARN: V1CatalogSync.ensureSchemaExists('" +
                schemaName + "'): " + e.getMessage());
        }
    }

    /**
     * Ensure a schema exists in the V1 SessionCatalog with a specific storage root.
     *
     * Sets the V1 database location to the UC schema storage root so that V1 table
     * paths ({dbLocation}/{tableName}) match UC table paths ({storageRoot}/{tableName}).
     * This is critical for DLP: materialized views write data via the V1 catalog path,
     * and reanalyzeFlow reads via the V2 catalog (UC). Both must point to the same location.
     *
     * @param schemaName  the schema name (e.g. "gold")
     * @param storageRoot the UC schema storage root (e.g. "s3://hive-warehouse/gold")
     */
    public static void ensureSchemaExists(String schemaName, String storageRoot) {
        try {
            SparkSession spark = SparkSession.active();
            SessionCatalog v1Catalog = spark.sessionState().catalog();
            if (!v1Catalog.databaseExists(schemaName)) {
                URI dbPath;
                try {
                    dbPath = new URI(storageRoot);
                } catch (Exception e) {
                    dbPath = v1Catalog.getDefaultDBPath(schemaName);
                }
                CatalogDatabase db = new CatalogDatabase(
                    schemaName,
                    "Auto-synced from V2 catalog (storage: " + storageRoot + ")",
                    dbPath,
                    scala.collection.immutable.Map$.MODULE$.<String, String>empty()
                );
                v1Catalog.createDatabase(db, true);
                System.out.println("V1CatalogSync: Created V1 schema '" + schemaName +
                    "' at " + dbPath);
            }
        } catch (Exception e) {
            System.err.println("WARN: V1CatalogSync.ensureSchemaExists('" +
                schemaName + "', '" + storageRoot + "'): " + e.getMessage());
        }
    }
}
