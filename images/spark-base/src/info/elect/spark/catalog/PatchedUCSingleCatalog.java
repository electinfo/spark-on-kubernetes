package info.elect.spark.catalog;

import io.unitycatalog.spark.UCSingleCatalog;
import org.apache.spark.sql.connector.catalog.Column;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;

import java.util.HashMap;
import java.util.Map;

/**
 * Patched UCSingleCatalog that fixes two UC OSS v0.4.0 issues:
 *
 * 1. alterTable: UC stubs this with UnsupportedOperationException at every level.
 *    Spark 4.1.0 Declarative Pipelines DatasetManager calls it during table
 *    materialization. This patch makes it a no-op that returns the current table.
 *
 * 2. createTable: UC requires 'delta.feature.catalogManaged'='supported' as a
 *    table property for managed tables. This patch injects it automatically so
 *    pipeline definitions don't need to specify it.
 */
public class PatchedUCSingleCatalog extends UCSingleCatalog {

    private static final String CATALOG_MANAGED_KEY = "delta.feature.catalogManaged";
    private static final String CATALOG_MANAGED_VALUE = "supported";

    private Map<String, String> ensureCatalogManaged(Map<String, String> properties) {
        if (properties != null && properties.containsKey(CATALOG_MANAGED_KEY)) {
            return properties;
        }
        Map<String, String> patched = new HashMap<>(properties != null ? properties : Map.of());
        patched.put(CATALOG_MANAGED_KEY, CATALOG_MANAGED_VALUE);
        return patched;
    }

    @Override
    @SuppressWarnings("deprecation")
    public Table createTable(Identifier ident, Column[] columns, Transform[] partitions,
                             Map<String, String> properties) {
        try {
            return super.createTable(ident, columns, partitions, ensureCatalogManaged(properties));
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("createTable failed", e);
        }
    }

    @Override
    @SuppressWarnings("deprecation")
    public Table createTable(Identifier ident, StructType schema, Transform[] partitions,
                             Map<String, String> properties) {
        try {
            return super.createTable(ident, schema, partitions, ensureCatalogManaged(properties));
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("createTable failed", e);
        }
    }

    @Override
    public Table alterTable(Identifier ident, TableChange... changes) {
        // UC OSS v0.4.0 doesn't support alterTable at any level.
        // Return the current table unchanged — pipeline metadata properties
        // won't be persisted to UC but the pipeline framework tracks them internally.
        try {
            return loadTable(ident);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("alterTable: failed to load table " + ident, e);
        }
    }
}
