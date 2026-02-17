package info.elect.spark.catalog;

import io.unitycatalog.spark.UCSingleCatalog;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Column;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;

import java.util.HashMap;
import java.util.Map;

/**
 * Patched UCSingleCatalog that fixes three UC OSS v0.4.0 issues:
 *
 * 1. alterTable: UC stubs this with UnsupportedOperationException at every level.
 *    Spark 4.1.0 Declarative Pipelines DatasetManager calls it during table
 *    materialization. This patch makes it a no-op that returns the current table.
 *
 * 2. createTable missing properties: UCProxy.createTable(StructType) asserts that
 *    'provider' is non-null. The pipeline engine doesn't always include it in
 *    the properties map. This patch reads from spark.sql.sources.default.
 *    For Delta tables, also injects delta.feature.catalogManaged=supported.
 *
 * 3. createTable assertion safety net: If any assertion in UCProxy still fires
 *    (e.g. missing location), we catch AssertionError and attempt loadTable in case
 *    the table was partially created.
 */
public class PatchedUCSingleCatalog extends UCSingleCatalog {

    private static final String CATALOG_MANAGED_KEY = "delta.feature.catalogManaged";
    private static final String CATALOG_MANAGED_VALUE = "supported";
    private static final String PROVIDER_KEY = "provider";

    private String getDefaultProvider() {
        try {
            return SparkSession.active().conf().get("spark.sql.sources.default");
        } catch (Exception e) {
            return "parquet";
        }
    }

    private Map<String, String> ensureRequiredProperties(Map<String, String> properties) {
        Map<String, String> patched = new HashMap<>(properties != null ? properties : Map.of());
        patched.putIfAbsent(PROVIDER_KEY, getDefaultProvider());
        // UC v0.4.0 requires catalogManaged for all managed tables, regardless of format
        patched.putIfAbsent(CATALOG_MANAGED_KEY, CATALOG_MANAGED_VALUE);
        return patched;
    }

    @Override
    @SuppressWarnings("deprecation")
    public Table createTable(Identifier ident, Column[] columns, Transform[] partitions,
                             Map<String, String> properties) {
        try {
            return super.createTable(ident, columns, partitions, ensureRequiredProperties(properties));
        } catch (AssertionError e) {
            // Safety net: if an assertion in UCProxy still fires despite property injection,
            // try loading the table in case it was partially created.
            try {
                return loadTable(ident);
            } catch (Exception ex) {
                throw new RuntimeException("createTable: assertion in UC and loadTable failed for " + ident, ex);
            }
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
            return super.createTable(ident, schema, partitions, ensureRequiredProperties(properties));
        } catch (AssertionError e) {
            try {
                return loadTable(ident);
            } catch (Exception ex) {
                throw new RuntimeException("createTable: assertion in UC and loadTable failed for " + ident, ex);
            }
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
