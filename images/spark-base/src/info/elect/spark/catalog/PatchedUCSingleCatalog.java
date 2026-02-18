package info.elect.spark.catalog;

import io.unitycatalog.spark.UCSingleCatalog;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Column;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Patched UCSingleCatalog that fixes UC OSS v0.4.0 issues for Spark 4.1.0
 * Declarative Pipelines:
 *
 * 1. alterTable: UC stubs this with UnsupportedOperationException.
 *    Pipeline DatasetManager calls it during materialization.
 *    Patched to return current table (no-op).
 *
 * 2. createTable: Bypasses UCProxy entirely and creates all tables as
 *    EXTERNAL via the UC REST API with deterministic storage paths
 *    ({storage_root}/{table_name}). This avoids UC managed table storage
 *    (__unitystorage/) which DLP doesn't reliably write to, and supports
 *    any format (delta, parquet, csv, json).
 */
public class PatchedUCSingleCatalog extends UCSingleCatalog {

    private static final String PROVIDER_KEY = "provider";

    private String ucApiBase;
    private String catalogName;

    @Override
    public void initialize(String name, CaseInsensitiveStringMap options) {
        // When used as spark_catalog, delegate to UC's "electinfo" catalog
        String ucCatalogName = options.containsKey("uc-catalog")
            ? options.get("uc-catalog") : name;
        super.initialize(ucCatalogName, options);
        this.catalogName = ucCatalogName;
        String uri = options.get("uri");
        if (uri != null) {
            this.ucApiBase = uri + "/api/2.1/unity-catalog";
        }
    }

    private String getDefaultProvider() {
        try {
            return SparkSession.active().conf().get("spark.sql.sources.default");
        } catch (Exception e) {
            return "parquet";
        }
    }

    // --- Create EXTERNAL table via UC REST API ---

    private String getSchemaStorageRoot(String schemaName) {
        try {
            URL url = new URL(ucApiBase + "/schemas/" + catalogName + "." + schemaName);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(30000);
            conn.setReadTimeout(30000);
            if (conn.getResponseCode() == 200) {
                String body = new String(conn.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
                // Extract storage_root from JSON response
                int idx = body.indexOf("\"storage_root\"");
                if (idx >= 0) {
                    int start = body.indexOf("\"", idx + 14) + 1;
                    int end = body.indexOf("\"", start);
                    return body.substring(start, end);
                }
            }
        } catch (Exception e) {
            // fall through to default
        }
        return "s3://hive-warehouse/" + schemaName;
    }

    private String escapeJson(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private String ucTypeName(String sparkSqlType) {
        String upper = sparkSqlType.toUpperCase();
        if (upper.startsWith("DECIMAL")) return "DECIMAL";
        if (upper.startsWith("VARCHAR")) return "STRING";
        if (upper.startsWith("CHAR")) return "STRING";
        if (upper.startsWith("ARRAY")) return "ARRAY";
        if (upper.startsWith("MAP")) return "MAP";
        if (upper.startsWith("STRUCT")) return "STRUCT";
        switch (upper) {
            case "INT": case "INTEGER": return "INT";
            case "BIGINT": case "LONG": return "LONG";
            case "SMALLINT": case "SHORT": return "SHORT";
            case "TINYINT": case "BYTE": return "BYTE";
            case "FLOAT": case "REAL": return "FLOAT";
            case "DOUBLE": return "DOUBLE";
            case "STRING": return "STRING";
            case "BOOLEAN": return "BOOLEAN";
            case "DATE": return "DATE";
            case "TIMESTAMP": return "TIMESTAMP";
            case "TIMESTAMP_NTZ": return "TIMESTAMP_NTZ";
            case "BINARY": return "BINARY";
            default: return "STRING";
        }
    }

    private String typeJson(String name, String sqlType, boolean nullable) {
        // Spark-style JSON schema: {"name":"col","type":"string","nullable":true,"metadata":{}}
        String sparkType = sqlType.toLowerCase();
        return "{\\\"name\\\":\\\"" + escapeJson(name) +
               "\\\",\\\"type\\\":\\\"" + escapeJson(sparkType) +
               "\\\",\\\"nullable\\\":" + nullable +
               ",\\\"metadata\\\":{}}";
    }

    private String columnsToJson(Column[] columns) {
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < columns.length; i++) {
            if (i > 0) sb.append(",");
            String sqlType = columns[i].dataType().sql();
            sb.append("{")
              .append("\"name\":\"").append(escapeJson(columns[i].name())).append("\",")
              .append("\"type_text\":\"").append(escapeJson(sqlType)).append("\",")
              .append("\"type_json\":\"").append(typeJson(columns[i].name(), sqlType, columns[i].nullable())).append("\",")
              .append("\"type_name\":\"").append(ucTypeName(sqlType)).append("\",")
              .append("\"position\":").append(i).append(",")
              .append("\"nullable\":").append(columns[i].nullable())
              .append("}");
        }
        sb.append("]");
        return sb.toString();
    }

    private String structTypeToJson(StructType schema) {
        StructField[] fields = schema.fields();
        StringBuilder sb = new StringBuilder("[");
        for (int i = 0; i < fields.length; i++) {
            if (i > 0) sb.append(",");
            String sqlType = fields[i].dataType().sql();
            sb.append("{")
              .append("\"name\":\"").append(escapeJson(fields[i].name())).append("\",")
              .append("\"type_text\":\"").append(escapeJson(sqlType)).append("\",")
              .append("\"type_json\":\"").append(typeJson(fields[i].name(), sqlType, fields[i].nullable())).append("\",")
              .append("\"type_name\":\"").append(ucTypeName(sqlType)).append("\",")
              .append("\"position\":").append(i).append(",")
              .append("\"nullable\":").append(fields[i].nullable())
              .append("}");
        }
        sb.append("]");
        return sb.toString();
    }

    private Table createExternalTable(Identifier ident, String columnsJson,
                                      Map<String, String> properties) {
        String schemaName = ident.namespace()[0];
        String tableName = ident.name();

        // Ensure the V1 SessionCatalog has this schema.
        // CreateDataSourceTableAsSelectCommand validates via V1's requireDbExists(),
        // which doesn't know about V2 catalog schemas.
        org.apache.spark.sql.V1CatalogSync.ensureSchemaExists(schemaName);
        String provider = properties.getOrDefault(PROVIDER_KEY, "parquet");
        String storageRoot = getSchemaStorageRoot(schemaName);
        String location = storageRoot + "/" + tableName;

        String body = "{" +
            "\"name\":\"" + escapeJson(tableName) + "\"," +
            "\"catalog_name\":\"" + escapeJson(catalogName) + "\"," +
            "\"schema_name\":\"" + escapeJson(schemaName) + "\"," +
            "\"table_type\":\"EXTERNAL\"," +
            "\"data_source_format\":\"" + provider.toUpperCase() + "\"," +
            "\"storage_location\":\"" + escapeJson(location) + "\"," +
            "\"columns\":" + columnsJson +
            "}";

        try {
            URL url = new URL(ucApiBase + "/tables");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setConnectTimeout(30000);
            conn.setReadTimeout(30000);
            try (OutputStream os = conn.getOutputStream()) {
                os.write(body.getBytes(StandardCharsets.UTF_8));
            }
            int status = conn.getResponseCode();
            if (status >= 400) {
                String error = new String(
                    conn.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
                // Table already exists — fine, load it
                if (status == 409 || error.contains("already exists")) {
                    return loadTable(ident);
                }
                throw new RuntimeException(
                    "UC REST API createTable " + status + ": " + error);
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Failed to create external table via UC API", e);
        }

        try {
            return loadTable(ident);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(
                "createTable: table created via API but loadTable failed for " + ident, e);
        }
    }

    // --- createTable overrides ---

    @Override
    @SuppressWarnings("deprecation")
    public Table createTable(Identifier ident, Column[] columns, Transform[] partitions,
                             Map<String, String> properties) {
        Map<String, String> props = new HashMap<>(properties != null ? properties : Map.of());
        props.putIfAbsent(PROVIDER_KEY, getDefaultProvider());
        return createExternalTable(ident, columnsToJson(columns), props);
    }

    @Override
    @SuppressWarnings("deprecation")
    public Table createTable(Identifier ident, StructType schema, Transform[] partitions,
                             Map<String, String> properties) {
        Map<String, String> props = new HashMap<>(properties != null ? properties : Map.of());
        props.putIfAbsent(PROVIDER_KEY, getDefaultProvider());
        return createExternalTable(ident, structTypeToJson(schema), props);
    }

    @Override
    public Table alterTable(Identifier ident, TableChange... changes) {
        try {
            return loadTable(ident);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("alterTable: failed to load table " + ident, e);
        }
    }
}
