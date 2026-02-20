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
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.connector.catalog.StagedTable;
import org.apache.spark.sql.connector.catalog.TruncatableTable;
import org.apache.spark.sql.connector.catalog.V1Table;

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
    // Cache schema storage roots to avoid repeated UC REST calls in loadTable
    private final java.util.concurrent.ConcurrentHashMap<String, String>
        schemaStorageRoots = new java.util.concurrent.ConcurrentHashMap<>();

    // Original S3A credential provider from Spark config, saved at initialization.
    // UC connector's loadTable() replaces this with AwsVendedTokenProvider (which
    // isn't on Hadoop's classloader). We restore the original after every super call.
    private volatile String originalS3ACredentialProvider;

    @Override
    public void initialize(String name, CaseInsensitiveStringMap options) {
        // When used as spark_catalog, delegate to UC's "electinfo" catalog
        String ucCatalogName = options.containsKey("uc-catalog")
            ? options.get("uc-catalog") : name;

        // Save the original S3A credential provider BEFORE UC can poison it.
        // This is typically SimpleAWSCredentialsProvider from spark-defaults.conf.
        try {
            org.apache.hadoop.conf.Configuration conf =
                SparkSession.active().sparkContext().hadoopConfiguration();
            this.originalS3ACredentialProvider =
                conf.get("fs.s3a.aws.credentials.provider");
        } catch (Exception e) {
            // SparkSession may not be active yet during early init
            this.originalS3ACredentialProvider =
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider";
        }

        super.initialize(ucCatalogName, options);
        this.catalogName = ucCatalogName;
        String uri = options.get("uri");
        if (uri != null) {
            this.ucApiBase = uri + "/api/2.1/unity-catalog";
        }

        // Clean any AwsVendedTokenProvider UC may have set during initialize()
        restoreS3ACredentialProvider();

        // Pre-warm the S3A FileSystem cache with clean config. Once cached,
        // FileSystem.get() returns this instance regardless of config changes.
        ensureCleanS3ACache();
    }

    private String getDefaultProvider() {
        try {
            return SparkSession.active().conf().get("spark.sql.sources.default");
        } catch (Exception e) {
            return "parquet";
        }
    }

    // --- UC REST API helpers ---

    private String getTableFormat(String schemaName, String tableName) {
        try {
            String fullName = catalogName + "." + schemaName + "." + tableName;
            URL url = new URL(ucApiBase + "/tables/" + fullName);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(30000);
            conn.setReadTimeout(30000);
            if (conn.getResponseCode() == 200) {
                String body = new String(conn.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
                int idx = body.indexOf("\"data_source_format\"");
                if (idx >= 0) {
                    int start = body.indexOf("\"", idx + 20) + 1;
                    int end = body.indexOf("\"", start);
                    return body.substring(start, end);
                }
            }
        } catch (Exception e) {
            // fall through
        }
        return null;
    }

    private void deleteTable(String schemaName, String tableName) {
        try {
            String fullName = catalogName + "." + schemaName + "." + tableName;
            URL url = new URL(ucApiBase + "/tables/" + fullName);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("DELETE");
            conn.setConnectTimeout(30000);
            conn.setReadTimeout(30000);
            int status = conn.getResponseCode();
            if (status >= 400 && status != 404) {
                String error = new String(
                    conn.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
                System.err.println("WARN: deleteTable " + fullName + " returned " +
                    status + ": " + error);
            }
        } catch (Exception e) {
            System.err.println("WARN: deleteTable failed: " + e.getMessage());
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
                // Extract storage_root from JSON response (handle null values)
                int idx = body.indexOf("\"storage_root\"");
                if (idx >= 0) {
                    int colonIdx = body.indexOf(":", idx + 14);
                    if (colonIdx >= 0) {
                        String afterColon = body.substring(colonIdx + 1).trim();
                        // Skip if value is JSON null
                        if (!afterColon.startsWith("null")) {
                            int start = body.indexOf("\"", colonIdx) + 1;
                            int end = body.indexOf("\"", start);
                            if (start > 0 && end > start) {
                                return body.substring(start, end);
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            // fall through to default
        }
        return "s3://hive-warehouse/" + schemaName;
    }

    /**
     * Restore the original S3A credential provider in the SHARED Hadoop config.
     * UC connector's loadTable() replaces it with AwsVendedTokenProvider, which
     * isn't on Hadoop's classloader. We restore the original (typically
     * SimpleAWSCredentialsProvider) so DLP's data writes work correctly.
     *
     * Unlike just unset()-ting the key, restoring the original value ensures
     * any NEW S3A FileSystem instances created between calls get the correct
     * provider class rather than falling back to an unpredictable default chain.
     */
    private void restoreS3ACredentialProvider() {
        try {
            org.apache.hadoop.conf.Configuration conf =
                SparkSession.active().sparkContext().hadoopConfiguration();

            // Restore global provider
            String current = conf.get("fs.s3a.aws.credentials.provider");
            if (current != null && current.contains("AwsVendedTokenProvider")) {
                if (originalS3ACredentialProvider != null) {
                    conf.set("fs.s3a.aws.credentials.provider",
                        originalS3ACredentialProvider);
                } else {
                    conf.unset("fs.s3a.aws.credentials.provider");
                }
                System.out.println(
                    "PatchedUCSingleCatalog: Restored S3A credential provider from " +
                    current + " to " + originalS3ACredentialProvider);
            }

            // Clean per-bucket overrides UC may have set
            java.util.List<String> toRemove = new java.util.ArrayList<>();
            for (Map.Entry<String, String> entry : conf) {
                if (entry.getKey().startsWith("fs.s3a.bucket.") &&
                        entry.getKey().endsWith(".aws.credentials.provider") &&
                        entry.getValue().contains("AwsVendedTokenProvider")) {
                    toRemove.add(entry.getKey());
                }
            }
            for (String key : toRemove) {
                conf.unset(key);
            }
        } catch (Exception e) {
            System.err.println(
                "WARN: restoreS3ACredentialProvider: " + e.getMessage());
        }
    }

    /**
     * Ensure a clean S3A FileSystem is cached for the hive-warehouse bucket.
     * Once cached, FileSystem.get() returns this instance regardless of later
     * config changes — protecting DLP's writes from UC's config poisoning.
     */
    private void ensureCleanS3ACache() {
        try {
            org.apache.hadoop.conf.Configuration conf =
                SparkSession.active().sparkContext().hadoopConfiguration();
            // Verify config is clean before FileSystem.get() — if the cache is
            // empty, get() creates a new FS with the current config. A poisoned
            // config would create an FS that throws ClassNotFoundException.
            String provider = conf.get("fs.s3a.aws.credentials.provider");
            if (provider != null && provider.contains("AwsVendedTokenProvider")) {
                restoreS3ACredentialProvider();
            }
            // Pre-warm FileSystem cache for all schemes and buckets used by
            // Spark/DLP. UC storage locations use s3:// (mapped to S3AFileSystem
            // via fs.s3.impl), while Spark configs use s3a://. The FS cache key
            // includes the scheme, so both must be pre-warmed separately.
            FileSystem.get(new java.net.URI("s3://hive-warehouse/"), conf);
            FileSystem.get(new java.net.URI("s3a://hive-warehouse/"), conf);
            FileSystem.get(new java.net.URI("s3a://spark-history/"), conf);
        } catch (Exception e) {
            System.err.println(
                "WARN: ensureCleanS3ACache: " + e.getMessage());
        }
    }

    /**
     * Get a Hadoop Configuration safe for direct S3A FileSystem operations.
     * Creates a COPY with the original credential provider restored, used
     * by our own code (truncate, S3 cleanup) with FileSystem.newInstance().
     */
    private org.apache.hadoop.conf.Configuration getCleanS3Config() {
        org.apache.hadoop.conf.Configuration conf =
            new org.apache.hadoop.conf.Configuration(
                SparkSession.active().sparkContext().hadoopConfiguration());
        // Restore original provider in the copy
        if (originalS3ACredentialProvider != null) {
            conf.set("fs.s3a.aws.credentials.provider",
                originalS3ACredentialProvider);
        } else {
            conf.unset("fs.s3a.aws.credentials.provider");
        }
        // Remove any per-bucket overrides UC may have set
        for (Map.Entry<String, String> entry : conf) {
            if (entry.getKey().startsWith("fs.s3a.bucket.") &&
                    entry.getKey().endsWith(".aws.credentials.provider")) {
                conf.unset(entry.getKey());
            }
        }
        return conf;
    }

    /** Bridge Scala's erased Map&lt;Object,Object&gt; to Map&lt;String,String&gt;. */
    @SuppressWarnings("unchecked")
    private static scala.collection.immutable.Map<String, String> castMap(
            scala.collection.immutable.Map<?, ?> map) {
        return (scala.collection.immutable.Map<String, String>) (Object) map;
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

        String provider = properties.getOrDefault(PROVIDER_KEY, "parquet");
        String storageRoot = getSchemaStorageRoot(schemaName);
        String location = storageRoot + "/" + tableName;

        // Ensure the V1 SessionCatalog has this schema with the UC storage root
        // as its location. DLP materializes views via the V1 catalog path, and
        // reanalyzeFlow reads via V2 (UC). Both must resolve to the same S3 path.
        org.apache.spark.sql.V1CatalogSync.ensureSchemaExists(schemaName, storageRoot);

        // Clean up any existing S3 data at the table location. The V1 write path
        // (CreateDataSourceTableAsSelectCommand) rejects creating tables at existing
        // locations. This can happen when UC table metadata was deleted but S3 data
        // was left behind (e.g., format correction, manual cleanup).
        try {
            org.apache.hadoop.conf.Configuration conf = getCleanS3Config();
            Path tablePath = new Path(location);
            FileSystem fs = FileSystem.newInstance(tablePath.toUri(), conf);
            try {
                if (fs.exists(tablePath)) {
                    fs.delete(tablePath, true);
                    System.out.println("PatchedUCSingleCatalog: Cleaned up existing data at " +
                        location + " before creating table");
                }
            } finally {
                fs.close();
            }
        } catch (Exception e) {
            System.err.println("WARN: createExternalTable: failed to clean up " +
                location + ": " + e.getMessage());
        }

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
                // Table already exists — check format matches
                if (status == 409 || error.contains("already exists")) {
                    String existingFormat = getTableFormat(schemaName, tableName);
                    if (existingFormat != null &&
                            !existingFormat.equalsIgnoreCase(provider)) {
                        // Format mismatch: drop and recreate
                        System.out.println("PatchedUCSingleCatalog: Format mismatch for " +
                            schemaName + "." + tableName + " (existing=" + existingFormat +
                            ", requested=" + provider.toUpperCase() + "). Dropping and recreating.");
                        deleteTable(schemaName, tableName);
                        return createExternalTable(ident, columnsJson, properties);
                    }
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
        // Force session default format — DLP may pass provider=delta for materialized
        // views, but V1 write path uses spark.sql.sources.default (parquet).
        // UC metadata must match the actual data format on disk.
        String requested = props.get(PROVIDER_KEY);
        String defaultFmt = getDefaultProvider();
        if (requested != null && !requested.equalsIgnoreCase(defaultFmt)) {
            System.out.println("PatchedUCSingleCatalog: createTable " + ident +
                " overriding provider " + requested + " -> " + defaultFmt);
        }
        props.put(PROVIDER_KEY, defaultFmt);
        return createExternalTable(ident, columnsToJson(columns), props);
    }

    @Override
    @SuppressWarnings("deprecation")
    public Table createTable(Identifier ident, StructType schema, Transform[] partitions,
                             Map<String, String> properties) {
        Map<String, String> props = new HashMap<>(properties != null ? properties : Map.of());
        String requested = props.get(PROVIDER_KEY);
        String defaultFmt = getDefaultProvider();
        if (requested != null && !requested.equalsIgnoreCase(defaultFmt)) {
            System.out.println("PatchedUCSingleCatalog: createTable " + ident +
                " overriding provider " + requested + " -> " + defaultFmt);
        }
        props.put(PROVIDER_KEY, defaultFmt);
        return createExternalTable(ident, structTypeToJson(schema), props);
    }

    // --- Staging overrides: UC OSS doesn't support staging tables ---
    // UCSingleCatalog extends StagingTableCatalog, so Spark always uses the staging
    // path for saveAsTable/CTAS. Override to create tables eagerly via our REST API
    // and return a simple StagedTable wrapper. Data is written to the S3 location
    // pointed by the V1Table; commitStagedChanges is a no-op.

    @Override
    public StagedTable stageCreate(Identifier ident, Column[] columns,
            Transform[] partitions, Map<String, String> properties) {
        Table table = createTable(ident, columns, partitions, properties);
        return wrapAsStagedTable(table, ident);
    }

    @Override
    @SuppressWarnings("deprecation")
    public StagedTable stageCreate(Identifier ident, StructType schema,
            Transform[] partitions, Map<String, String> properties) {
        Table table = createTable(ident, schema, partitions, properties);
        return wrapAsStagedTable(table, ident);
    }

    @Override
    public StagedTable stageReplace(Identifier ident, Column[] columns,
            Transform[] partitions, Map<String, String> properties) {
        try { dropTable(ident); } catch (Exception e) { /* ignore */ }
        Table table = createTable(ident, columns, partitions, properties);
        return wrapAsStagedTable(table, ident);
    }

    @Override
    @SuppressWarnings("deprecation")
    public StagedTable stageReplace(Identifier ident, StructType schema,
            Transform[] partitions, Map<String, String> properties) {
        try { dropTable(ident); } catch (Exception e) { /* ignore */ }
        Table table = createTable(ident, schema, partitions, properties);
        return wrapAsStagedTable(table, ident);
    }

    @Override
    public StagedTable stageCreateOrReplace(Identifier ident, Column[] columns,
            Transform[] partitions, Map<String, String> properties) {
        try { dropTable(ident); } catch (Exception e) { /* ignore */ }
        Table table = createTable(ident, columns, partitions, properties);
        return wrapAsStagedTable(table, ident);
    }

    @Override
    @SuppressWarnings("deprecation")
    public StagedTable stageCreateOrReplace(Identifier ident, StructType schema,
            Transform[] partitions, Map<String, String> properties) {
        try { dropTable(ident); } catch (Exception e) { /* ignore */ }
        Table table = createTable(ident, schema, partitions, properties);
        return wrapAsStagedTable(table, ident);
    }

    private StagedTable wrapAsStagedTable(Table table, Identifier ident) {
        if (table instanceof V1Table) {
            return new SimpleStagedTable((V1Table) table, ident);
        }
        throw new UnsupportedOperationException(
            "Cannot stage non-V1 table: " + table.getClass().getName());
    }

    /**
     * Simple StagedTable that wraps a V1Table. Table metadata and S3 location
     * are created eagerly in stageCreate; commitStagedChanges is a no-op since
     * data is written directly to the final S3 location during the staged write.
     */
    private class SimpleStagedTable extends V1Table implements StagedTable {
        private final Identifier ident;

        SimpleStagedTable(V1Table original, Identifier ident) {
            super(original.v1Table());
            this.ident = ident;
        }

        @Override
        public void commitStagedChanges() {
            // Data already written to S3, UC metadata already in place. No-op.
        }

        @Override
        public void abortStagedChanges() {
            // On failure, clean up the UC table
            if (ident.namespace().length > 0) {
                deleteTable(ident.namespace()[0], ident.name());
            }
        }
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

    // --- loadTable override: wrap with TruncatableTable support ---
    // DLP DatasetManager calls truncateTable() during full-refresh materialization.
    // UC connector tables don't implement TruncatableTable, so we wrap them in a
    // concrete delegate that adds TruncatableTable + delegates SupportsRead/Write.
    // (Dynamic Proxy breaks Spark's V2 batch scan capability detection.)

    @Override
    public Table loadTable(Identifier ident) {
        // Ensure V1 schema exists — DLP full-refresh may drop V1 schemas during
        // its initialization phase, so we recreate them here on every load.
        // The storage root is cached after the first UC REST call per schema.
        if (ident.namespace().length > 0) {
            String schemaName = ident.namespace()[0];
            String storageRoot = schemaStorageRoots.computeIfAbsent(
                schemaName, this::getSchemaStorageRoot);
            org.apache.spark.sql.V1CatalogSync.ensureSchemaExists(
                schemaName, storageRoot);
        }

        // Synchronized to prevent race condition: one thread's super.loadTable()
        // poisons the Hadoop config with AwsVendedTokenProvider while another
        // thread creates an S3A FileSystem with the poisoned config.
        synchronized (this) {
            // Ensure S3A FileSystem is cached with clean config BEFORE UC can
            // poison it. Once cached, FileSystem.get() returns this instance
            // regardless of config changes — even within super.loadTable().
            ensureCleanS3ACache();

            Table table = super.loadTable(ident);

            // UC connector sets AwsVendedTokenProvider in the shared Hadoop config.
            // Restore the original provider (SimpleAWSCredentialsProvider) so any
            // NEW FileSystem instances (e.g., for spark-history or other buckets)
            // get the correct provider class.
            restoreS3ACredentialProvider();

            // Strip UC vended credential properties from per-table storage config.
            // UC connector adds fs.s3a.* credentials + impl.disable.cache=true to
            // each table's CatalogStorageFormat properties. The disable.cache=true
            // forces a new S3A FileSystem instance that lacks MinIO-specific config
            // (endpoint, SSL disabled, change detection mode none), causing MinIO
            // 501 "header implies functionality not implemented" errors. Stripping
            // these per-table properties makes Spark use the pre-warmed cached
            // FileSystem (from ensureCleanS3ACache) which has the correct settings.
            if (table instanceof V1Table) {
                V1Table cleaned = stripUCStorageCredentials((V1Table) table);
                return new TruncatableV1Table(cleaned, ident);
            }
            if (table instanceof TruncatableTable) {
                return table;
            }
            return table;
        }
    }

    /**
     * Strip UC vended credential properties from a V1Table's CatalogTable
     * storage properties. Returns a new V1Table with a modified CatalogTable
     * if UC properties were found, or the original table if none were present.
     *
     * Properties stripped: fs.s3.impl.disable.cache, fs.s3a.impl.disable.cache,
     * fs.s3a.access.key, fs.s3a.secret.key, fs.s3a.session.token,
     * fs.s3a.path.style.access. These are all set by UC's credential vending
     * and are unnecessary when using static MinIO credentials configured globally.
     */
    @SuppressWarnings("unchecked")
    private V1Table stripUCStorageCredentials(V1Table table) {
        org.apache.spark.sql.catalyst.catalog.CatalogTable ct = table.v1Table();
        org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat storage = ct.storage();
        scala.collection.immutable.Map<String, String> props = storage.properties();

        // Build a filtered map, skipping UC credential/cache-bypass properties.
        // Cast through Object to bridge Scala's Map<Object,Object> erasure to
        // Map<String,String> — safe because the actual entries are String pairs.
        boolean changed = false;
        @SuppressWarnings("rawtypes")
        scala.collection.immutable.Map raw =
            scala.collection.immutable.Map$.MODULE$.empty();
        scala.collection.immutable.Map<String, String> filtered =
            castMap(raw);

        scala.collection.Iterator<scala.Tuple2<String, String>> iter = props.iterator();
        while (iter.hasNext()) {
            scala.Tuple2<String, String> entry = iter.next();
            String key = entry._1();
            if (key.startsWith("fs.s3") || key.contains("impl.disable.cache")) {
                changed = true; // skip UC credential property
            } else {
                filtered = castMap(filtered.$plus(entry));
            }
        }

        if (!changed) {
            return table; // No UC properties found, return original
        }

        // Create modified CatalogTable with cleaned storage properties
        org.apache.spark.sql.catalyst.catalog.CatalogTable newCT = ct.withNewStorage(
            storage.locationUri(),
            storage.inputFormat(),
            storage.outputFormat(),
            storage.compressed(),
            storage.serde(),
            filtered);

        return new V1Table(newCT);
    }

    /**
     * V1Table subclass that adds TruncatableTable support.
     * Extends V1Table so Spark's V1 scan resolution continues to work
     * (instanceof V1Table remains true), while DLP can call truncateTable().
     */
    private class TruncatableV1Table extends V1Table implements TruncatableTable {

        private final Identifier ident;

        TruncatableV1Table(V1Table original, Identifier ident) {
            super(original.v1Table());
            this.ident = ident;
        }

        @Override
        public boolean truncateTable() {
            return truncateTableData(ident);
        }
    }

    private boolean truncateTableData(Identifier ident) {
        String schemaName = ident.namespace()[0];
        String tableName = ident.name();
        String storageRoot = getSchemaStorageRoot(schemaName);
        String location = storageRoot + "/" + tableName;
        try {
            org.apache.hadoop.conf.Configuration conf = getCleanS3Config();
            Path tablePath = new Path(location);
            FileSystem fs = FileSystem.newInstance(tablePath.toUri(), conf);
            try {
                if (fs.exists(tablePath)) {
                    fs.delete(tablePath, true);
                    System.out.println("PatchedUCSingleCatalog: Truncated table data at " +
                        location);
                }
            } finally {
                fs.close();
            }
            return true;
        } catch (Exception e) {
            System.err.println("WARN: truncateTableData " + schemaName + "." + tableName +
                ": " + e.getMessage());
            return true;  // Return true so DLP can proceed with fresh write
        }
    }
}
