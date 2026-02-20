package io.unitycatalog.spark.auth.storage;

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider;

/**
 * Shim for Unity Catalog connector's AwsVendedTokenProvider.
 *
 * UC connector's loadTable() sets fs.s3a.aws.credentials.provider to this
 * class in the shared Hadoop config, but the real class isn't on Hadoop's
 * classloader. This shim delegates to SimpleAWSCredentialsProvider, which
 * reads the static MinIO credentials (fs.s3a.access.key / fs.s3a.secret.key)
 * from spark-defaults.conf.
 *
 * With this shim on the classpath (in spark-catalog-patch-1.0.jar), the
 * ClassNotFoundException race condition between UC's config poisoning and
 * DLP's parallel FileSystem creation is eliminated entirely.
 */
public class AwsVendedTokenProvider extends SimpleAWSCredentialsProvider {

    public AwsVendedTokenProvider(URI uri, Configuration conf) throws IOException {
        super(uri, conf);
    }
}
