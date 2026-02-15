package info.elect.spark.streaming;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.spark.sql.execution.streaming.checkpointing.FileSystemBasedCheckpointFileManager;

import java.io.FileNotFoundException;

/**
 * CheckpointFileManager that handles S3A's FileNotFoundException on directory listing.
 *
 * S3AFileSystem.listStatus() throws FileNotFoundException when a directory doesn't
 * exist, unlike HDFS which returns an empty array. Spark's streaming checkpoint code
 * (CompactibleFileStreamLog.compactInterval) calls list() on the sources directory
 * before it exists, causing all streaming flows to fail on first run.
 *
 * This manager catches FileNotFoundException in list() and returns an empty array,
 * matching HDFS behavior.
 *
 * Note: The parent class is Scala-compiled and throws IOException at runtime despite
 * not declaring it, so we catch Exception and re-throw non-FNFE exceptions.
 */
public class S3ACheckpointFileManager extends FileSystemBasedCheckpointFileManager {

    public S3ACheckpointFileManager(Path path, Configuration conf) {
        super(path, conf);
    }

    @Override
    @SuppressWarnings("all")
    public FileStatus[] list(Path path) {
        try {
            return super.list(path);
        } catch (Exception e) {
            if (e instanceof FileNotFoundException) {
                return new FileStatus[0];
            }
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    @SuppressWarnings("all")
    public FileStatus[] list(Path path, PathFilter filter) {
        try {
            return super.list(path, filter);
        } catch (Exception e) {
            if (e instanceof FileNotFoundException) {
                return new FileStatus[0];
            }
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            }
            throw new RuntimeException(e);
        }
    }
}
