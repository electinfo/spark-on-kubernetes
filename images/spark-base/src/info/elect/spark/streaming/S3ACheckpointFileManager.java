package info.elect.spark.streaming;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.spark.sql.execution.streaming.checkpointing.FileSystemBasedCheckpointFileManager;

import java.io.FileNotFoundException;
import java.io.IOException;

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
 */
public class S3ACheckpointFileManager extends FileSystemBasedCheckpointFileManager {

    public S3ACheckpointFileManager(Path path, Configuration conf) {
        super(path, conf);
    }

    @Override
    public FileStatus[] list(Path path) throws IOException {
        try {
            return super.list(path);
        } catch (FileNotFoundException e) {
            return new FileStatus[0];
        }
    }

    @Override
    public FileStatus[] list(Path path, PathFilter filter) throws IOException {
        try {
            return super.list(path, filter);
        } catch (FileNotFoundException e) {
            return new FileStatus[0];
        }
    }
}
