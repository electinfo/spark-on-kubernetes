package org.apache.spark.sql.execution.streaming;

import org.apache.spark.sql.execution.datasources.DataSource;

/**
 * Stub class for binary compatibility between Delta Spark 4.0.1 and Spark 4.1.0.
 *
 * In Spark 4.1.0 StreamingRelation was moved to
 * org.apache.spark.sql.execution.streaming.runtime.StreamingRelation.
 *
 * Delta 4.0.1's DeltaAnalysis.verifyDeltaSourceSchemaLocation references this
 * class in instanceof checks. The class must exist to avoid NoClassDefFoundError,
 * but no runtime object will ever be an instance of it (Spark 4.1.0 creates
 * runtime.StreamingRelation nodes), so the instanceof check always returns false
 * and the methods are never called.
 */
public abstract class StreamingRelation {

    public DataSource dataSource() {
        throw new UnsupportedOperationException("stub");
    }

    public String sourceName() {
        throw new UnsupportedOperationException("stub");
    }
}
