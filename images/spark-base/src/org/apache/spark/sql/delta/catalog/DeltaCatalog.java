package org.apache.spark.sql.delta.catalog;

import org.apache.spark.sql.connector.catalog.DelegatingCatalogExtension;

/**
 * Shim DeltaCatalog for Spark 4.1.0 compatibility.
 *
 * Delta Spark 4.0.1's DeltaCatalog has binary incompatibility with Spark 4.1.0
 * (NoSuchMethodError on LogKey.$init$). This shim extends DelegatingCatalogExtension
 * which satisfies the DELTA_CONFIGURE_SPARK_SESSION_WITH_EXTENSION_AND_CATALOG check
 * while being compiled against Spark 4.1.0's API.
 *
 * This class is loaded via spark.jars (custom-jars) which takes priority over the
 * Delta 4.0.1 DeltaCatalog loaded via spark.jars.packages (Ivy).
 */
public class DeltaCatalog extends DelegatingCatalogExtension {
}
