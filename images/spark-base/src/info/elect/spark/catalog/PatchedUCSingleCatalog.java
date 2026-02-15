package info.elect.spark.catalog;

import io.unitycatalog.spark.UCSingleCatalog;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;

import java.lang.reflect.Field;

/**
 * Patched UCSingleCatalog that delegates alterTable to the underlying catalog.
 *
 * UC OSS v0.4.0 has alterTable as a stub that throws UnsupportedOperationException.
 * Spark 4.1.0 Declarative Pipelines DatasetManager calls alterTable during table
 * materialization to sync schema/properties. This patch delegates to the underlying
 * DelegatingCatalogExtension which fully supports ALTER TABLE.
 */
public class PatchedUCSingleCatalog extends UCSingleCatalog {

    private volatile TableCatalog cachedDelegate;

    private TableCatalog getDelegate() {
        if (cachedDelegate == null) {
            synchronized (this) {
                if (cachedDelegate == null) {
                    try {
                        Field f = UCSingleCatalog.class.getDeclaredField("delegate");
                        f.setAccessible(true);
                        cachedDelegate = (TableCatalog) f.get(this);
                    } catch (ReflectiveOperationException e) {
                        throw new RuntimeException("Failed to access UCSingleCatalog delegate", e);
                    }
                }
            }
        }
        return cachedDelegate;
    }

    @Override
    public Table alterTable(Identifier ident, TableChange... changes)
            throws NoSuchTableException {
        return getDelegate().alterTable(ident, changes);
    }
}
