package com.dmetasoul.lakesoul.spark.lineage;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.lifecycle.plan.catalog.CatalogHandler;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;

import java.util.Map;

public class LakeSoulCatalogHandler implements CatalogHandler {

    private final OpenLineageContext context;
    public LakeSoulCatalogHandler(OpenLineageContext context) {
        this.context = context;
    }

    @Override
    public boolean hasClasses() {
        try {
            Class.forName("org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog");
            return true;
        } catch (Throwable t) {
            return false;
        }
    }

    @Override
    public boolean isClass(TableCatalog tableCatalog) {
        return tableCatalog.getClass().getCanonicalName().equals(
                "org.apache.spark.sql.lakesoul.catalog.LakeSoulCatalog"
        );
    }

    @Override
    public DatasetIdentifier getDatasetIdentifier(SparkSession sparkSession,
                                                  TableCatalog tableCatalog,
                                                  Identifier identifier,
                                                  Map<String, String> map) {
        String tableName = null;
        try {
            Table table = tableCatalog.loadTable(identifier);
            tableName = table.name();
            System.out.println(tableName);
        } catch (NoSuchTableException e) {
            throw new RuntimeException(e);
        }
        return new DatasetIdentifier(tableName,"lakesoul");
    }

    @Override
    public String getName() {
        return "";
    }
}
