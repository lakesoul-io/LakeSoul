package com.dmetasoul.lakesoul.spark.lineage;

import com.dmetasoul.lakesoul.meta.DBManager;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.OpenLineageContext;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import scala.PartialFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * LakeSoul 写操作（INSERT/MERGE/DELETE/UPSERT/COMPACTION）输出血缘采集
 * 完全对标 Iceberg 官方实现
 */
public class LakeSoulOutputQueryPlanVisitor implements PartialFunction<LogicalPlan, List<OpenLineage.OutputDataset>> {

    private final OpenLineage ol;
    private final OpenLineageContext context;

    public LakeSoulOutputQueryPlanVisitor(OpenLineageContext context) {
        this.context = context;
        this.ol = context.getOpenLineage();
    }

    @Override
    public boolean isDefinedAt(LogicalPlan plan) {
        System.out.println("LakeSoulOutputVisitor isDefinedAt called on: " + plan.getClass().getName());
        // 关键：写操作的 LogicalPlan 外层是各种 Command，但 child 是 DataSourceV2Relation
        // 我们只关心 child 是 LakeSoul 表的场景
//        if (!(plan instanceof org.apache.spark.sql.catalyst.plans.logical.UnaryNode)) {
//            return false;
//        }
//
//        LogicalPlan child = ((org.apache.spark.sql.catalyst.plans.logical.UnaryNode) plan).child();
//
//        if (!(child instanceof DataSourceV2Relation)) {
//            return false;
//        }
//
        return true;
    }

    @Override
    public List<OpenLineage.OutputDataset> apply(LogicalPlan plan) {
        LogicalPlan child = ((org.apache.spark.sql.catalyst.plans.logical.UnaryNode) plan).child();
        if (!(child instanceof DataSourceV2Relation)) {
            return Collections.emptyList();
        }

        DataSourceV2Relation relation = (DataSourceV2Relation) child;

        String tableName = relation.table().name();
        String namespace = getNamespace(tableName);

        // Schema Facet
        OpenLineage.SchemaDatasetFacet schemaFacet = buildSchemaFacet(relation.schema());

        // Dataset Facets（加上 DataSource + 自定义 LakeSoul Facet）

        // 构建 OutputDataset
//        OpenLineage.OutputDataset outputDataset = ol.newOutputDatasetBuilder()
//                .name(tableName)
//                .namespace(namespace)
//                .facets(facets)
//                // 可选：加上 OutputStatistics（行数）
//                // .outputFacets(buildOutputFacets())
//                .build();

        //return Collections.singletonList(outputDataset);

        return Collections.emptyList();
    }

    private String getNamespace(String tableName) {
        DBManager dbManager = new DBManager();
        return dbManager.getNamespaceByTableName(tableName);
    }

    private OpenLineage.SchemaDatasetFacet buildSchemaFacet(org.apache.spark.sql.types.StructType schema) {
        List<OpenLineage.SchemaDatasetFacetFields> fields = new ArrayList<>();
        for (org.apache.spark.sql.types.StructField field : schema.fields()) {
            fields.add(ol.newSchemaDatasetFacetFieldsBuilder()
                    .name(field.name())
                    .type(field.dataType().typeName())
                    .build());
        }
        return ol.newSchemaDatasetFacetBuilder()
                .fields(fields)
                .build();
    }


    // 简单判断当前写操作类型（可扩展）
    private String getCurrentOperation(LogicalPlan plan) {
        String planName = plan.getClass().getSimpleName().toLowerCase();
        if (planName.contains("insert")) return "INSERT";
        if (planName.contains("merge")) return "MERGE";
        if (planName.contains("delete")) return "DELETE";
        if (planName.contains("upsert")) return "UPSERT";
        if (planName.contains("compaction")) return "COMPACTION";
        return "WRITE";
    }
}