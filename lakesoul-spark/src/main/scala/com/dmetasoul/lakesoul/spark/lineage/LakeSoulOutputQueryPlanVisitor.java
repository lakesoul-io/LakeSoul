package com.dmetasoul.lakesoul.spark.lineage;

import com.dmetasoul.lakesoul.meta.DBManager;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.OpenLineageContext;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.lakesoul.sources.LakeSoulBaseRelation;
import org.apache.spark.sql.types.StructType;
import scala.None;
import scala.Option;
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
        return plan instanceof LogicalRelation;
    }

    @Override
    public List<OpenLineage.OutputDataset> apply(LogicalPlan plan) {

        if (plan instanceof LogicalRelation) {
            LogicalRelation relation = (LogicalRelation) plan;
            StructType schema = relation.relation().schema();
            LakeSoulBaseRelation lakeSoulBaseRelation = (LakeSoulBaseRelation) relation.relation();
            String namespace = lakeSoulBaseRelation.tableInfo().namespace();
            String tableName = null;
            String tablePath = "";
            if (lakeSoulBaseRelation.tableInfo().short_table_name().isEmpty()){
                tablePath = lakeSoulBaseRelation.tableInfo().table_path().toString();
            } else {
                tableName = lakeSoulBaseRelation.tableInfo().short_table_name().get();
            }
            OpenLineage.SchemaDatasetFacet datasetFacet = buildSchemaFacet(schema);
            OpenLineage.DatasetFacets facets = buildDatasetFacets(datasetFacet);
            OpenLineage.OutputDataset outputDataset = ol.newOutputDatasetBuilder()
                    .name(tableName == null ? tablePath:tableName)
                    .namespace(namespace)
                    .facets(facets)
                    .build();

            return Collections.singletonList(outputDataset);
        }
        return Collections.emptyList();
    }

    private OpenLineage.SchemaDatasetFacet buildSchemaFacet(StructType schema) {
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

    private OpenLineage.DatasetFacets buildDatasetFacets(OpenLineage.SchemaDatasetFacet schemaFacet) {
        return ol.newDatasetFacetsBuilder()
                .schema(schemaFacet)
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