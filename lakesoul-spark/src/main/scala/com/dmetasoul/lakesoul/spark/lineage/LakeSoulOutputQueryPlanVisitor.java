// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
package com.dmetasoul.lakesoul.spark.lineage;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.OpenLineageContext;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.lakesoul.sources.LakeSoulBaseRelation;
import org.apache.spark.sql.types.StructType;
import scala.PartialFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class LakeSoulOutputQueryPlanVisitor implements PartialFunction<LogicalPlan, List<OpenLineage.OutputDataset>> {

    private final OpenLineage ol;
    private final OpenLineageContext context;

    public LakeSoulOutputQueryPlanVisitor(OpenLineageContext context) {
        this.context = context;
        this.ol = context.getOpenLineage();
    }

    @Override
    public boolean isDefinedAt(LogicalPlan plan) {
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

}