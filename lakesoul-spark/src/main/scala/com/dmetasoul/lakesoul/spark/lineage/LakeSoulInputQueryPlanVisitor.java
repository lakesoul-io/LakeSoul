// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
package com.dmetasoul.lakesoul.spark.lineage;

import com.dmetasoul.lakesoul.meta.DBManager;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.OpenLineageContext;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.UnaryNode;
import org.apache.spark.sql.catalyst.plans.logical.LeafNode;
import org.apache.spark.sql.execution.columnar.InMemoryRelation;
import org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation;
import org.apache.spark.sql.lakesoul.sources.LakeSoulDataSource;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.PartialFunction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class LakeSoulInputQueryPlanVisitor implements PartialFunction<LogicalPlan, List<OpenLineage.InputDataset>> {
    private final OpenLineage ol;

    private final OpenLineageContext context;

    public LakeSoulInputQueryPlanVisitor(OpenLineageContext context) {
        this.context = context;
        this.ol = context.getOpenLineage();
    }

    @Override
    public boolean isDefinedAt(LogicalPlan x) {
        // 可以加判断，只匹配 DataSourceV2Relation

        return  (x instanceof DataSourceV2ScanRelation || x instanceof SaveIntoDataSourceCommand);
    }
    @Override
    public List<OpenLineage.InputDataset> apply(LogicalPlan plan) {
        if (plan instanceof DataSourceV2ScanRelation) {
            DataSourceV2ScanRelation relation = (DataSourceV2ScanRelation) plan;
            String tableName = relation.name();
            StructType schema = relation.schema();
            DBManager dbManager = new DBManager();
            String tableNamespace = dbManager.getNamespaceByTableName(tableName);
            OpenLineage.SchemaDatasetFacet schemaFacet = buildSchemaFacet(schema);
            OpenLineage.DatasetFacets datasetFacets = buildDatasetFacets(schemaFacet);
            OpenLineage.InputDataset dataset = ol.newInputDatasetBuilder()
                    .name(tableName)
                    .namespace(tableNamespace)
                    .facets(datasetFacets)
                    .build();
            List<OpenLineage.InputDataset> datasets = new ArrayList<>();
            datasets.add(dataset);
            return datasets;
        } else if (plan instanceof SaveIntoDataSourceCommand) {
            SaveIntoDataSourceCommand sourceCommand = (SaveIntoDataSourceCommand) plan;
            StructType schema = sourceCommand.schema();
            OpenLineage.SchemaDatasetFacet schemaFacet = buildSchemaFacet(schema);
            OpenLineage.DatasetFacets datasetFacets = buildDatasetFacets(schemaFacet);
            OpenLineage.InputDataset dataset = ol.newInputDatasetBuilder()
                    .name("tableName=None")
                    .namespace("tableNamespace=None")
                    .facets(datasetFacets)
                    .build();
            List<OpenLineage.InputDataset> datasets = new ArrayList<>();
            datasets.add(dataset);
            return datasets;
        }
        return Collections.emptyList();
    }

    private OpenLineage.SchemaDatasetFacet buildSchemaFacet(StructType schema){
        List<OpenLineage.SchemaDatasetFacetFields> schemaFields = new ArrayList<>();
        for (StructField field : schema.fields()) {
           schemaFields.add(ol.newSchemaDatasetFacetFieldsBuilder()
                   .name(field.name())
                   .type(field.dataType().typeName())
                   .build());
        }
        return ol.newSchemaDatasetFacetBuilder()
                .fields(schemaFields).build();
    }

    private OpenLineage.DatasetFacets buildDatasetFacets(OpenLineage.SchemaDatasetFacet schemaFacet) {
        return ol.newDatasetFacetsBuilder()
                .schema(schemaFacet)
                .build();
    }
}
