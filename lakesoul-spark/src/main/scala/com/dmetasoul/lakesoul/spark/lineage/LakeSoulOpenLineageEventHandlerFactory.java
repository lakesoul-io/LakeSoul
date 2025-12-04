// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
package com.dmetasoul.lakesoul.spark.lineage;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.CustomFacetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.OpenLineageEventHandlerFactory;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import scala.PartialFunction;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class LakeSoulOpenLineageEventHandlerFactory implements OpenLineageEventHandlerFactory {

    @Override
    public Collection<PartialFunction<LogicalPlan, List<OpenLineage.InputDataset>>> createInputDatasetQueryPlanVisitors(OpenLineageContext context) {
        return Collections.singletonList(new LakeSoulInputQueryPlanVisitor(context));
    }

    @Override
    public Collection<PartialFunction<LogicalPlan, List<OpenLineage.OutputDataset>>> createOutputDatasetQueryPlanVisitors(OpenLineageContext context) {
        return Collections.singletonList(new LakeSoulOutputQueryPlanVisitor(context));
    }
}
