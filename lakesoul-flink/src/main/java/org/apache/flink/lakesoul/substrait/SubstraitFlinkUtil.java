package org.apache.flink.lakesoul.substrait;

import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.extension.SimpleExtension;
import io.substrait.plan.Plan;
import io.substrait.type.TypeCreator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.dmetasoul.lakesoul.lakesoul.io.substrait.SubstraitUtil.*;

public class SubstraitFlinkUtil {

    public static Tuple2<SupportsFilterPushDown.Result, io.substrait.proto.Plan> flinkExprToSubStraitPlan(
            List<ResolvedExpression> nonPartitionFilters,
            List<ResolvedExpression> remaining,
            String tableName
    ) {
        List<ResolvedExpression> pushed = new ArrayList<>();
        Expression last = null;
        for (ResolvedExpression fExpr : nonPartitionFilters) {
            Expression substraitExpr = toSubstraitExpr(fExpr);
            if (substraitExpr == null) {
                remaining.add(fExpr);
            } else {
                pushed.add(fExpr);
                if (last != null) {
                    SimpleExtension.FunctionAnchor fa = SimpleExtension.FunctionAnchor.of(BooleanNamespace, "and:bool");
                    last = ExpressionCreator.scalarFunction(EXTENSIONS.getScalarFunction(fa), TypeCreator.NULLABLE.BOOLEAN, last, substraitExpr);
                } else {
                    last = substraitExpr;
                }
            }
        }
        Plan filter = exprToFilter(last, tableName);
        return Tuple2.of(SupportsFilterPushDown.Result.of(pushed, remaining), planToProto(filter));
    }

    public static Expression toSubstraitExpr(ResolvedExpression flinkExpression) {
        SubstraitVisitor substraitVisitor = new SubstraitVisitor();
        return flinkExpression.accept(substraitVisitor);
    }

    public static boolean filterContainsPartitionColumn(ResolvedExpression expression, Set<String> partitionCols) {
        if (expression instanceof FieldReferenceExpression) {
            return partitionCols.contains(((FieldReferenceExpression) expression).getName());
        } else if (expression instanceof CallExpression) {
            for (ResolvedExpression child : expression.getResolvedChildren()) {
                if (filterContainsPartitionColumn(child, partitionCols)) {
                    return true;
                }
            }
        }
        return false;
    }
}
