package org.apache.flink.lakesoul.substrait;

import io.substrait.expression.Expression;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;

import java.util.*;

import static com.dmetasoul.lakesoul.lakesoul.io.substrait.SubstraitUtil.and;

public class SubstraitFlinkUtil {

    public static Tuple2<SupportsFilterPushDown.Result, Expression> flinkExprToSubStraitExpr(
            List<ResolvedExpression> filters
    ) {
        List<ResolvedExpression> pushed = new ArrayList<>();
        List<ResolvedExpression> remaining = new ArrayList<>();
        Expression combined = null;
        for (ResolvedExpression fExpr : filters) {
            Expression substraitExpr = toSubstraitExpr(fExpr);
            if (substraitExpr == null) {
                remaining.add(fExpr);
            } else {
                pushed.add(fExpr);
                if (combined != null) {
                    combined = and(combined, substraitExpr);
                } else {
                    combined = substraitExpr;
                }
            }
        }
        return Tuple2.of(SupportsFilterPushDown.Result.of(pushed, remaining), combined);
    }

    public static Expression toSubstraitExpr(ResolvedExpression flinkExpression) {
        int nested_depth = calculateDepth(flinkExpression);
        if (nested_depth > 10) {
            // do not convert too deeply nested expressions
            return null;
        }
        SubstraitVisitor substraitVisitor = new SubstraitVisitor();
        return flinkExpression.accept(substraitVisitor);
    }

    private static int calculateDepth(ResolvedExpression expr) {
        if (expr instanceof FieldReferenceExpression || expr instanceof ValueLiteralExpression) {
            return 1;
        } else if (expr instanceof CallExpression) {
            int maxDepth = 0;
            for (ResolvedExpression child : ((CallExpression) expr).getResolvedChildren()) {
                int childDepth = calculateDepth(child);
                if (childDepth > maxDepth) {
                    maxDepth = childDepth;
                }
            }
            return 1 + maxDepth;
        } else {
            return 1;
        }
    }

    public static boolean filterAllPartitionColumn(ResolvedExpression expression, Set<String> partitionCols) {
        if (expression instanceof FieldReferenceExpression) {
            return partitionCols.contains(((FieldReferenceExpression) expression).getName());
        } else if (expression instanceof CallExpression) {
            return expression.getResolvedChildren().stream().allMatch(child -> filterAllPartitionColumn(child, partitionCols));

        } else return expression instanceof ValueLiteralExpression;
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
        } else if (expression instanceof ValueLiteralExpression) {
            return false;
        }
        return false;
    }

    // check if this partition filter is equality with literal
    public static Map<String, String> equalityFilterFieldNames(List<ResolvedExpression> partitionFilters) {
        Map<String, String> fieldNames = new HashMap<>();
        for (ResolvedExpression expression : partitionFilters) {
            if (expression instanceof CallExpression) {
                CallExpression call = (CallExpression) expression;
                if (call.getFunctionDefinition() instanceof BuiltInFunctionDefinition) {
                    BuiltInFunctionDefinition functionDefinition = (BuiltInFunctionDefinition) call.getFunctionDefinition();
                    if (functionDefinition.getName().equals("equals")) {
                        List<ResolvedExpression> resolvedChildren = call.getResolvedChildren();
                        if (resolvedChildren.size() == 2) {
                            if (resolvedChildren.get(0) instanceof FieldReferenceExpression) {
                                FieldReferenceExpression fieldRef = (FieldReferenceExpression) resolvedChildren.get(0);
                                if (resolvedChildren.get(1) instanceof ValueLiteralExpression) {
                                    ValueLiteralExpression valueLiteral = (ValueLiteralExpression) resolvedChildren.get(1);
                                    fieldNames.put(fieldRef.getName(),
                                            StringUtils.strip(valueLiteral.asSummaryString(), "'"));
                                }
                            }
                        }
                    }
                }
            }
        }
        return fieldNames;
    }
}