package org.apache.flink.lakesoul.substrait;

import static com.dmetasoul.lakesoul.lakesoul.io.substrait.SubstraitUtil.and;

import io.substrait.expression.Expression;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class SubstraitFlinkUtil {

    public static Tuple2<SupportsFilterPushDown.Result, Expression> flinkExprToSubStraitExpr(
            List<ResolvedExpression> filters) {
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

    /**
     * Replaces query-start time functions with literals so that both row-level filters and
     * partition filters can be converted to Substrait.
     *
     * <p>This is only used for batch scans. Streaming scans intentionally keep these expressions
     * and let Flink evaluate them.
     */
    public static List<ResolvedExpression> foldQueryStartFunctions(
            List<ResolvedExpression> filters, ZoneId zoneId) {
        List<ResolvedExpression> folded = new ArrayList<>(filters.size());
        for (ResolvedExpression filter : filters) {
            folded.add(foldQueryStartFunctions(filter, zoneId));
        }
        return folded;
    }

    public static ResolvedExpression foldQueryStartFunctions(
            ResolvedExpression expression, ZoneId zoneId) {
        return foldQueryStartFunctions(expression, Clock.system(zoneId));
    }

    static ResolvedExpression foldQueryStartFunctions(ResolvedExpression expression, Clock clock) {
        if (!(expression instanceof CallExpression)) {
            return expression;
        }
        CallExpression call = (CallExpression) expression;

        ValueLiteralExpression folded = evaluateQueryStartFunction(call, clock);
        if (folded != null) {
            return folded;
        }

        List<ResolvedExpression> children = call.getResolvedChildren();
        List<ResolvedExpression> foldedChildren = new ArrayList<>(children.size());
        boolean changed = false;
        for (ResolvedExpression child : children) {
            ResolvedExpression foldedChild = foldQueryStartFunctions(child, clock);
            foldedChildren.add(foldedChild);
            changed |= foldedChild != child;
        }
        if (!changed) {
            return call;
        }
        return new CallExpression(
                call.isTemporary(),
                call.getFunctionIdentifier().orElse(null),
                call.getFunctionDefinition(),
                foldedChildren,
                call.getOutputDataType());
    }

    private static ValueLiteralExpression evaluateQueryStartFunction(
            CallExpression call, Clock clock) {
        if (!call.getResolvedChildren().isEmpty()) {
            return null;
        }

        DataType outputType = call.getOutputDataType();
        if (call.getFunctionDefinition().equals(BuiltInFunctionDefinitions.CURRENT_DATE)) {
            return new ValueLiteralExpression(LocalDate.now(clock), outputType.notNull());
        } else if (call.getFunctionDefinition().equals(BuiltInFunctionDefinitions.CURRENT_TIME)
                || call.getFunctionDefinition().equals(BuiltInFunctionDefinitions.LOCAL_TIME)) {
            return new ValueLiteralExpression(
                    truncate(LocalTime.now(clock), getPrecision(outputType.getLogicalType())),
                    outputType.notNull());
        } else if (call.getFunctionDefinition().equals(BuiltInFunctionDefinitions.CURRENT_TIMESTAMP)
                || call.getFunctionDefinition().equals(BuiltInFunctionDefinitions.NOW)) {
            return new ValueLiteralExpression(
                    truncate(Instant.now(clock), getPrecision(outputType.getLogicalType())),
                    outputType.notNull());
        } else if (call.getFunctionDefinition()
                .equals(BuiltInFunctionDefinitions.LOCAL_TIMESTAMP)) {
            return new ValueLiteralExpression(
                    truncate(LocalDateTime.now(clock), getPrecision(outputType.getLogicalType())),
                    outputType.notNull());
        } else if (call.getFunctionDefinition().equals(BuiltInFunctionDefinitions.UNIX_TIMESTAMP)) {
            return new ValueLiteralExpression(
                    Instant.now(clock).getEpochSecond(), outputType.notNull());
        }
        return null;
    }

    private static int getPrecision(LogicalType logicalType) {
        if (logicalType instanceof TimeType) {
            return ((TimeType) logicalType).getPrecision();
        } else if (logicalType instanceof TimestampType) {
            return ((TimestampType) logicalType).getPrecision();
        } else if (logicalType instanceof LocalZonedTimestampType) {
            return ((LocalZonedTimestampType) logicalType).getPrecision();
        }
        return 3;
    }

    private static LocalTime truncate(LocalTime time, int precision) {
        return time.withNano(truncateNanos(time.getNano(), precision));
    }

    private static LocalDateTime truncate(LocalDateTime time, int precision) {
        return time.withNano(truncateNanos(time.getNano(), precision));
    }

    private static Instant truncate(Instant time, int precision) {
        return time.truncatedTo(precisionUnit(precision));
    }

    private static int truncateNanos(int nanos, int precision) {
        if (precision >= 9) {
            return nanos;
        }
        int factor = (int) Math.pow(10, 9 - precision);
        return (nanos / factor) * factor;
    }

    private static ChronoUnit precisionUnit(int precision) {
        if (precision >= 9) {
            return ChronoUnit.NANOS;
        } else if (precision >= 6) {
            return ChronoUnit.MICROS;
        } else if (precision >= 3) {
            return ChronoUnit.MILLIS;
        }
        return ChronoUnit.SECONDS;
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

    public static boolean filterAllPartitionColumn(
            ResolvedExpression expression, Set<String> partitionCols) {
        if (expression instanceof FieldReferenceExpression) {
            return partitionCols.contains(((FieldReferenceExpression) expression).getName());
        } else if (expression instanceof CallExpression) {
            return expression.getResolvedChildren().stream()
                    .allMatch(child -> filterAllPartitionColumn(child, partitionCols));

        } else return expression instanceof ValueLiteralExpression;
    }

    public static boolean filterContainsPartitionColumn(
            ResolvedExpression expression, Set<String> partitionCols) {
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
    public static Map<String, String> equalityFilterFieldNames(
            List<ResolvedExpression> partitionFilters) {
        Map<String, String> fieldNames = new HashMap<>();
        for (ResolvedExpression expression : partitionFilters) {
            if (expression instanceof CallExpression) {
                CallExpression call = (CallExpression) expression;
                if (call.getFunctionDefinition() instanceof BuiltInFunctionDefinition) {
                    BuiltInFunctionDefinition functionDefinition =
                            (BuiltInFunctionDefinition) call.getFunctionDefinition();
                    if (functionDefinition.getName().equals("equals")) {
                        List<ResolvedExpression> resolvedChildren = call.getResolvedChildren();
                        if (resolvedChildren.size() == 2) {
                            if (resolvedChildren.get(0) instanceof FieldReferenceExpression) {
                                FieldReferenceExpression fieldRef =
                                        (FieldReferenceExpression) resolvedChildren.get(0);
                                if (resolvedChildren.get(1) instanceof ValueLiteralExpression) {
                                    ValueLiteralExpression valueLiteral =
                                            (ValueLiteralExpression) resolvedChildren.get(1);
                                    fieldNames.put(
                                            fieldRef.getName(),
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
