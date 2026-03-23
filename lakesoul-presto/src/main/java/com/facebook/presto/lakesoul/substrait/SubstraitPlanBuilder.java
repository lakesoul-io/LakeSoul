package com.facebook.presto.lakesoul.substrait;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.facebook.airlift.log.Logger;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import com.dmetasoul.lakesoul.lakesoul.io.substrait.SubstraitUtil;
import io.substrait.expression.Expression;
import io.substrait.type.TypeCreator;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.lakesoul.handle.LakeSoulTableColumnHandle;
import com.fasterxml.jackson.core.JsonProcessingException;
import static com.dmetasoul.lakesoul.meta.DBConfig.LAKESOUL_NULL_STRING;


public class SubstraitPlanBuilder {

    private static final Logger log = Logger.get(SubstraitPlanBuilder.class);

    public static io.substrait.proto.Plan buildSubstraitPlan(List<FilterPredicate> parFilters, Map<String, ColumnHandle> allColumns, String tableName) throws Exception {
        if (parFilters == null || parFilters.isEmpty()) {
            return null;
        }
        Expression finalExpr = null;

        for (FilterPredicate fp : parFilters) {
            Expression expr = convertToSubstraitExpr(fp, allColumns);
            if (expr != null) {
                if (finalExpr == null) {
                    finalExpr = expr;
                } else {
                    finalExpr = SubstraitUtil.and(finalExpr, expr);
                }
            }
        }
        if (finalExpr == null) {
            return null;
        }
        log.info("buildSubstraitPlan finalExpr: " + finalExpr);
        return SubstraitUtil.substraitExprToProto(finalExpr, tableName);
    }

    public static String convertToBase64(List<FilterPredicate> fp,  Map<String, ColumnHandle> allColumns, String tableName) throws Exception {
    
        io.substrait.proto.Plan plan = buildSubstraitPlan(fp, allColumns, tableName);  
        return SubstraitUtil.encodeBase64String(plan);
    }

    public static Map<String, String> extractEqualityFilters(List<FilterPredicate> parFilters, Schema partitionSchema) {
        if (parFilters == null) {
            return null;
        }

        Map<String, String> equalityKeys = new HashMap<>();

        for (FilterPredicate fp : parFilters) {
            if (fp instanceof Operators.Eq) {
                Operators.Eq eq = (Operators.Eq) fp;
                String colName = eq.getColumn().getColumnPath().toDotString();
                Object value = eq.getValue();
                Field arrowField = partitionSchema.findField(colName);
                if (arrowField == null) {
                    return null;
                }

                String strValue;
                if(value == null  ){
                    strValue = LAKESOUL_NULL_STRING;
                } else if ((value instanceof Integer  || value instanceof Long ) && arrowField.getType() instanceof ArrowType.Date) {
                    long epochDay = ((Number) value).longValue();
                    strValue = LocalDate.ofEpochDay(epochDay).toString();
                } else if (value instanceof org.apache.parquet.io.api.Binary) {
                    strValue = ((org.apache.parquet.io.api.Binary) value).toStringUsingUTF8();
                } else {
                    strValue = String.valueOf(value);
                }

                equalityKeys.put(colName, strValue);
            
            } else {
                return null;
            }
        }
        return equalityKeys;
    }


    private static Expression convertToSubstraitExpr(FilterPredicate fp, Map<String, ColumnHandle> allColumns) throws Exception {
        if (fp instanceof Operators.Eq) {
            if(((Operators.Eq) fp).getValue() == null) {
                String columnName = ((Operators.Eq) fp).getColumn().getColumnPath().toDotString();
                LakeSoulTableColumnHandle handle = (LakeSoulTableColumnHandle) allColumns.get(columnName);
                Field arrowField = handle.getArrowField();
                Expression fieldRef = SubstraitUtil.arrowFieldToSubstraitField(arrowField);
                return SubstraitUtil.isNull(fieldRef);
            }
            return buildBinaryExpr(((Operators.Eq) fp).getColumn(), ((Operators.Eq) fp).getValue(),
                               "equal:any_any", allColumns);
        } else if(fp instanceof Operators.NotEq) {
            return buildBinaryExpr(((Operators.NotEq) fp).getColumn(), ((Operators.NotEq) fp).getValue(),
                           "not_equal:any_any", allColumns);
        } else if (fp instanceof Operators.GtEq) {
            return buildBinaryExpr(((Operators.GtEq) fp).getColumn(), ((Operators.GtEq) fp).getValue(),
                               "gte:any_any", allColumns);
        } else if (fp instanceof Operators.Lt) {
            return buildBinaryExpr(((Operators.Lt) fp).getColumn(), ((Operators.Lt) fp).getValue(),
                               "lt:any_any", allColumns);
        } else if (fp instanceof Operators.Gt) {
            return buildBinaryExpr(((Operators.Gt) fp).getColumn(), ((Operators.Gt) fp).getValue(),
                               "gt:any_any", allColumns);
        } else if (fp instanceof Operators.LtEq) {
            return buildBinaryExpr(((Operators.LtEq) fp).getColumn(), ((Operators.LtEq) fp).getValue(),
                               "lte:any_any", allColumns);
        } else if (fp instanceof Operators.And) {
            Expression left = convertToSubstraitExpr(((Operators.And) fp).getLeft(), allColumns);
            Expression right = convertToSubstraitExpr(((Operators.And) fp).getRight(), allColumns);
            return SubstraitUtil.and(left, right);
        } else if (fp instanceof Operators.Or) {
            Expression left = convertToSubstraitExpr(((Operators.Or) fp).getLeft(), allColumns);
            Expression right = convertToSubstraitExpr(((Operators.Or) fp).getRight(), allColumns);
            return SubstraitUtil.or(left, right);
        } else if(fp instanceof Operators.Not) {
            FilterPredicate innerPredicate = ((Operators.Not) fp).getPredicate();
            if(innerPredicate instanceof Operators.Eq && ((Operators.Eq) innerPredicate).getValue() == null) {// is not null
                String columnName = ((Operators.Eq) innerPredicate).getColumn().getColumnPath().toDotString(); ;
                LakeSoulTableColumnHandle handle = (LakeSoulTableColumnHandle) allColumns.get(columnName);
                Field arrowField = handle.getArrowField();
                Expression fieldRef = SubstraitUtil.arrowFieldToSubstraitField(arrowField);
                return SubstraitUtil.notNull(fieldRef);
            }
            Expression innerExpr = convertToSubstraitExpr(innerPredicate, allColumns);
            return (innerExpr != null ) ? SubstraitUtil.not(innerExpr) : null;
        }
        log.info("convertToSubstraitExpr Unsupported FilterPredicate:" + fp.getClass().getName());
        return null;
    }

    private static Expression buildBinaryExpr(Operators.Column column, Object value, String funcKey,  Map<String, ColumnHandle> allColumns) throws Exception {
        String columnName = column.getColumnPath().toDotString();
        LakeSoulTableColumnHandle handle = (LakeSoulTableColumnHandle) allColumns.get(columnName);
        org.apache.arrow.vector.types.pojo.Field arrowField = null;
        try {
            arrowField = handle.getArrowField();
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Fatal Error: Failed to parse Arrow Schema for column: " + columnName, e);
        }
        if (arrowField == null) {
            return null;
        }
        Object realValue = value;
        if (value instanceof org.apache.parquet.io.api.Binary) {
            realValue = ((org.apache.parquet.io.api.Binary) value).toStringUsingUTF8();
        }
        io.substrait.type.Type subType = SubstraitUtil.arrowFieldToSubstraitType(arrowField);
        Expression literal;
        //if(realValue == null) {
        //    io.substrait.type.Type nullableSubType = io.substrait.type.TypeCreator.asNullable(subType); 
        //    literal = SubstraitUtil.typedNull(nullableSubType);
        //} else {
        literal = SubstraitUtil.anyToSubstraitLiteral(subType, realValue);
        //}
        Expression fieldRef = SubstraitUtil.arrowFieldToSubstraitField(arrowField);
        log.info("buildBinaryExpr Successfully built binary expr for columnName = " + columnName + ", fieldRef = " + fieldRef + ", literal = " + literal + ", subType = " + subType);
        return SubstraitUtil.makeBinary(
                fieldRef,
                literal,
                SubstraitUtil.CompNamespace,
                funcKey,
                subType
        );
    }

}
