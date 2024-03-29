package com.dmetasoul.lakesoul.lakesoul.io.substrait;

import io.substrait.dsl.SubstraitBuilder;
import io.substrait.expression.Expression;

import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.proto.ExpressionProtoConverter;
import io.substrait.extension.SimpleExtension;
import io.substrait.plan.Plan;
import io.substrait.plan.PlanProtoConverter;
import io.substrait.relation.NamedScan;
import io.substrait.type.Type;
import io.substrait.type.TypeCreator;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.IntervalUnit;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SubstraitUtil {
    public static final SimpleExtension.ExtensionCollection Se;
    public static final SubstraitBuilder Builder;

    public static final String CompNamespace = "/functions_comparison.yaml";
    public static final String BooleanNamespace = "/functions_boolean.yaml";

    static {
        try {
            Se = SimpleExtension.loadDefaults();
            Builder = new SubstraitBuilder(Se);
        } catch (IOException e) {
            throw new RuntimeException("load simple extension failed");
        }
    }



    public static Plan exprToFilter(Expression e, String tableName) {
        if (e == null) {
            return null;
        }
        List<String> tableNames = Stream.of(tableName).collect(Collectors.toList());
        List<String> columnNames = new ArrayList<>();
        List<Type> columnTypes = new ArrayList<>();
        NamedScan namedScan = Builder.namedScan(tableNames, columnNames, columnTypes);
        namedScan =
                NamedScan.builder()
                        .from(namedScan)
                        .filter(e)
                        .build();


        Plan.Root root = Builder.root(namedScan);
        return Builder.plan(root);
    }


    public static io.substrait.proto.Expression exprToProto(Expression expr) {
        ExpressionProtoConverter converter = new ExpressionProtoConverter(null, null);
        return expr.accept(converter);
    }


    public static io.substrait.proto.Plan planToProto(Plan plan) {
        if (plan == null) {
            return null;
        }
        return new PlanProtoConverter().toProto(plan);
    }
}

