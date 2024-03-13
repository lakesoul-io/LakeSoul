//package com.dmetasoul.lakesoul.meta.jnr;
//
//import com.dmetasoul.lakesoul.meta.substrait.SubstraitUtil;
//import io.substrait.expression.Expression;
//import io.substrait.expression.ExpressionCreator;
//import io.substrait.expression.FunctionArg;
//import io.substrait.expression.ImmutableExpression;
//import io.substrait.expression.proto.ExpressionProtoConverter;
//import io.substrait.extension.ExtensionCollector;
//import io.substrait.extension.ImmutableSimpleExtension;
//import io.substrait.extension.SimpleExtension;
//import io.substrait.type.Type;
//import io.substrait.type.TypeCreator;
//import org.apache.flink.table.expressions.*;
//import org.apache.flink.table.functions.BuiltInFunctionDefinition;
//import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
//import org.apache.flink.table.types.AtomicDataType;
//import org.apache.flink.table.types.logical.BooleanType;
//import org.apache.flink.table.types.logical.IntType;
//import org.junit.Test;
//
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.List;
//
//public class SubstraitTest {
//    @Test
//    public void generalExprTest() {
//        BuiltInFunctionDefinition equalsFunction = BuiltInFunctionDefinitions.EQUALS;
//        ValueLiteralExpression valExpr = new ValueLiteralExpression(3, new AtomicDataType(new IntType(false)));
//        FieldReferenceExpression orderId = new FieldReferenceExpression("order_id",
//                new AtomicDataType(new IntType())
//                , 0, 0);
//        List<ResolvedExpression> args = new ArrayList<>();
//        args.add(orderId);
//        args.add(valExpr);
//        CallExpression p = CallExpression.permanent(
//                equalsFunction,
//                args,
//                new AtomicDataType(new BooleanType()));
//        Expression substraitExpr = SubstraitUtil.doTransform(p);
//        io.substrait.proto.Expression proto = toProto(new ExtensionCollector(), substraitExpr);
//        NativeMetadataJavaClient.getInstance().filter_rel(proto);
//    }
//
//    @Test
//    public void literalExprTest() {
//        ValueLiteralExpression valExpr = new ValueLiteralExpression(3, new AtomicDataType(new IntType(false)));
//        Expression substraitExpr = SubstraitUtil.doTransform(valExpr);
//        System.out.println(substraitExpr);
//    }
//
//    @Test
//    public void FieldRefTest() {
//        FieldReferenceExpression orderId = new FieldReferenceExpression("order_id",
//                new AtomicDataType(new IntType())
//                , 0, 0);
//        Expression expr = SubstraitUtil.doTransform(orderId);
//        System.out.println(expr);
//        System.out.println(toProto(null, expr));
//    }
//
//    private io.substrait.proto.Expression toProto(ExtensionCollector collector, Expression expr) {
//        return expr.accept(new ExpressionProtoConverter(collector, null));
//    }
//
//    @Test
//    public void callExprTest() {
//        try {
//            SimpleExtension.ExtensionCollection extensionCollection = SimpleExtension.loadDefaults();
//            SimpleExtension.ScalarFunctionVariant desc = extensionCollection.getScalarFunction(SimpleExtension.FunctionAnchor.of("/functions_comparison.yaml", "equal:any_any"));
//            Expression.ScalarFunctionInvocation si = ExpressionCreator.scalarFunction(desc, TypeCreator.NULLABLE.I32);
//            io.substrait.proto.Expression p = toProto(new ExtensionCollector(), si);
//            System.out.println(p);
//
//
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//    }
//}
