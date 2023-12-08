// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
package org.apache.spark.sql.catalyst.parser.extensions;

import org.antlr.v4.runtime.tree.ParseTreeVisitor;

public interface LakeSoulSqlExtensionsVisitor<T> extends ParseTreeVisitor<T> {
    T visitSingleStatement(org.apache.spark.sql.catalyst.parser.extensions.LakeSoulSqlExtensionsParser.SingleStatementContext ctx);

    T visitCall(org.apache.spark.sql.catalyst.parser.extensions.LakeSoulSqlExtensionsParser.CallContext ctx);

    T visitPositionalArgument(org.apache.spark.sql.catalyst.parser.extensions.LakeSoulSqlExtensionsParser.PositionalArgumentContext ctx);

    T visitNamedArgument(org.apache.spark.sql.catalyst.parser.extensions.LakeSoulSqlExtensionsParser.NamedArgumentContext ctx);

    T visitExpression(org.apache.spark.sql.catalyst.parser.extensions.LakeSoulSqlExtensionsParser.ExpressionContext ctx);

    T visitNumericLiteral(org.apache.spark.sql.catalyst.parser.extensions.LakeSoulSqlExtensionsParser.NumericLiteralContext ctx);

    T visitBooleanLiteral(org.apache.spark.sql.catalyst.parser.extensions.LakeSoulSqlExtensionsParser.BooleanLiteralContext ctx);

    T visitStringLiteral(org.apache.spark.sql.catalyst.parser.extensions.LakeSoulSqlExtensionsParser.StringLiteralContext ctx);

    T visitTypeConstructor(org.apache.spark.sql.catalyst.parser.extensions.LakeSoulSqlExtensionsParser.TypeConstructorContext ctx);

    T visitStringMap(org.apache.spark.sql.catalyst.parser.extensions.LakeSoulSqlExtensionsParser.StringMapContext ctx);

    T visitStringArray(org.apache.spark.sql.catalyst.parser.extensions.LakeSoulSqlExtensionsParser.StringArrayContext ctx);

    T visitBooleanValue(org.apache.spark.sql.catalyst.parser.extensions.LakeSoulSqlExtensionsParser.BooleanValueContext ctx);

    T visitExponentLiteral(org.apache.spark.sql.catalyst.parser.extensions.LakeSoulSqlExtensionsParser.ExponentLiteralContext ctx);

    T visitDecimalLiteral(org.apache.spark.sql.catalyst.parser.extensions.LakeSoulSqlExtensionsParser.DecimalLiteralContext ctx);

    T visitIntegerLiteral(org.apache.spark.sql.catalyst.parser.extensions.LakeSoulSqlExtensionsParser.IntegerLiteralContext ctx);

    T visitBigIntLiteral(org.apache.spark.sql.catalyst.parser.extensions.LakeSoulSqlExtensionsParser.BigIntLiteralContext ctx);

    T visitSmallIntLiteral(org.apache.spark.sql.catalyst.parser.extensions.LakeSoulSqlExtensionsParser.SmallIntLiteralContext ctx);

    T visitTinyIntLiteral(org.apache.spark.sql.catalyst.parser.extensions.LakeSoulSqlExtensionsParser.TinyIntLiteralContext ctx);

    T visitDoubleLiteral(org.apache.spark.sql.catalyst.parser.extensions.LakeSoulSqlExtensionsParser.DoubleLiteralContext ctx);

    T visitFloatLiteral(org.apache.spark.sql.catalyst.parser.extensions.LakeSoulSqlExtensionsParser.FloatLiteralContext ctx);
    T visitBigDecimalLiteral(org.apache.spark.sql.catalyst.parser.extensions.LakeSoulSqlExtensionsParser.BigDecimalLiteralContext ctx);
    T visitMultipartIdentifier(org.apache.spark.sql.catalyst.parser.extensions.LakeSoulSqlExtensionsParser.MultipartIdentifierContext ctx);
    T visitUnquotedIdentifier(org.apache.spark.sql.catalyst.parser.extensions.LakeSoulSqlExtensionsParser.UnquotedIdentifierContext ctx);
    T visitQuotedIdentifierAlternative(org.apache.spark.sql.catalyst.parser.extensions.LakeSoulSqlExtensionsParser.QuotedIdentifierAlternativeContext ctx);
    T visitQuotedIdentifier(org.apache.spark.sql.catalyst.parser.extensions.LakeSoulSqlExtensionsParser.QuotedIdentifierContext ctx);
    T visitNonReserved(org.apache.spark.sql.catalyst.parser.extensions.LakeSoulSqlExtensionsParser.NonReservedContext ctx);

}
