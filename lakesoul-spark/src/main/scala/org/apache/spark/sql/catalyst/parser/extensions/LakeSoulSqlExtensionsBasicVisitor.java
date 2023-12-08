// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
package org.apache.spark.sql.catalyst.parser.extensions;

import org.antlr.v4.runtime.tree.AbstractParseTreeVisitor;

public class LakeSoulSqlExtensionsBasicVisitor<T> extends AbstractParseTreeVisitor<T> implements LakeSoulSqlExtensionsVisitor<T>{
    @Override
    public T visitSingleStatement(LakeSoulSqlExtensionsParser.SingleStatementContext ctx) {
        return visitChildren(ctx); 
    }

    @Override
    public T visitCall(LakeSoulSqlExtensionsParser.CallContext ctx) {
        return visitChildren(ctx); 
    }

    @Override
    public T visitPositionalArgument(LakeSoulSqlExtensionsParser.PositionalArgumentContext ctx) {
        return visitChildren(ctx); 
    }

    @Override
    public T visitNamedArgument(LakeSoulSqlExtensionsParser.NamedArgumentContext ctx) {
        return visitChildren(ctx); 
    }

    @Override
    public T visitExpression(LakeSoulSqlExtensionsParser.ExpressionContext ctx) {
        return visitChildren(ctx); 
    }

    @Override
    public T visitNumericLiteral(LakeSoulSqlExtensionsParser.NumericLiteralContext ctx) {
        return visitChildren(ctx); 
    }

    @Override
    public T visitBooleanLiteral(LakeSoulSqlExtensionsParser.BooleanLiteralContext ctx) {
        return visitChildren(ctx); 
    }

    @Override
    public T visitStringLiteral(LakeSoulSqlExtensionsParser.StringLiteralContext ctx) {
        return visitChildren(ctx); 
    }

    @Override
    public T visitTypeConstructor(LakeSoulSqlExtensionsParser.TypeConstructorContext ctx) {
        return visitChildren(ctx); 
    }

    @Override
    public T visitStringMap(LakeSoulSqlExtensionsParser.StringMapContext ctx) {
        return visitChildren(ctx); 
    }

    @Override
    public T visitStringArray(LakeSoulSqlExtensionsParser.StringArrayContext ctx) {
        return visitChildren(ctx); 
    }

    @Override
    public T visitBooleanValue(LakeSoulSqlExtensionsParser.BooleanValueContext ctx) {
        return visitChildren(ctx); 
    }

    @Override
    public T visitExponentLiteral(LakeSoulSqlExtensionsParser.ExponentLiteralContext ctx) {
        return visitChildren(ctx); 
    }

    @Override
    public T visitDecimalLiteral(LakeSoulSqlExtensionsParser.DecimalLiteralContext ctx) {
        return visitChildren(ctx); 
    }

    @Override
    public T visitIntegerLiteral(LakeSoulSqlExtensionsParser.IntegerLiteralContext ctx) {
        return visitChildren(ctx); 
    }

    @Override
    public T visitBigIntLiteral(LakeSoulSqlExtensionsParser.BigIntLiteralContext ctx) {
        return visitChildren(ctx); 
    }

    @Override
    public T visitSmallIntLiteral(LakeSoulSqlExtensionsParser.SmallIntLiteralContext ctx) {
        return visitChildren(ctx); 
    }

    @Override
    public T visitTinyIntLiteral(LakeSoulSqlExtensionsParser.TinyIntLiteralContext ctx) {
        return visitChildren(ctx); 
    }

    @Override
    public T visitDoubleLiteral(LakeSoulSqlExtensionsParser.DoubleLiteralContext ctx) {
        return visitChildren(ctx); 
    }

    @Override
    public T visitFloatLiteral(LakeSoulSqlExtensionsParser.FloatLiteralContext ctx) {
        return visitChildren(ctx); 
    }

    @Override
    public T visitBigDecimalLiteral(LakeSoulSqlExtensionsParser.BigDecimalLiteralContext ctx) {
        return visitChildren(ctx); 
    }

    @Override
    public T visitMultipartIdentifier(LakeSoulSqlExtensionsParser.MultipartIdentifierContext ctx) {
        return visitChildren(ctx); 
    }

    @Override
    public T visitUnquotedIdentifier(LakeSoulSqlExtensionsParser.UnquotedIdentifierContext ctx) {
        return visitChildren(ctx); 
    }

    @Override
    public T visitQuotedIdentifierAlternative(LakeSoulSqlExtensionsParser.QuotedIdentifierAlternativeContext ctx) {
        return visitChildren(ctx); 
    }

    @Override
    public T visitQuotedIdentifier(LakeSoulSqlExtensionsParser.QuotedIdentifierContext ctx) {
        return visitChildren(ctx); 
    }

    @Override
    public T visitNonReserved(LakeSoulSqlExtensionsParser.NonReservedContext ctx) {
        return visitChildren(ctx); 
    }
}
