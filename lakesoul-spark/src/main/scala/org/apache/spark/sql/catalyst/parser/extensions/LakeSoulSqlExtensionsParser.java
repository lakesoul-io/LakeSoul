// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
package org.apache.spark.sql.catalyst.parser.extensions;

import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;
@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class LakeSoulSqlExtensionsParser extends Parser {
    static { RuntimeMetaData.checkVersion("4.8", RuntimeMetaData.VERSION); }

    protected static final DFA[] _decisionToDFA;
    protected static final PredictionContextCache _sharedContextCache =
            new PredictionContextCache();
    public static final int
            T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, ADD=6, ALTER=7, AS=8, ASC=9, BRANCH=10,
            BY=11, CALL=12, DAYS=13, DESC=14, DISTRIBUTED=15, DROP=16, EXISTS=17,
            FIELD=18, FIELDS=19, FIRST=20, HOURS=21, IF=22, LAST=23, LOCALLY=24, MINUTES=25,
            MONTHS=26, CREATE=27, NOT=28, NULLS=29, OF=30, OR=31, ORDERED=32, PARTITION=33,
            REPLACE=34, RETAIN=35, RETENTION=36, IDENTIFIER_KW=37, SET=38, SNAPSHOT=39,
            SNAPSHOTS=40, TABLE=41, TAG=42, UNORDERED=43, VERSION=44, WITH=45, WRITE=46,
            TRUE=47, FALSE=48, MAP=49, ARRAY=50, PLUS=51, MINUS=52, STRING=53, BIGINT_LITERAL=54,
            SMALLINT_LITERAL=55, TINYINT_LITERAL=56, INTEGER_VALUE=57, EXPONENT_VALUE=58,
            DECIMAL_VALUE=59, FLOAT_LITERAL=60, DOUBLE_LITERAL=61, BIGDECIMAL_LITERAL=62,
            IDENTIFIER=63, BACKQUOTED_IDENTIFIER=64, SIMPLE_COMMENT=65, BRACKETED_COMMENT=66,
            WS=67, UNRECOGNIZED=68;
    public static final int
            RULE_singleStatement = 0, RULE_statement = 1, RULE_createReplaceTagClause = 2,
            RULE_createReplaceBranchClause = 3, RULE_tagOptions = 4, RULE_branchOptions = 5,
            RULE_snapshotRetention = 6, RULE_refRetain = 7, RULE_maxSnapshotAge = 8,
            RULE_minSnapshotsToKeep = 9, RULE_writeSpec = 10, RULE_writeDistributionSpec = 11,
            RULE_writeOrderingSpec = 12, RULE_callArgument = 13, RULE_singleOrder = 14,
            RULE_order = 15, RULE_orderField = 16, RULE_transform = 17, RULE_transformArgument = 18,
            RULE_expression = 19, RULE_constant = 20, RULE_stringMap = 21, RULE_stringArray = 22,
            RULE_booleanValue = 23, RULE_number = 24, RULE_multipartIdentifier = 25,
            RULE_identifier = 26, RULE_quotedIdentifier = 27, RULE_fieldList = 28,
            RULE_nonReserved = 29, RULE_snapshotId = 30, RULE_numSnapshots = 31, RULE_timeUnit = 32;
    private static String[] makeRuleNames() {
        return new String[] {
                "singleStatement", "statement", "createReplaceTagClause", "createReplaceBranchClause",
                "tagOptions", "branchOptions", "snapshotRetention", "refRetain", "maxSnapshotAge",
                "minSnapshotsToKeep", "writeSpec", "writeDistributionSpec", "writeOrderingSpec",
                "callArgument", "singleOrder", "order", "orderField", "transform", "transformArgument",
                "expression", "constant", "stringMap", "stringArray", "booleanValue",
                "number", "multipartIdentifier", "identifier", "quotedIdentifier", "fieldList",
                "nonReserved", "snapshotId", "numSnapshots", "timeUnit"
        };
    }
    public static final String[] ruleNames = makeRuleNames();

    private static String[] makeLiteralNames() {
        return new String[] {
                null, "'('", "','", "')'", "'=>'", "'.'", "'ADD'", "'ALTER'", "'AS'",
                "'ASC'", "'BRANCH'", "'BY'", "'CALL'", "'DAYS'", "'DESC'", "'DISTRIBUTED'",
                "'DROP'", "'EXISTS'", "'FIELD'", "'FIELDS'", "'FIRST'", "'HOURS'", "'IF'",
                "'LAST'", "'LOCALLY'", "'MINUTES'", "'MONTHS'", "'CREATE'", "'NOT'",
                "'NULLS'", "'OF'", "'OR'", "'ORDERED'", "'PARTITION'", "'REPLACE'", "'RETAIN'",
                "'RETENTION'", "'IDENTIFIER'", "'SET'", "'SNAPSHOT'", "'SNAPSHOTS'",
                "'TABLE'", "'TAG'", "'UNORDERED'", "'VERSION'", "'WITH'", "'WRITE'",
                "'TRUE'", "'FALSE'", "'MAP'", "'ARRAY'", "'+'", "'-'"
        };
    }
    private static final String[] _LITERAL_NAMES = makeLiteralNames();
    private static String[] makeSymbolicNames() {
        return new String[] {
                null, null, null, null, null, null, "ADD", "ALTER", "AS", "ASC", "BRANCH",
                "BY", "CALL", "DAYS", "DESC", "DISTRIBUTED", "DROP", "EXISTS", "FIELD",
                "FIELDS", "FIRST", "HOURS", "IF", "LAST", "LOCALLY", "MINUTES", "MONTHS",
                "CREATE", "NOT", "NULLS", "OF", "OR", "ORDERED", "PARTITION", "REPLACE",
                "RETAIN", "RETENTION", "IDENTIFIER_KW", "SET", "SNAPSHOT", "SNAPSHOTS",
                "TABLE", "TAG", "UNORDERED", "VERSION", "WITH", "WRITE", "TRUE", "FALSE",
                "MAP", "ARRAY", "PLUS", "MINUS", "STRING", "BIGINT_LITERAL", "SMALLINT_LITERAL",
                "TINYINT_LITERAL", "INTEGER_VALUE", "EXPONENT_VALUE", "DECIMAL_VALUE",
                "FLOAT_LITERAL", "DOUBLE_LITERAL", "BIGDECIMAL_LITERAL", "IDENTIFIER",
                "BACKQUOTED_IDENTIFIER", "SIMPLE_COMMENT", "BRACKETED_COMMENT", "WS",
                "UNRECOGNIZED"
        };
    }
    private static final String[] _SYMBOLIC_NAMES = makeSymbolicNames();
    public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

    /**
     * @deprecated Use {@link #VOCABULARY} instead.
     */
    @Deprecated
    public static final String[] tokenNames;
    static {
        tokenNames = new String[_SYMBOLIC_NAMES.length];
        for (int i = 0; i < tokenNames.length; i++) {
            tokenNames[i] = VOCABULARY.getLiteralName(i);
            if (tokenNames[i] == null) {
                tokenNames[i] = VOCABULARY.getSymbolicName(i);
            }

            if (tokenNames[i] == null) {
                tokenNames[i] = "<INVALID>";
            }
        }
    }

    @Override
    @Deprecated
    public String[] getTokenNames() {
        return tokenNames;
    }

    @Override

    public Vocabulary getVocabulary() {
        return VOCABULARY;
    }

    @Override
    public String getGrammarFileName() { return "LakeSoulSqlExtensions.g4"; }

    @Override
    public String[] getRuleNames() { return ruleNames; }

    @Override
    public String getSerializedATN() { return _serializedATN; }

    @Override
    public ATN getATN() { return _ATN; }

    public LakeSoulSqlExtensionsParser(TokenStream input) {
        super(input);
        _interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
    }
    public static class StatementContext extends ParserRuleContext {
        public StatementContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }
        @Override public int getRuleIndex() { return RULE_statement; }

        public StatementContext() { }
        public void copyFrom(StatementContext ctx) {
            super.copyFrom(ctx);
        }
    }
    public static class CallContext extends StatementContext {
        public TerminalNode CALL() { return getToken(LakeSoulSqlExtensionsParser.CALL, 0); }
        public MultipartIdentifierContext multipartIdentifier() {
            return getRuleContext(MultipartIdentifierContext.class,0);
        }
        public List<CallArgumentContext> callArgument() {
            return getRuleContexts(CallArgumentContext.class);
        }
        public CallArgumentContext callArgument(int i) {
            return getRuleContext(CallArgumentContext.class,i);
        }
        public CallContext(StatementContext ctx) { copyFrom(ctx); }
        @Override
        public void enterRule(ParseTreeListener listener) {
            if ( listener instanceof LakeSoulSqlExtensionsListener ) ((LakeSoulSqlExtensionsListener)listener).enterCall(this);
        }
        @Override
        public void exitRule(ParseTreeListener listener) {
            if ( listener instanceof LakeSoulSqlExtensionsListener ) ((LakeSoulSqlExtensionsListener)listener).exitCall(this);
        }
        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if ( visitor instanceof LakeSoulSqlExtensionsVisitor ) return ((LakeSoulSqlExtensionsVisitor<? extends T>)visitor).visitCall(this);
            else return visitor.visitChildren(this);
        }
    }
    public static class SingleStatementContext extends ParserRuleContext {
        public StatementContext statement() {
            return getRuleContext(StatementContext.class,0);
        }
        public TerminalNode EOF() { return getToken(LakeSoulSqlExtensionsParser.EOF, 0); }
        public SingleStatementContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }
        @Override public int getRuleIndex() { return RULE_singleStatement; }
        @Override
        public void enterRule(ParseTreeListener listener) {
            if ( listener instanceof LakeSoulSqlExtensionsListener ) ((LakeSoulSqlExtensionsListener)listener).enterSingleStatement(this);
        }
        @Override
        public void exitRule(ParseTreeListener listener) {
            if ( listener instanceof LakeSoulSqlExtensionsListener ) ((LakeSoulSqlExtensionsListener)listener).exitSingleStatement(this);
        }
        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if ( visitor instanceof LakeSoulSqlExtensionsVisitor ) return ((LakeSoulSqlExtensionsVisitor<? extends T>)visitor).visitSingleStatement(this);
            else return visitor.visitChildren(this);
        }
    }
    public final SingleStatementContext singleStatement() throws RecognitionException {
        SingleStatementContext _localctx = new SingleStatementContext(_ctx, getState());
        enterRule(_localctx, 0, RULE_singleStatement);
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(66);
                statement();
                setState(67);
                match(EOF);
            }
        }
        catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        }
        finally {
            exitRule();
        }
        return _localctx;
    }
    public static class CallArgumentContext extends ParserRuleContext {
        public CallArgumentContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }
        @Override public int getRuleIndex() { return RULE_callArgument; }

        public CallArgumentContext() { }
        public void copyFrom(CallArgumentContext ctx) {
            super.copyFrom(ctx);
        }
    }
    public static class ConstantContext extends ParserRuleContext {
        public ConstantContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }
        @Override public int getRuleIndex() { return RULE_constant; }

        public ConstantContext() { }
        public void copyFrom(ConstantContext ctx) {
            super.copyFrom(ctx);
        }
    }
    public static class StringMapContext extends ParserRuleContext {
        public TerminalNode MAP() { return getToken(LakeSoulSqlExtensionsParser.MAP, 0); }
        public List<ConstantContext> constant() {
            return getRuleContexts(ConstantContext.class);
        }
        public ConstantContext constant(int i) {
            return getRuleContext(ConstantContext.class,i);
        }
        public StringMapContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }
        @Override public int getRuleIndex() { return RULE_stringMap; }
        @Override
        public void enterRule(ParseTreeListener listener) {
            if ( listener instanceof LakeSoulSqlExtensionsListener ) ((LakeSoulSqlExtensionsListener)listener).enterStringMap(this);
        }
        @Override
        public void exitRule(ParseTreeListener listener) {
            if ( listener instanceof LakeSoulSqlExtensionsListener ) ((LakeSoulSqlExtensionsListener)listener).exitStringMap(this);
        }
        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if ( visitor instanceof LakeSoulSqlExtensionsVisitor ) return ((LakeSoulSqlExtensionsVisitor<? extends T>)visitor).visitStringMap(this);
            else return visitor.visitChildren(this);
        }
    }

    public final StringMapContext stringMap() throws RecognitionException {
        StringMapContext _localctx = new StringMapContext(_ctx, getState());
        enterRule(_localctx, 42, RULE_stringMap);
        int _la;
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(356);
                match(MAP);
                setState(357);
                match(T__0);
                setState(358);
                constant();
                setState(363);
                _errHandler.sync(this);
                _la = _input.LA(1);
                while (_la==T__1) {
                    {
                        {
                            setState(359);
                            match(T__1);
                            setState(360);
                            constant();
                        }
                    }
                    setState(365);
                    _errHandler.sync(this);
                    _la = _input.LA(1);
                }
                setState(366);
                match(T__2);
            }
        }
        catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        }
        finally {
            exitRule();
        }
        return _localctx;
    }

    public static class StringArrayContext extends ParserRuleContext {
        public TerminalNode ARRAY() { return getToken(LakeSoulSqlExtensionsParser.ARRAY, 0); }
        public List<ConstantContext> constant() {
            return getRuleContexts(ConstantContext.class);
        }
        public ConstantContext constant(int i) {
            return getRuleContext(ConstantContext.class,i);
        }
        public StringArrayContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }
        @Override public int getRuleIndex() { return RULE_stringArray; }
        @Override
        public void enterRule(ParseTreeListener listener) {
            if ( listener instanceof LakeSoulSqlExtensionsListener ) ((LakeSoulSqlExtensionsListener)listener).enterStringArray(this);
        }
        @Override
        public void exitRule(ParseTreeListener listener) {
            if ( listener instanceof LakeSoulSqlExtensionsListener ) ((LakeSoulSqlExtensionsListener)listener).exitStringArray(this);
        }
        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if ( visitor instanceof LakeSoulSqlExtensionsVisitor ) return ((LakeSoulSqlExtensionsVisitor<? extends T>)visitor).visitStringArray(this);
            else return visitor.visitChildren(this);
        }
    }
    public static class TypeConstructorContext extends ConstantContext {
        public IdentifierContext identifier() {
            return getRuleContext(IdentifierContext.class,0);
        }
        public TerminalNode STRING() { return getToken(LakeSoulSqlExtensionsParser.STRING, 0); }
        public TypeConstructorContext(ConstantContext ctx) { copyFrom(ctx); }
        @Override
        public void enterRule(ParseTreeListener listener) {
            if ( listener instanceof LakeSoulSqlExtensionsListener ) ((LakeSoulSqlExtensionsListener)listener).enterTypeConstructor(this);
        }
        @Override
        public void exitRule(ParseTreeListener listener) {
            if ( listener instanceof LakeSoulSqlExtensionsListener ) ((LakeSoulSqlExtensionsListener)listener).exitTypeConstructor(this);
        }
        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if ( visitor instanceof LakeSoulSqlExtensionsVisitor ) return ((LakeSoulSqlExtensionsVisitor<? extends T>)visitor).visitTypeConstructor(this);
            else return visitor.visitChildren(this);
        }
    }
    public static class IdentifierContext extends ParserRuleContext {
        public IdentifierContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }
        @Override public int getRuleIndex() { return RULE_identifier; }

        public IdentifierContext() { }
        public void copyFrom(IdentifierContext ctx) {
            super.copyFrom(ctx);
        }
    }
    public static class NumberContext extends ParserRuleContext {
        public NumberContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }
        @Override public int getRuleIndex() { return RULE_number; }

        public NumberContext() { }
        public void copyFrom(NumberContext ctx) {
            super.copyFrom(ctx);
        }
    }
    public static class NumericLiteralContext extends ConstantContext {
        public NumberContext number() {
            return getRuleContext(NumberContext.class,0);
        }
        public NumericLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
        @Override
        public void enterRule(ParseTreeListener listener) {
            if ( listener instanceof LakeSoulSqlExtensionsListener ) ((LakeSoulSqlExtensionsListener)listener).enterNumericLiteral(this);
        }
        @Override
        public void exitRule(ParseTreeListener listener) {
            if ( listener instanceof LakeSoulSqlExtensionsListener ) ((LakeSoulSqlExtensionsListener)listener).exitNumericLiteral(this);
        }
        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if ( visitor instanceof LakeSoulSqlExtensionsVisitor ) return ((LakeSoulSqlExtensionsVisitor<? extends T>)visitor).visitNumericLiteral(this);
            else return visitor.visitChildren(this);
        }
    }


    public static class DecimalLiteralContext extends NumberContext {
        public TerminalNode DECIMAL_VALUE() { return getToken(LakeSoulSqlExtensionsParser.DECIMAL_VALUE, 0); }
        public TerminalNode MINUS() { return getToken(LakeSoulSqlExtensionsParser.MINUS, 0); }
        public DecimalLiteralContext(NumberContext ctx) { copyFrom(ctx); }
        @Override
        public void enterRule(ParseTreeListener listener) {
            if ( listener instanceof LakeSoulSqlExtensionsListener ) ((LakeSoulSqlExtensionsListener)listener).enterDecimalLiteral(this);
        }
        @Override
        public void exitRule(ParseTreeListener listener) {
            if ( listener instanceof LakeSoulSqlExtensionsListener ) ((LakeSoulSqlExtensionsListener)listener).exitDecimalLiteral(this);
        }
        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if ( visitor instanceof LakeSoulSqlExtensionsVisitor ) return ((LakeSoulSqlExtensionsVisitor<? extends T>)visitor).visitDecimalLiteral(this);
            else return visitor.visitChildren(this);
        }
    }
    public static class BigIntLiteralContext extends NumberContext {
        public TerminalNode BIGINT_LITERAL() { return getToken(LakeSoulSqlExtensionsParser.BIGINT_LITERAL, 0); }
        public TerminalNode MINUS() { return getToken(LakeSoulSqlExtensionsParser.MINUS, 0); }
        public BigIntLiteralContext(NumberContext ctx) { copyFrom(ctx); }
        @Override
        public void enterRule(ParseTreeListener listener) {
            if ( listener instanceof LakeSoulSqlExtensionsListener ) ((LakeSoulSqlExtensionsListener)listener).enterBigIntLiteral(this);
        }
        @Override
        public void exitRule(ParseTreeListener listener) {
            if ( listener instanceof LakeSoulSqlExtensionsListener ) ((LakeSoulSqlExtensionsListener)listener).exitBigIntLiteral(this);
        }
        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if ( visitor instanceof LakeSoulSqlExtensionsVisitor ) return ((LakeSoulSqlExtensionsVisitor<? extends T>)visitor).visitBigIntLiteral(this);
            else return visitor.visitChildren(this);
        }
    }
    public static class TinyIntLiteralContext extends NumberContext {
        public TerminalNode TINYINT_LITERAL() { return getToken(LakeSoulSqlExtensionsParser.TINYINT_LITERAL, 0); }
        public TerminalNode MINUS() { return getToken(LakeSoulSqlExtensionsParser.MINUS, 0); }
        public TinyIntLiteralContext(NumberContext ctx) { copyFrom(ctx); }
        @Override
        public void enterRule(ParseTreeListener listener) {
            if ( listener instanceof LakeSoulSqlExtensionsListener ) ((LakeSoulSqlExtensionsListener)listener).enterTinyIntLiteral(this);
        }
        @Override
        public void exitRule(ParseTreeListener listener) {
            if ( listener instanceof LakeSoulSqlExtensionsListener ) ((LakeSoulSqlExtensionsListener)listener).exitTinyIntLiteral(this);
        }
        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if ( visitor instanceof LakeSoulSqlExtensionsVisitor ) return ((LakeSoulSqlExtensionsVisitor<? extends T>)visitor).visitTinyIntLiteral(this);
            else return visitor.visitChildren(this);
        }
    }
    public static class BigDecimalLiteralContext extends NumberContext {
        public TerminalNode BIGDECIMAL_LITERAL() { return getToken(LakeSoulSqlExtensionsParser.BIGDECIMAL_LITERAL, 0); }
        public TerminalNode MINUS() { return getToken(LakeSoulSqlExtensionsParser.MINUS, 0); }
        public BigDecimalLiteralContext(NumberContext ctx) { copyFrom(ctx); }
        @Override
        public void enterRule(ParseTreeListener listener) {
            if ( listener instanceof LakeSoulSqlExtensionsListener ) ((LakeSoulSqlExtensionsListener)listener).enterBigDecimalLiteral(this);
        }
        @Override
        public void exitRule(ParseTreeListener listener) {
            if ( listener instanceof LakeSoulSqlExtensionsListener ) ((LakeSoulSqlExtensionsListener)listener).exitBigDecimalLiteral(this);
        }
        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if ( visitor instanceof LakeSoulSqlExtensionsVisitor ) return ((LakeSoulSqlExtensionsVisitor<? extends T>)visitor).visitBigDecimalLiteral(this);
            else return visitor.visitChildren(this);
        }
    }
    public static class ExponentLiteralContext extends NumberContext {
        public TerminalNode EXPONENT_VALUE() { return getToken(LakeSoulSqlExtensionsParser.EXPONENT_VALUE, 0); }
        public TerminalNode MINUS() { return getToken(LakeSoulSqlExtensionsParser.MINUS, 0); }
        public ExponentLiteralContext(NumberContext ctx) { copyFrom(ctx); }
        @Override
        public void enterRule(ParseTreeListener listener) {
            if ( listener instanceof LakeSoulSqlExtensionsListener ) ((LakeSoulSqlExtensionsListener)listener).enterExponentLiteral(this);
        }
        @Override
        public void exitRule(ParseTreeListener listener) {
            if ( listener instanceof LakeSoulSqlExtensionsListener ) ((LakeSoulSqlExtensionsListener)listener).exitExponentLiteral(this);
        }
        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if ( visitor instanceof LakeSoulSqlExtensionsVisitor ) return ((LakeSoulSqlExtensionsVisitor<? extends T>)visitor).visitExponentLiteral(this);
            else return visitor.visitChildren(this);
        }
    }
    public static class DoubleLiteralContext extends NumberContext {
        public TerminalNode DOUBLE_LITERAL() { return getToken(LakeSoulSqlExtensionsParser.DOUBLE_LITERAL, 0); }
        public TerminalNode MINUS() { return getToken(LakeSoulSqlExtensionsParser.MINUS, 0); }
        public DoubleLiteralContext(NumberContext ctx) { copyFrom(ctx); }
        @Override
        public void enterRule(ParseTreeListener listener) {
            if ( listener instanceof LakeSoulSqlExtensionsListener ) ((LakeSoulSqlExtensionsListener)listener).enterDoubleLiteral(this);
        }
        @Override
        public void exitRule(ParseTreeListener listener) {
            if ( listener instanceof LakeSoulSqlExtensionsListener ) ((LakeSoulSqlExtensionsListener)listener).exitDoubleLiteral(this);
        }
        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if ( visitor instanceof LakeSoulSqlExtensionsVisitor ) return ((LakeSoulSqlExtensionsVisitor<? extends T>)visitor).visitDoubleLiteral(this);
            else return visitor.visitChildren(this);
        }
    }
    public static class IntegerLiteralContext extends NumberContext {
        public TerminalNode INTEGER_VALUE() { return getToken(LakeSoulSqlExtensionsParser.INTEGER_VALUE, 0); }
        public TerminalNode MINUS() { return getToken(LakeSoulSqlExtensionsParser.MINUS, 0); }
        public IntegerLiteralContext(NumberContext ctx) { copyFrom(ctx); }
        @Override
        public void enterRule(ParseTreeListener listener) {
            if ( listener instanceof LakeSoulSqlExtensionsListener ) ((LakeSoulSqlExtensionsListener)listener).enterIntegerLiteral(this);
        }
        @Override
        public void exitRule(ParseTreeListener listener) {
            if ( listener instanceof LakeSoulSqlExtensionsListener ) ((LakeSoulSqlExtensionsListener)listener).exitIntegerLiteral(this);
        }
        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if ( visitor instanceof LakeSoulSqlExtensionsVisitor ) return ((LakeSoulSqlExtensionsVisitor<? extends T>)visitor).visitIntegerLiteral(this);
            else return visitor.visitChildren(this);
        }
    }
    public static class FloatLiteralContext extends NumberContext {
        public TerminalNode FLOAT_LITERAL() { return getToken(LakeSoulSqlExtensionsParser.FLOAT_LITERAL, 0); }
        public TerminalNode MINUS() { return getToken(LakeSoulSqlExtensionsParser.MINUS, 0); }
        public FloatLiteralContext(NumberContext ctx) { copyFrom(ctx); }
        @Override
        public void enterRule(ParseTreeListener listener) {
            if ( listener instanceof LakeSoulSqlExtensionsListener ) ((LakeSoulSqlExtensionsListener)listener).enterFloatLiteral(this);
        }
        @Override
        public void exitRule(ParseTreeListener listener) {
            if ( listener instanceof LakeSoulSqlExtensionsListener ) ((LakeSoulSqlExtensionsListener)listener).exitFloatLiteral(this);
        }
        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if ( visitor instanceof LakeSoulSqlExtensionsVisitor ) return ((LakeSoulSqlExtensionsVisitor<? extends T>)visitor).visitFloatLiteral(this);
            else return visitor.visitChildren(this);
        }
    }
    public static class SmallIntLiteralContext extends NumberContext {
        public TerminalNode SMALLINT_LITERAL() { return getToken(LakeSoulSqlExtensionsParser.SMALLINT_LITERAL, 0); }
        public TerminalNode MINUS() { return getToken(LakeSoulSqlExtensionsParser.MINUS, 0); }
        public SmallIntLiteralContext(NumberContext ctx) { copyFrom(ctx); }
        @Override
        public void enterRule(ParseTreeListener listener) {
            if ( listener instanceof LakeSoulSqlExtensionsListener ) ((LakeSoulSqlExtensionsListener)listener).enterSmallIntLiteral(this);
        }
        @Override
        public void exitRule(ParseTreeListener listener) {
            if ( listener instanceof LakeSoulSqlExtensionsListener ) ((LakeSoulSqlExtensionsListener)listener).exitSmallIntLiteral(this);
        }
        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if ( visitor instanceof LakeSoulSqlExtensionsVisitor ) return ((LakeSoulSqlExtensionsVisitor<? extends T>)visitor).visitSmallIntLiteral(this);
            else return visitor.visitChildren(this);
        }
    }
    
    public final NumberContext number() throws RecognitionException {
        NumberContext _localctx = new NumberContext(_ctx, getState());
        enterRule(_localctx, 48, RULE_number);
        int _la;
        try {
            setState(418);
            _errHandler.sync(this);
            switch ( getInterpreter().adaptivePredict(_input,46,_ctx) ) {
                case 1:
                    _localctx = new ExponentLiteralContext(_localctx);
                    enterOuterAlt(_localctx, 1);
                {
                    setState(383);
                    _errHandler.sync(this);
                    _la = _input.LA(1);
                    if (_la==MINUS) {
                        {
                            setState(382);
                            match(MINUS);
                        }
                    }

                    setState(385);
                    match(EXPONENT_VALUE);
                }
                break;
                case 2:
                    _localctx = new DecimalLiteralContext(_localctx);
                    enterOuterAlt(_localctx, 2);
                {
                    setState(387);
                    _errHandler.sync(this);
                    _la = _input.LA(1);
                    if (_la==MINUS) {
                        {
                            setState(386);
                            match(MINUS);
                        }
                    }

                    setState(389);
                    match(DECIMAL_VALUE);
                }
                break;
                case 3:
                    _localctx = new IntegerLiteralContext(_localctx);
                    enterOuterAlt(_localctx, 3);
                {
                    setState(391);
                    _errHandler.sync(this);
                    _la = _input.LA(1);
                    if (_la==MINUS) {
                        {
                            setState(390);
                            match(MINUS);
                        }
                    }

                    setState(393);
                    match(INTEGER_VALUE);
                }
                break;
                case 4:
                    _localctx = new BigIntLiteralContext(_localctx);
                    enterOuterAlt(_localctx, 4);
                {
                    setState(395);
                    _errHandler.sync(this);
                    _la = _input.LA(1);
                    if (_la==MINUS) {
                        {
                            setState(394);
                            match(MINUS);
                        }
                    }

                    setState(397);
                    match(BIGINT_LITERAL);
                }
                break;
                case 5:
                    _localctx = new SmallIntLiteralContext(_localctx);
                    enterOuterAlt(_localctx, 5);
                {
                    setState(399);
                    _errHandler.sync(this);
                    _la = _input.LA(1);
                    if (_la==MINUS) {
                        {
                            setState(398);
                            match(MINUS);
                        }
                    }

                    setState(401);
                    match(SMALLINT_LITERAL);
                }
                break;
                case 6:
                    _localctx = new TinyIntLiteralContext(_localctx);
                    enterOuterAlt(_localctx, 6);
                {
                    setState(403);
                    _errHandler.sync(this);
                    _la = _input.LA(1);
                    if (_la==MINUS) {
                        {
                            setState(402);
                            match(MINUS);
                        }
                    }

                    setState(405);
                    match(TINYINT_LITERAL);
                }
                break;
                case 7:
                    _localctx = new DoubleLiteralContext(_localctx);
                    enterOuterAlt(_localctx, 7);
                {
                    setState(407);
                    _errHandler.sync(this);
                    _la = _input.LA(1);
                    if (_la==MINUS) {
                        {
                            setState(406);
                            match(MINUS);
                        }
                    }

                    setState(409);
                    match(DOUBLE_LITERAL);
                }
                break;
                case 8:
                    _localctx = new FloatLiteralContext(_localctx);
                    enterOuterAlt(_localctx, 8);
                {
                    setState(411);
                    _errHandler.sync(this);
                    _la = _input.LA(1);
                    if (_la==MINUS) {
                        {
                            setState(410);
                            match(MINUS);
                        }
                    }

                    setState(413);
                    match(FLOAT_LITERAL);
                }
                break;
                case 9:
                    _localctx = new BigDecimalLiteralContext(_localctx);
                    enterOuterAlt(_localctx, 9);
                {
                    setState(415);
                    _errHandler.sync(this);
                    _la = _input.LA(1);
                    if (_la==MINUS) {
                        {
                            setState(414);
                            match(MINUS);
                        }
                    }

                    setState(417);
                    match(BIGDECIMAL_LITERAL);
                }
                break;
            }
        }
        catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        }
        finally {
            exitRule();
        }
        return _localctx;
    }
    public static class BooleanLiteralContext extends ConstantContext {
        public BooleanValueContext booleanValue() {
            return getRuleContext(BooleanValueContext.class,0);
        }
        public BooleanLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
        @Override
        public void enterRule(ParseTreeListener listener) {
            if ( listener instanceof LakeSoulSqlExtensionsListener ) ((LakeSoulSqlExtensionsListener)listener).enterBooleanLiteral(this);
        }
        @Override
        public void exitRule(ParseTreeListener listener) {
            if ( listener instanceof LakeSoulSqlExtensionsListener ) ((LakeSoulSqlExtensionsListener)listener).exitBooleanLiteral(this);
        }
        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if ( visitor instanceof LakeSoulSqlExtensionsVisitor ) return ((LakeSoulSqlExtensionsVisitor<? extends T>)visitor).visitBooleanLiteral(this);
            else return visitor.visitChildren(this);
        }
    }
    public static class StringLiteralContext extends ConstantContext {
        public List<TerminalNode> STRING() { return getTokens(LakeSoulSqlExtensionsParser.STRING); }
        public TerminalNode STRING(int i) {
            return getToken(LakeSoulSqlExtensionsParser.STRING, i);
        }
        public StringLiteralContext(ConstantContext ctx) { copyFrom(ctx); }
        @Override
        public void enterRule(ParseTreeListener listener) {
            if ( listener instanceof LakeSoulSqlExtensionsListener ) ((LakeSoulSqlExtensionsListener)listener).enterStringLiteral(this);
        }
        @Override
        public void exitRule(ParseTreeListener listener) {
            if ( listener instanceof LakeSoulSqlExtensionsListener ) ((LakeSoulSqlExtensionsListener)listener).exitStringLiteral(this);
        }
        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if ( visitor instanceof LakeSoulSqlExtensionsVisitor ) return ((LakeSoulSqlExtensionsVisitor<? extends T>)visitor).visitStringLiteral(this);
            else return visitor.visitChildren(this);
        }
    }
    public static class BooleanValueContext extends ParserRuleContext {
        public TerminalNode TRUE() { return getToken(LakeSoulSqlExtensionsParser.TRUE, 0); }
        public TerminalNode FALSE() { return getToken(LakeSoulSqlExtensionsParser.FALSE, 0); }
        public BooleanValueContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }
        @Override public int getRuleIndex() { return RULE_booleanValue; }
        @Override
        public void enterRule(ParseTreeListener listener) {
            if ( listener instanceof LakeSoulSqlExtensionsListener ) ((LakeSoulSqlExtensionsListener)listener).enterBooleanValue(this);
        }
        @Override
        public void exitRule(ParseTreeListener listener) {
            if ( listener instanceof LakeSoulSqlExtensionsListener ) ((LakeSoulSqlExtensionsListener)listener).exitBooleanValue(this);
        }
        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if ( visitor instanceof LakeSoulSqlExtensionsVisitor ) return ((LakeSoulSqlExtensionsVisitor<? extends T>)visitor).visitBooleanValue(this);
            else return visitor.visitChildren(this);
        }
    }
    public final BooleanValueContext booleanValue() throws RecognitionException {
        BooleanValueContext _localctx = new BooleanValueContext(_ctx, getState());
        enterRule(_localctx, 46, RULE_booleanValue);
        int _la;
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(380);
                _la = _input.LA(1);
                if ( !(_la==TRUE || _la==FALSE) ) {
                    _errHandler.recoverInline(this);
                }
                else {
                    if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
                    _errHandler.reportMatch(this);
                    consume();
                }
            }
        }
        catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        }
        finally {
            exitRule();
        }
        return _localctx;
    }
    public final ConstantContext constant() throws RecognitionException {
        ConstantContext _localctx = new ConstantContext(_ctx, getState());
        enterRule(_localctx, 40, RULE_constant);
        int _la;
        try {
            setState(354);
            _errHandler.sync(this);
            switch ( getInterpreter().adaptivePredict(_input,34,_ctx) ) {
                case 1:
                    _localctx = new NumericLiteralContext(_localctx);
                    enterOuterAlt(_localctx, 1);
                {
                    setState(344);
                    number();
                }
                break;
                case 2:
                    _localctx = new BooleanLiteralContext(_localctx);
                    enterOuterAlt(_localctx, 2);
                {
                    setState(345);
                    booleanValue();
                }
                break;
                case 3:
                    _localctx = new StringLiteralContext(_localctx);
                    enterOuterAlt(_localctx, 3);
                {
                    setState(347);
                    _errHandler.sync(this);
                    _la = _input.LA(1);
                    do {
                        {
                            {
                                setState(346);
                                match(STRING);
                            }
                        }
                        setState(349);
                        _errHandler.sync(this);
                        _la = _input.LA(1);
                    } while ( _la==STRING );
                }
                break;
                case 4:
                    _localctx = new TypeConstructorContext(_localctx);
                    enterOuterAlt(_localctx, 4);
                {
                    setState(351);
                    identifier();
                    setState(352);
                    match(STRING);
                }
                break;
            }
        }
        catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        }
        finally {
            exitRule();
        }
        return _localctx;
    }
    
    public final StringArrayContext stringArray() throws RecognitionException {
        StringArrayContext _localctx = new StringArrayContext(_ctx, getState());
        enterRule(_localctx, 44, RULE_stringArray);
        int _la;
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(368);
                match(ARRAY);
                setState(369);
                match(T__0);
                setState(370);
                constant();
                setState(375);
                _errHandler.sync(this);
                _la = _input.LA(1);
                while (_la==T__1) {
                    {
                        {
                            setState(371);
                            match(T__1);
                            setState(372);
                            constant();
                        }
                    }
                    setState(377);
                    _errHandler.sync(this);
                    _la = _input.LA(1);
                }
                setState(378);
                match(T__2);
            }
        }
        catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        }
        finally {
            exitRule();
        }
        return _localctx;
    }
    public static class ExpressionContext extends ParserRuleContext {
        public ConstantContext constant() {
            return getRuleContext(ConstantContext.class,0);
        }
        public StringMapContext stringMap() {
            return getRuleContext(StringMapContext.class,0);
        }
        public StringArrayContext stringArray() {
            return getRuleContext(StringArrayContext.class,0);
        }
        public ExpressionContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }
        @Override public int getRuleIndex() { return RULE_expression; }
        @Override
        public void enterRule(ParseTreeListener listener) {
            if ( listener instanceof LakeSoulSqlExtensionsListener ) ((LakeSoulSqlExtensionsListener)listener).enterExpression(this);
        }
        @Override
        public void exitRule(ParseTreeListener listener) {
            if ( listener instanceof LakeSoulSqlExtensionsListener ) ((LakeSoulSqlExtensionsListener)listener).exitExpression(this);
        }
        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if ( visitor instanceof LakeSoulSqlExtensionsVisitor ) return ((LakeSoulSqlExtensionsVisitor<? extends T>)visitor).visitExpression(this);
            else return visitor.visitChildren(this);
        }
    }
    public static class PositionalArgumentContext extends CallArgumentContext {
        public ExpressionContext expression() {
            return getRuleContext(ExpressionContext.class,0);
        }
        public PositionalArgumentContext(CallArgumentContext ctx) { copyFrom(ctx); }
        @Override
        public void enterRule(ParseTreeListener listener) {
            if ( listener instanceof LakeSoulSqlExtensionsListener ) ((LakeSoulSqlExtensionsListener)listener).enterPositionalArgument(this);
        }
        @Override
        public void exitRule(ParseTreeListener listener) {
            if ( listener instanceof LakeSoulSqlExtensionsListener ) ((LakeSoulSqlExtensionsListener)listener).exitPositionalArgument(this);
        }
        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if ( visitor instanceof LakeSoulSqlExtensionsVisitor ) return ((LakeSoulSqlExtensionsVisitor<? extends T>)visitor).visitPositionalArgument(this);
            else return visitor.visitChildren(this);
        }
    }
    public final ExpressionContext expression() throws RecognitionException {
        ExpressionContext _localctx = new ExpressionContext(_ctx, getState());
        enterRule(_localctx, 38, RULE_expression);
        try {
            setState(342);
            _errHandler.sync(this);
            switch ( getInterpreter().adaptivePredict(_input,32,_ctx) ) {
                case 1:
                    enterOuterAlt(_localctx, 1);
                {
                    setState(339);
                    constant();
                }
                break;
                case 2:
                    enterOuterAlt(_localctx, 2);
                {
                    setState(340);
                    stringMap();
                }
                break;
                case 3:
                    enterOuterAlt(_localctx, 3);
                {
                    setState(341);
                    stringArray();
                }
                break;
            }
        }
        catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        }
        finally {
            exitRule();
        }
        return _localctx;
    }
    public static class NamedArgumentContext extends CallArgumentContext {
        public IdentifierContext identifier() {
            return getRuleContext(IdentifierContext.class,0);
        }
        public ExpressionContext expression() {
            return getRuleContext(ExpressionContext.class,0);
        }
        public NamedArgumentContext(CallArgumentContext ctx) { copyFrom(ctx); }
        @Override
        public void enterRule(ParseTreeListener listener) {
            if ( listener instanceof LakeSoulSqlExtensionsListener ) ((LakeSoulSqlExtensionsListener)listener).enterNamedArgument(this);
        }
        @Override
        public void exitRule(ParseTreeListener listener) {
            if ( listener instanceof LakeSoulSqlExtensionsListener ) ((LakeSoulSqlExtensionsListener)listener).exitNamedArgument(this);
        }
        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if ( visitor instanceof LakeSoulSqlExtensionsVisitor ) return ((LakeSoulSqlExtensionsVisitor<? extends T>)visitor).visitNamedArgument(this);
            else return visitor.visitChildren(this);
        }
    }
    public final CallArgumentContext callArgument() throws RecognitionException {
        CallArgumentContext _localctx = new CallArgumentContext(_ctx, getState());
        enterRule(_localctx, 26, RULE_callArgument);
        try {
            setState(286);
            _errHandler.sync(this);
            switch ( getInterpreter().adaptivePredict(_input,23,_ctx) ) {
                case 1:
                    _localctx = new PositionalArgumentContext(_localctx);
                    enterOuterAlt(_localctx, 1);
                {
                    setState(281);
                    expression();
                }
                break;
                case 2:
                    _localctx = new NamedArgumentContext(_localctx);
                    enterOuterAlt(_localctx, 2);
                {
                    setState(282);
                    identifier();
                    setState(283);
                    match(T__3);
                    setState(284);
                    expression();
                }
                break;
            }
        }
        catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        }
        finally {
            exitRule();
        }
        return _localctx;
    }
    public final StatementContext statement() throws RecognitionException {
        StatementContext _localctx = new StatementContext(_ctx, getState());
        enterRule(_localctx, 2, RULE_statement);
        int _la;
        try {
            setState(170);
            _errHandler.sync(this);
            switch ( getInterpreter().adaptivePredict(_input,6,_ctx) ) {
                case 1:
                    _localctx = new CallContext(_localctx);
                    enterOuterAlt(_localctx, 1);
                {
                    setState(69);
                    match(CALL);
                    setState(70);
                    multipartIdentifier();
                    setState(71);
                    match(T__0);
                    setState(80);
                    _errHandler.sync(this);
                    _la = _input.LA(1);
                    if (((((_la - 6)) & ~0x3f) == 0 && ((1L << (_la - 6)) & ((1L << (ADD - 6)) | (1L << (ALTER - 6)) | (1L << (AS - 6)) | (1L << (ASC - 6)) | (1L << (BRANCH - 6)) | (1L << (BY - 6)) | (1L << (CALL - 6)) | (1L << (DAYS - 6)) | (1L << (DESC - 6)) | (1L << (DISTRIBUTED - 6)) | (1L << (DROP - 6)) | (1L << (EXISTS - 6)) | (1L << (FIELD - 6)) | (1L << (FIELDS - 6)) | (1L << (FIRST - 6)) | (1L << (HOURS - 6)) | (1L << (IF - 6)) | (1L << (LAST - 6)) | (1L << (LOCALLY - 6)) | (1L << (MINUTES - 6)) | (1L << (MONTHS - 6)) | (1L << (CREATE - 6)) | (1L << (NOT - 6)) | (1L << (NULLS - 6)) | (1L << (OF - 6)) | (1L << (OR - 6)) | (1L << (ORDERED - 6)) | (1L << (PARTITION - 6)) | (1L << (REPLACE - 6)) | (1L << (RETAIN - 6)) | (1L << (IDENTIFIER_KW - 6)) | (1L << (SET - 6)) | (1L << (SNAPSHOT - 6)) | (1L << (SNAPSHOTS - 6)) | (1L << (TABLE - 6)) | (1L << (TAG - 6)) | (1L << (UNORDERED - 6)) | (1L << (VERSION - 6)) | (1L << (WITH - 6)) | (1L << (WRITE - 6)) | (1L << (TRUE - 6)) | (1L << (FALSE - 6)) | (1L << (MAP - 6)) | (1L << (ARRAY - 6)) | (1L << (MINUS - 6)) | (1L << (STRING - 6)) | (1L << (BIGINT_LITERAL - 6)) | (1L << (SMALLINT_LITERAL - 6)) | (1L << (TINYINT_LITERAL - 6)) | (1L << (INTEGER_VALUE - 6)) | (1L << (EXPONENT_VALUE - 6)) | (1L << (DECIMAL_VALUE - 6)) | (1L << (FLOAT_LITERAL - 6)) | (1L << (DOUBLE_LITERAL - 6)) | (1L << (BIGDECIMAL_LITERAL - 6)) | (1L << (IDENTIFIER - 6)) | (1L << (BACKQUOTED_IDENTIFIER - 6)))) != 0)) {
                        {
                            setState(72);
                            callArgument();
                            setState(77);
                            _errHandler.sync(this);
                            _la = _input.LA(1);
                            while (_la==T__1) {
                                {
                                    {
                                        setState(73);
                                        match(T__1);
                                        setState(74);
                                        callArgument();
                                    }
                                }
                                setState(79);
                                _errHandler.sync(this);
                                _la = _input.LA(1);
                            }
                        }
                    }

                    setState(82);
                    match(T__2);
                }
                break;
            }
        }
        catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        }
        finally {
            exitRule();
        }
        return _localctx;
    }
    public static class MultipartIdentifierContext extends ParserRuleContext {
        public IdentifierContext identifier;
        public List<IdentifierContext> parts = new ArrayList<IdentifierContext>();
        public List<IdentifierContext> identifier() {
            return getRuleContexts(IdentifierContext.class);
        }
        public IdentifierContext identifier(int i) {
            return getRuleContext(IdentifierContext.class,i);
        }
        public MultipartIdentifierContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }
        @Override public int getRuleIndex() { return RULE_multipartIdentifier; }
        @Override
        public void enterRule(ParseTreeListener listener) {
            if ( listener instanceof LakeSoulSqlExtensionsListener ) ((LakeSoulSqlExtensionsListener)listener).enterMultipartIdentifier(this);
        }
        @Override
        public void exitRule(ParseTreeListener listener) {
            if ( listener instanceof LakeSoulSqlExtensionsListener ) ((LakeSoulSqlExtensionsListener)listener).exitMultipartIdentifier(this);
        }
        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if ( visitor instanceof LakeSoulSqlExtensionsVisitor ) return ((LakeSoulSqlExtensionsVisitor<? extends T>)visitor).visitMultipartIdentifier(this);
            else return visitor.visitChildren(this);
        }
    }
    public final MultipartIdentifierContext multipartIdentifier() throws RecognitionException {
        MultipartIdentifierContext _localctx = new MultipartIdentifierContext(_ctx, getState());
        enterRule(_localctx, 50, RULE_multipartIdentifier);
        int _la;
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(420);
                ((MultipartIdentifierContext)_localctx).identifier = identifier();
                ((MultipartIdentifierContext)_localctx).parts.add(((MultipartIdentifierContext)_localctx).identifier);
                setState(425);
                _errHandler.sync(this);
                _la = _input.LA(1);
                while (_la==T__4) {
                    {
                        {
                            setState(421);
                            match(T__4);
                            setState(422);
                            ((MultipartIdentifierContext)_localctx).identifier = identifier();
                            ((MultipartIdentifierContext)_localctx).parts.add(((MultipartIdentifierContext)_localctx).identifier);
                        }
                    }
                    setState(427);
                    _errHandler.sync(this);
                    _la = _input.LA(1);
                }
            }
        }
        catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        }
        finally {
            exitRule();
        }
        return _localctx;
    }
    public static class NonReservedContext extends ParserRuleContext {
        public TerminalNode ADD() { return getToken(LakeSoulSqlExtensionsParser.ADD, 0); }
        public TerminalNode ALTER() { return getToken(LakeSoulSqlExtensionsParser.ALTER, 0); }
        public TerminalNode AS() { return getToken(LakeSoulSqlExtensionsParser.AS, 0); }
        public TerminalNode ASC() { return getToken(LakeSoulSqlExtensionsParser.ASC, 0); }
        public TerminalNode BRANCH() { return getToken(LakeSoulSqlExtensionsParser.BRANCH, 0); }
        public TerminalNode BY() { return getToken(LakeSoulSqlExtensionsParser.BY, 0); }
        public TerminalNode CALL() { return getToken(LakeSoulSqlExtensionsParser.CALL, 0); }
        public TerminalNode CREATE() { return getToken(LakeSoulSqlExtensionsParser.CREATE, 0); }
        public TerminalNode DAYS() { return getToken(LakeSoulSqlExtensionsParser.DAYS, 0); }
        public TerminalNode DESC() { return getToken(LakeSoulSqlExtensionsParser.DESC, 0); }
        public TerminalNode DROP() { return getToken(LakeSoulSqlExtensionsParser.DROP, 0); }
        public TerminalNode EXISTS() { return getToken(LakeSoulSqlExtensionsParser.EXISTS, 0); }
        public TerminalNode FIELD() { return getToken(LakeSoulSqlExtensionsParser.FIELD, 0); }
        public TerminalNode FIRST() { return getToken(LakeSoulSqlExtensionsParser.FIRST, 0); }
        public TerminalNode HOURS() { return getToken(LakeSoulSqlExtensionsParser.HOURS, 0); }
        public TerminalNode IF() { return getToken(LakeSoulSqlExtensionsParser.IF, 0); }
        public TerminalNode LAST() { return getToken(LakeSoulSqlExtensionsParser.LAST, 0); }
        public TerminalNode NOT() { return getToken(LakeSoulSqlExtensionsParser.NOT, 0); }
        public TerminalNode NULLS() { return getToken(LakeSoulSqlExtensionsParser.NULLS, 0); }
        public TerminalNode OF() { return getToken(LakeSoulSqlExtensionsParser.OF, 0); }
        public TerminalNode OR() { return getToken(LakeSoulSqlExtensionsParser.OR, 0); }
        public TerminalNode ORDERED() { return getToken(LakeSoulSqlExtensionsParser.ORDERED, 0); }
        public TerminalNode PARTITION() { return getToken(LakeSoulSqlExtensionsParser.PARTITION, 0); }
        public TerminalNode TABLE() { return getToken(LakeSoulSqlExtensionsParser.TABLE, 0); }
        public TerminalNode WRITE() { return getToken(LakeSoulSqlExtensionsParser.WRITE, 0); }
        public TerminalNode DISTRIBUTED() { return getToken(LakeSoulSqlExtensionsParser.DISTRIBUTED, 0); }
        public TerminalNode LOCALLY() { return getToken(LakeSoulSqlExtensionsParser.LOCALLY, 0); }
        public TerminalNode MINUTES() { return getToken(LakeSoulSqlExtensionsParser.MINUTES, 0); }
        public TerminalNode MONTHS() { return getToken(LakeSoulSqlExtensionsParser.MONTHS, 0); }
        public TerminalNode UNORDERED() { return getToken(LakeSoulSqlExtensionsParser.UNORDERED, 0); }
        public TerminalNode REPLACE() { return getToken(LakeSoulSqlExtensionsParser.REPLACE, 0); }
        public TerminalNode RETAIN() { return getToken(LakeSoulSqlExtensionsParser.RETAIN, 0); }
        public TerminalNode VERSION() { return getToken(LakeSoulSqlExtensionsParser.VERSION, 0); }
        public TerminalNode WITH() { return getToken(LakeSoulSqlExtensionsParser.WITH, 0); }
        public TerminalNode IDENTIFIER_KW() { return getToken(LakeSoulSqlExtensionsParser.IDENTIFIER_KW, 0); }
        public TerminalNode FIELDS() { return getToken(LakeSoulSqlExtensionsParser.FIELDS, 0); }
        public TerminalNode SET() { return getToken(LakeSoulSqlExtensionsParser.SET, 0); }
        public TerminalNode SNAPSHOT() { return getToken(LakeSoulSqlExtensionsParser.SNAPSHOT, 0); }
        public TerminalNode SNAPSHOTS() { return getToken(LakeSoulSqlExtensionsParser.SNAPSHOTS, 0); }
        public TerminalNode TAG() { return getToken(LakeSoulSqlExtensionsParser.TAG, 0); }
        public TerminalNode TRUE() { return getToken(LakeSoulSqlExtensionsParser.TRUE, 0); }
        public TerminalNode FALSE() { return getToken(LakeSoulSqlExtensionsParser.FALSE, 0); }
        public TerminalNode MAP() { return getToken(LakeSoulSqlExtensionsParser.MAP, 0); }
        public NonReservedContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }
        @Override public int getRuleIndex() { return RULE_nonReserved; }
        @Override
        public void enterRule(ParseTreeListener listener) {
            if ( listener instanceof LakeSoulSqlExtensionsListener ) ((LakeSoulSqlExtensionsListener)listener).enterNonReserved(this);
        }
        @Override
        public void exitRule(ParseTreeListener listener) {
            if ( listener instanceof LakeSoulSqlExtensionsListener ) ((LakeSoulSqlExtensionsListener)listener).exitNonReserved(this);
        }
        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if ( visitor instanceof LakeSoulSqlExtensionsVisitor ) return ((LakeSoulSqlExtensionsVisitor<? extends T>)visitor).visitNonReserved(this);
            else return visitor.visitChildren(this);
        }
    }
    public static class UnquotedIdentifierContext extends IdentifierContext {
        public TerminalNode IDENTIFIER() { return getToken(LakeSoulSqlExtensionsParser.IDENTIFIER, 0); }
        public NonReservedContext nonReserved() {
            return getRuleContext(NonReservedContext.class,0);
        }
        public UnquotedIdentifierContext(IdentifierContext ctx) { copyFrom(ctx); }
        @Override
        public void enterRule(ParseTreeListener listener) {
            if ( listener instanceof LakeSoulSqlExtensionsListener ) ((LakeSoulSqlExtensionsListener)listener).enterUnquotedIdentifier(this);
        }
        @Override
        public void exitRule(ParseTreeListener listener) {
            if ( listener instanceof LakeSoulSqlExtensionsListener ) ((LakeSoulSqlExtensionsListener)listener).exitUnquotedIdentifier(this);
        }
        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if ( visitor instanceof LakeSoulSqlExtensionsVisitor ) return ((LakeSoulSqlExtensionsVisitor<? extends T>)visitor).visitUnquotedIdentifier(this);
            else return visitor.visitChildren(this);
        }
    }
    public static class QuotedIdentifierContext extends ParserRuleContext {
        public TerminalNode BACKQUOTED_IDENTIFIER() { return getToken(LakeSoulSqlExtensionsParser.BACKQUOTED_IDENTIFIER, 0); }
        public QuotedIdentifierContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }
        @Override public int getRuleIndex() { return RULE_quotedIdentifier; }
        @Override
        public void enterRule(ParseTreeListener listener) {
            if ( listener instanceof LakeSoulSqlExtensionsListener ) ((LakeSoulSqlExtensionsListener)listener).enterQuotedIdentifier(this);
        }
        @Override
        public void exitRule(ParseTreeListener listener) {
            if ( listener instanceof LakeSoulSqlExtensionsListener ) ((LakeSoulSqlExtensionsListener)listener).exitQuotedIdentifier(this);
        }
        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if ( visitor instanceof LakeSoulSqlExtensionsVisitor ) return ((LakeSoulSqlExtensionsVisitor<? extends T>)visitor).visitQuotedIdentifier(this);
            else return visitor.visitChildren(this);
        }
    }
    public static class QuotedIdentifierAlternativeContext extends IdentifierContext {
        public QuotedIdentifierContext quotedIdentifier() {
            return getRuleContext(QuotedIdentifierContext.class,0);
        }
        public QuotedIdentifierAlternativeContext(IdentifierContext ctx) { copyFrom(ctx); }
        @Override
        public void enterRule(ParseTreeListener listener) {
            if ( listener instanceof LakeSoulSqlExtensionsListener ) ((LakeSoulSqlExtensionsListener)listener).enterQuotedIdentifierAlternative(this);
        }
        @Override
        public void exitRule(ParseTreeListener listener) {
            if ( listener instanceof LakeSoulSqlExtensionsListener ) ((LakeSoulSqlExtensionsListener)listener).exitQuotedIdentifierAlternative(this);
        }
        @Override
        public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
            if ( visitor instanceof LakeSoulSqlExtensionsVisitor ) return ((LakeSoulSqlExtensionsVisitor<? extends T>)visitor).visitQuotedIdentifierAlternative(this);
            else return visitor.visitChildren(this);
        }
    }
    public final QuotedIdentifierContext quotedIdentifier() throws RecognitionException {
        QuotedIdentifierContext _localctx = new QuotedIdentifierContext(_ctx, getState());
        enterRule(_localctx, 54, RULE_quotedIdentifier);
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(433);
                match(BACKQUOTED_IDENTIFIER);
            }
        }
        catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        }
        finally {
            exitRule();
        }
        return _localctx;
    }
    public final IdentifierContext identifier() throws RecognitionException {
        IdentifierContext _localctx = new IdentifierContext(_ctx, getState());
        enterRule(_localctx, 52, RULE_identifier);
        try {
            setState(431);
            _errHandler.sync(this);
            switch (_input.LA(1)) {
                case IDENTIFIER:
                    _localctx = new UnquotedIdentifierContext(_localctx);
                    enterOuterAlt(_localctx, 1);
                {
                    setState(428);
                    match(IDENTIFIER);
                }
                break;
                case BACKQUOTED_IDENTIFIER:
                    _localctx = new QuotedIdentifierAlternativeContext(_localctx);
                    enterOuterAlt(_localctx, 2);
                {
                    setState(429);
                    quotedIdentifier();
                }
                break;
                case ADD:
                case ALTER:
                case AS:
                case ASC:
                case BRANCH:
                case BY:
                case CALL:
                case DAYS:
                case DESC:
                case DISTRIBUTED:
                case DROP:
                case EXISTS:
                case FIELD:
                case FIELDS:
                case FIRST:
                case HOURS:
                case IF:
                case LAST:
                case LOCALLY:
                case MINUTES:
                case MONTHS:
                case CREATE:
                case NOT:
                case NULLS:
                case OF:
                case OR:
                case ORDERED:
                case PARTITION:
                case REPLACE:
                case RETAIN:
                case IDENTIFIER_KW:
                case SET:
                case SNAPSHOT:
                case SNAPSHOTS:
                case TABLE:
                case TAG:
                case UNORDERED:
                case VERSION:
                case WITH:
                case WRITE:
                case TRUE:
                case FALSE:
                case MAP:
                    _localctx = new UnquotedIdentifierContext(_localctx);
                    enterOuterAlt(_localctx, 3);
                {
                    setState(430);
                    nonReserved();
                }
                break;
                default:
                    throw new NoViableAltException(this);
            }
        }
        catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        }
        finally {
            exitRule();
        }
        return _localctx;
    }
    public final NonReservedContext nonReserved() throws RecognitionException {
        NonReservedContext _localctx = new NonReservedContext(_ctx, getState());
        enterRule(_localctx, 58, RULE_nonReserved);
        int _la;
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(443);
                _la = _input.LA(1);
                if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << ADD) | (1L << ALTER) | (1L << AS) | (1L << ASC) | (1L << BRANCH) | (1L << BY) | (1L << CALL) | (1L << DAYS) | (1L << DESC) | (1L << DISTRIBUTED) | (1L << DROP) | (1L << EXISTS) | (1L << FIELD) | (1L << FIELDS) | (1L << FIRST) | (1L << HOURS) | (1L << IF) | (1L << LAST) | (1L << LOCALLY) | (1L << MINUTES) | (1L << MONTHS) | (1L << CREATE) | (1L << NOT) | (1L << NULLS) | (1L << OF) | (1L << OR) | (1L << ORDERED) | (1L << PARTITION) | (1L << REPLACE) | (1L << RETAIN) | (1L << IDENTIFIER_KW) | (1L << SET) | (1L << SNAPSHOT) | (1L << SNAPSHOTS) | (1L << TABLE) | (1L << TAG) | (1L << UNORDERED) | (1L << VERSION) | (1L << WITH) | (1L << WRITE) | (1L << TRUE) | (1L << FALSE) | (1L << MAP))) != 0)) ) {
                    _errHandler.recoverInline(this);
                }
                else {
                    if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
                    _errHandler.reportMatch(this);
                    consume();
                }
            }
        }
        catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        }
        finally {
            exitRule();
        }
        return _localctx;
    }
    public static final String _serializedATN =
            "\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\3F\u01c6\4\2\t\2\4"+
                    "\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13\t"+
                    "\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
                    "\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
                    "\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
                    "\t!\4\"\t\"\3\2\3\2\3\2\3\3\3\3\3\3\3\3\3\3\3\3\7\3N\n\3\f\3\16\3Q\13"+
                    "\3\5\3S\n\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\5\3`\n\3\3\3\3"+
                    "\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3"+
                    "\5\3u\n\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3"+
                    "\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3"+
                    "\3\3\3\3\3\3\3\3\3\3\3\3\3\5\3\u009e\n\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3\3"+
                    "\3\3\3\5\3\u00a9\n\3\3\3\3\3\5\3\u00ad\n\3\3\4\3\4\5\4\u00b1\n\4\3\4\3"+
                    "\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\5\4\u00bd\n\4\3\4\3\4\3\4\5\4\u00c2"+
                    "\n\4\3\5\3\5\5\5\u00c6\n\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\5\5"+
                    "\u00d2\n\5\3\5\3\5\3\5\5\5\u00d7\n\5\3\6\3\6\3\6\3\6\5\6\u00dd\n\6\3\6"+
                    "\5\6\u00e0\n\6\3\7\3\7\3\7\3\7\5\7\u00e6\n\7\3\7\5\7\u00e9\n\7\3\7\5\7"+
                    "\u00ec\n\7\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b\5\b"+
                    "\u00fc\n\b\3\t\3\t\3\t\3\t\3\n\3\n\3\n\3\13\3\13\3\13\3\f\3\f\7\f\u010a"+
                    "\n\f\f\f\16\f\u010d\13\f\3\r\3\r\3\r\3\r\3\16\5\16\u0114\n\16\3\16\3\16"+
                    "\3\16\3\16\5\16\u011a\n\16\3\17\3\17\3\17\3\17\3\17\5\17\u0121\n\17\3"+
                    "\20\3\20\3\20\3\21\3\21\3\21\7\21\u0129\n\21\f\21\16\21\u012c\13\21\3"+
                    "\21\3\21\3\21\3\21\7\21\u0132\n\21\f\21\16\21\u0135\13\21\3\21\3\21\5"+
                    "\21\u0139\n\21\3\22\3\22\5\22\u013d\n\22\3\22\3\22\5\22\u0141\n\22\3\23"+
                    "\3\23\3\23\3\23\3\23\3\23\7\23\u0149\n\23\f\23\16\23\u014c\13\23\3\23"+
                    "\3\23\5\23\u0150\n\23\3\24\3\24\5\24\u0154\n\24\3\25\3\25\3\25\5\25\u0159"+
                    "\n\25\3\26\3\26\3\26\6\26\u015e\n\26\r\26\16\26\u015f\3\26\3\26\3\26\5"+
                    "\26\u0165\n\26\3\27\3\27\3\27\3\27\3\27\7\27\u016c\n\27\f\27\16\27\u016f"+
                    "\13\27\3\27\3\27\3\30\3\30\3\30\3\30\3\30\7\30\u0178\n\30\f\30\16\30\u017b"+
                    "\13\30\3\30\3\30\3\31\3\31\3\32\5\32\u0182\n\32\3\32\3\32\5\32\u0186\n"+
                    "\32\3\32\3\32\5\32\u018a\n\32\3\32\3\32\5\32\u018e\n\32\3\32\3\32\5\32"+
                    "\u0192\n\32\3\32\3\32\5\32\u0196\n\32\3\32\3\32\5\32\u019a\n\32\3\32\3"+
                    "\32\5\32\u019e\n\32\3\32\3\32\5\32\u01a2\n\32\3\32\5\32\u01a5\n\32\3\33"+
                    "\3\33\3\33\7\33\u01aa\n\33\f\33\16\33\u01ad\13\33\3\34\3\34\3\34\5\34"+
                    "\u01b2\n\34\3\35\3\35\3\36\3\36\3\36\7\36\u01b9\n\36\f\36\16\36\u01bc"+
                    "\13\36\3\37\3\37\3 \3 \3!\3!\3\"\3\"\3\"\2\2#\2\4\6\b\n\f\16\20\22\24"+
                    "\26\30\32\34\36 \"$&(*,.\60\62\64\668:<>@B\2\7\4\2\13\13\20\20\4\2\26"+
                    "\26\31\31\3\2\61\62\4\2\b%\'\63\5\2\17\17\27\27\33\33\2\u01eb\2D\3\2\2"+
                    "\2\4\u00ac\3\2\2\2\6\u00c1\3\2\2\2\b\u00d6\3\2\2\2\n\u00dc\3\2\2\2\f\u00e5"+
                    "\3\2\2\2\16\u00fb\3\2\2\2\20\u00fd\3\2\2\2\22\u0101\3\2\2\2\24\u0104\3"+
                    "\2\2\2\26\u010b\3\2\2\2\30\u010e\3\2\2\2\32\u0119\3\2\2\2\34\u0120\3\2"+
                    "\2\2\36\u0122\3\2\2\2 \u0138\3\2\2\2\"\u013a\3\2\2\2$\u014f\3\2\2\2&\u0153"+
                    "\3\2\2\2(\u0158\3\2\2\2*\u0164\3\2\2\2,\u0166\3\2\2\2.\u0172\3\2\2\2\60"+
                    "\u017e\3\2\2\2\62\u01a4\3\2\2\2\64\u01a6\3\2\2\2\66\u01b1\3\2\2\28\u01b3"+
                    "\3\2\2\2:\u01b5\3\2\2\2<\u01bd\3\2\2\2>\u01bf\3\2\2\2@\u01c1\3\2\2\2B"+
                    "\u01c3\3\2\2\2DE\5\4\3\2EF\7\2\2\3F\3\3\2\2\2GH\7\16\2\2HI\5\64\33\2I"+
                    "R\7\3\2\2JO\5\34\17\2KL\7\4\2\2LN\5\34\17\2MK\3\2\2\2NQ\3\2\2\2OM\3\2"+
                    "\2\2OP\3\2\2\2PS\3\2\2\2QO\3\2\2\2RJ\3\2\2\2RS\3\2\2\2ST\3\2\2\2TU\7\5"+
                    "\2\2U\u00ad\3\2\2\2VW\7\t\2\2WX\7+\2\2XY\5\64\33\2YZ\7\b\2\2Z[\7#\2\2"+
                    "[\\\7\24\2\2\\_\5$\23\2]^\7\n\2\2^`\5\66\34\2_]\3\2\2\2_`\3\2\2\2`\u00ad"+
                    "\3\2\2\2ab\7\t\2\2bc\7+\2\2cd\5\64\33\2de\7\22\2\2ef\7#\2\2fg\7\24\2\2"+
                    "gh\5$\23\2h\u00ad\3\2\2\2ij\7\t\2\2jk\7+\2\2kl\5\64\33\2lm\7$\2\2mn\7"+
                    "#\2\2no\7\24\2\2op\5$\23\2pq\7/\2\2qt\5$\23\2rs\7\n\2\2su\5\66\34\2tr"+
                    "\3\2\2\2tu\3\2\2\2u\u00ad\3\2\2\2vw\7\t\2\2wx\7+\2\2xy\5\64\33\2yz\7\60"+
                    "\2\2z{\5\26\f\2{\u00ad\3\2\2\2|}\7\t\2\2}~\7+\2\2~\177\5\64\33\2\177\u0080"+
                    "\7(\2\2\u0080\u0081\7\'\2\2\u0081\u0082\7\25\2\2\u0082\u0083\5:\36\2\u0083"+
                    "\u00ad\3\2\2\2\u0084\u0085\7\t\2\2\u0085\u0086\7+\2\2\u0086\u0087\5\64"+
                    "\33\2\u0087\u0088\7\22\2\2\u0088\u0089\7\'\2\2\u0089\u008a\7\25\2\2\u008a"+
                    "\u008b\5:\36\2\u008b\u00ad\3\2\2\2\u008c\u008d\7\t\2\2\u008d\u008e\7+"+
                    "\2\2\u008e\u008f\5\64\33\2\u008f\u0090\5\b\5\2\u0090\u00ad\3\2\2\2\u0091"+
                    "\u0092\7\t\2\2\u0092\u0093\7+\2\2\u0093\u0094\5\64\33\2\u0094\u0095\5"+
                    "\6\4\2\u0095\u00ad\3\2\2\2\u0096\u0097\7\t\2\2\u0097\u0098\7+\2\2\u0098"+
                    "\u0099\5\64\33\2\u0099\u009a\7\22\2\2\u009a\u009d\7\f\2\2\u009b\u009c"+
                    "\7\30\2\2\u009c\u009e\7\23\2\2\u009d\u009b\3\2\2\2\u009d\u009e\3\2\2\2"+
                    "\u009e\u009f\3\2\2\2\u009f\u00a0\5\66\34\2\u00a0\u00ad\3\2\2\2\u00a1\u00a2"+
                    "\7\t\2\2\u00a2\u00a3\7+\2\2\u00a3\u00a4\5\64\33\2\u00a4\u00a5\7\22\2\2"+
                    "\u00a5\u00a8\7,\2\2\u00a6\u00a7\7\30\2\2\u00a7\u00a9\7\23\2\2\u00a8\u00a6"+
                    "\3\2\2\2\u00a8\u00a9\3\2\2\2\u00a9\u00aa\3\2\2\2\u00aa\u00ab\5\66\34\2"+
                    "\u00ab\u00ad\3\2\2\2\u00acG\3\2\2\2\u00acV\3\2\2\2\u00aca\3\2\2\2\u00ac"+
                    "i\3\2\2\2\u00acv\3\2\2\2\u00ac|\3\2\2\2\u00ac\u0084\3\2\2\2\u00ac\u008c"+
                    "\3\2\2\2\u00ac\u0091\3\2\2\2\u00ac\u0096\3\2\2\2\u00ac\u00a1\3\2\2\2\u00ad"+
                    "\5\3\2\2\2\u00ae\u00af\7\35\2\2\u00af\u00b1\7!\2\2\u00b0\u00ae\3\2\2\2"+
                    "\u00b0\u00b1\3\2\2\2\u00b1\u00b2\3\2\2\2\u00b2\u00b3\7$\2\2\u00b3\u00b4"+
                    "\7,\2\2\u00b4\u00b5\5\66\34\2\u00b5\u00b6\5\n\6\2\u00b6\u00c2\3\2\2\2"+
                    "\u00b7\u00b8\7\35\2\2\u00b8\u00bc\7,\2\2\u00b9\u00ba\7\30\2\2\u00ba\u00bb"+
                    "\7\36\2\2\u00bb\u00bd\7\23\2\2\u00bc\u00b9\3\2\2\2\u00bc\u00bd\3\2\2\2"+
                    "\u00bd\u00be\3\2\2\2\u00be\u00bf\5\66\34\2\u00bf\u00c0\5\n\6\2\u00c0\u00c2"+
                    "\3\2\2\2\u00c1\u00b0\3\2\2\2\u00c1\u00b7\3\2\2\2\u00c2\7\3\2\2\2\u00c3"+
                    "\u00c4\7\35\2\2\u00c4\u00c6\7!\2\2\u00c5\u00c3\3\2\2\2\u00c5\u00c6\3\2"+
                    "\2\2\u00c6\u00c7\3\2\2\2\u00c7\u00c8\7$\2\2\u00c8\u00c9\7\f\2\2\u00c9"+
                    "\u00ca\5\66\34\2\u00ca\u00cb\5\f\7\2\u00cb\u00d7\3\2\2\2\u00cc\u00cd\7"+
                    "\35\2\2\u00cd\u00d1\7\f\2\2\u00ce\u00cf\7\30\2\2\u00cf\u00d0\7\36\2\2"+
                    "\u00d0\u00d2\7\23\2\2\u00d1\u00ce\3\2\2\2\u00d1\u00d2\3\2\2\2\u00d2\u00d3"+
                    "\3\2\2\2\u00d3\u00d4\5\66\34\2\u00d4\u00d5\5\f\7\2\u00d5\u00d7\3\2\2\2"+
                    "\u00d6\u00c5\3\2\2\2\u00d6\u00cc\3\2\2\2\u00d7\t\3\2\2\2\u00d8\u00d9\7"+
                    "\n\2\2\u00d9\u00da\7 \2\2\u00da\u00db\7.\2\2\u00db\u00dd\5> \2\u00dc\u00d8"+
                    "\3\2\2\2\u00dc\u00dd\3\2\2\2\u00dd\u00df\3\2\2\2\u00de\u00e0\5\20\t\2"+
                    "\u00df\u00de\3\2\2\2\u00df\u00e0\3\2\2\2\u00e0\13\3\2\2\2\u00e1\u00e2"+
                    "\7\n\2\2\u00e2\u00e3\7 \2\2\u00e3\u00e4\7.\2\2\u00e4\u00e6\5> \2\u00e5"+
                    "\u00e1\3\2\2\2\u00e5\u00e6\3\2\2\2\u00e6\u00e8\3\2\2\2\u00e7\u00e9\5\20"+
                    "\t\2\u00e8\u00e7\3\2\2\2\u00e8\u00e9\3\2\2\2\u00e9\u00eb\3\2\2\2\u00ea"+
                    "\u00ec\5\16\b\2\u00eb\u00ea\3\2\2\2\u00eb\u00ec\3\2\2\2\u00ec\r\3\2\2"+
                    "\2\u00ed\u00ee\7/\2\2\u00ee\u00ef\7)\2\2\u00ef\u00f0\7&\2\2\u00f0\u00fc"+
                    "\5\24\13\2\u00f1\u00f2\7/\2\2\u00f2\u00f3\7)\2\2\u00f3\u00f4\7&\2\2\u00f4"+
                    "\u00fc\5\22\n\2\u00f5\u00f6\7/\2\2\u00f6\u00f7\7)\2\2\u00f7\u00f8\7&\2"+
                    "\2\u00f8\u00f9\5\24\13\2\u00f9\u00fa\5\22\n\2\u00fa\u00fc\3\2\2\2\u00fb"+
                    "\u00ed\3\2\2\2\u00fb\u00f1\3\2\2\2\u00fb\u00f5\3\2\2\2\u00fc\17\3\2\2"+
                    "\2\u00fd\u00fe\7%\2\2\u00fe\u00ff\5\62\32\2\u00ff\u0100\5B\"\2\u0100\21"+
                    "\3\2\2\2\u0101\u0102\5\62\32\2\u0102\u0103\5B\"\2\u0103\23\3\2\2\2\u0104"+
                    "\u0105\5\62\32\2\u0105\u0106\7*\2\2\u0106\25\3\2\2\2\u0107\u010a\5\30"+
                    "\r\2\u0108\u010a\5\32\16\2\u0109\u0107\3\2\2\2\u0109\u0108\3\2\2\2\u010a"+
                    "\u010d\3\2\2\2\u010b\u0109\3\2\2\2\u010b\u010c\3\2\2\2\u010c\27\3\2\2"+
                    "\2\u010d\u010b\3\2\2\2\u010e\u010f\7\21\2\2\u010f\u0110\7\r\2\2\u0110"+
                    "\u0111\7#\2\2\u0111\31\3\2\2\2\u0112\u0114\7\32\2\2\u0113\u0112\3\2\2"+
                    "\2\u0113\u0114\3\2\2\2\u0114\u0115\3\2\2\2\u0115\u0116\7\"\2\2\u0116\u0117"+
                    "\7\r\2\2\u0117\u011a\5 \21\2\u0118\u011a\7-\2\2\u0119\u0113\3\2\2\2\u0119"+
                    "\u0118\3\2\2\2\u011a\33\3\2\2\2\u011b\u0121\5(\25\2\u011c\u011d\5\66\34"+
                    "\2\u011d\u011e\7\6\2\2\u011e\u011f\5(\25\2\u011f\u0121\3\2\2\2\u0120\u011b"+
                    "\3\2\2\2\u0120\u011c\3\2\2\2\u0121\35\3\2\2\2\u0122\u0123\5 \21\2\u0123"+
                    "\u0124\7\2\2\3\u0124\37\3\2\2\2\u0125\u012a\5\"\22\2\u0126\u0127\7\4\2"+
                    "\2\u0127\u0129\5\"\22\2\u0128\u0126\3\2\2\2\u0129\u012c\3\2\2\2\u012a"+
                    "\u0128\3\2\2\2\u012a\u012b\3\2\2\2\u012b\u0139\3\2\2\2\u012c\u012a\3\2"+
                    "\2\2\u012d\u012e\7\3\2\2\u012e\u0133\5\"\22\2\u012f\u0130\7\4\2\2\u0130"+
                    "\u0132\5\"\22\2\u0131\u012f\3\2\2\2\u0132\u0135\3\2\2\2\u0133\u0131\3"+
                    "\2\2\2\u0133\u0134\3\2\2\2\u0134\u0136\3\2\2\2\u0135\u0133\3\2\2\2\u0136"+
                    "\u0137\7\5\2\2\u0137\u0139\3\2\2\2\u0138\u0125\3\2\2\2\u0138\u012d\3\2"+
                    "\2\2\u0139!\3\2\2\2\u013a\u013c\5$\23\2\u013b\u013d\t\2\2\2\u013c\u013b"+
                    "\3\2\2\2\u013c\u013d\3\2\2\2\u013d\u0140\3\2\2\2\u013e\u013f\7\37\2\2"+
                    "\u013f\u0141\t\3\2\2\u0140\u013e\3\2\2\2\u0140\u0141\3\2\2\2\u0141#\3"+
                    "\2\2\2\u0142\u0150\5\64\33\2\u0143\u0144\5\66\34\2\u0144\u0145\7\3\2\2"+
                    "\u0145\u014a\5&\24\2\u0146\u0147\7\4\2\2\u0147\u0149\5&\24\2\u0148\u0146"+
                    "\3\2\2\2\u0149\u014c\3\2\2\2\u014a\u0148\3\2\2\2\u014a\u014b\3\2\2\2\u014b"+
                    "\u014d\3\2\2\2\u014c\u014a\3\2\2\2\u014d\u014e\7\5\2\2\u014e\u0150\3\2"+
                    "\2\2\u014f\u0142\3\2\2\2\u014f\u0143\3\2\2\2\u0150%\3\2\2\2\u0151\u0154"+
                    "\5\64\33\2\u0152\u0154\5*\26\2\u0153\u0151\3\2\2\2\u0153\u0152\3\2\2\2"+
                    "\u0154\'\3\2\2\2\u0155\u0159\5*\26\2\u0156\u0159\5,\27\2\u0157\u0159\5"+
                    ".\30\2\u0158\u0155\3\2\2\2\u0158\u0156\3\2\2\2\u0158\u0157\3\2\2\2\u0159"+
                    ")\3\2\2\2\u015a\u0165\5\62\32\2\u015b\u0165\5\60\31\2\u015c\u015e\7\67"+
                    "\2\2\u015d\u015c\3\2\2\2\u015e\u015f\3\2\2\2\u015f\u015d\3\2\2\2\u015f"+
                    "\u0160\3\2\2\2\u0160\u0165\3\2\2\2\u0161\u0162\5\66\34\2\u0162\u0163\7"+
                    "\67\2\2\u0163\u0165\3\2\2\2\u0164\u015a\3\2\2\2\u0164\u015b\3\2\2\2\u0164"+
                    "\u015d\3\2\2\2\u0164\u0161\3\2\2\2\u0165+\3\2\2\2\u0166\u0167\7\63\2\2"+
                    "\u0167\u0168\7\3\2\2\u0168\u016d\5*\26\2\u0169\u016a\7\4\2\2\u016a\u016c"+
                    "\5*\26\2\u016b\u0169\3\2\2\2\u016c\u016f\3\2\2\2\u016d\u016b\3\2\2\2\u016d"+
                    "\u016e\3\2\2\2\u016e\u0170\3\2\2\2\u016f\u016d\3\2\2\2\u0170\u0171\7\5"+
                    "\2\2\u0171-\3\2\2\2\u0172\u0173\7\64\2\2\u0173\u0174\7\3\2\2\u0174\u0179"+
                    "\5*\26\2\u0175\u0176\7\4\2\2\u0176\u0178\5*\26\2\u0177\u0175\3\2\2\2\u0178"+
                    "\u017b\3\2\2\2\u0179\u0177\3\2\2\2\u0179\u017a\3\2\2\2\u017a\u017c\3\2"+
                    "\2\2\u017b\u0179\3\2\2\2\u017c\u017d\7\5\2\2\u017d/\3\2\2\2\u017e\u017f"+
                    "\t\4\2\2\u017f\61\3\2\2\2\u0180\u0182\7\66\2\2\u0181\u0180\3\2\2\2\u0181"+
                    "\u0182\3\2\2\2\u0182\u0183\3\2\2\2\u0183\u01a5\7<\2\2\u0184\u0186\7\66"+
                    "\2\2\u0185\u0184\3\2\2\2\u0185\u0186\3\2\2\2\u0186\u0187\3\2\2\2\u0187"+
                    "\u01a5\7=\2\2\u0188\u018a\7\66\2\2\u0189\u0188\3\2\2\2\u0189\u018a\3\2"+
                    "\2\2\u018a\u018b\3\2\2\2\u018b\u01a5\7;\2\2\u018c\u018e\7\66\2\2\u018d"+
                    "\u018c\3\2\2\2\u018d\u018e\3\2\2\2\u018e\u018f\3\2\2\2\u018f\u01a5\78"+
                    "\2\2\u0190\u0192\7\66\2\2\u0191\u0190\3\2\2\2\u0191\u0192\3\2\2\2\u0192"+
                    "\u0193\3\2\2\2\u0193\u01a5\79\2\2\u0194\u0196\7\66\2\2\u0195\u0194\3\2"+
                    "\2\2\u0195\u0196\3\2\2\2\u0196\u0197\3\2\2\2\u0197\u01a5\7:\2\2\u0198"+
                    "\u019a\7\66\2\2\u0199\u0198\3\2\2\2\u0199\u019a\3\2\2\2\u019a\u019b\3"+
                    "\2\2\2\u019b\u01a5\7?\2\2\u019c\u019e\7\66\2\2\u019d\u019c\3\2\2\2\u019d"+
                    "\u019e\3\2\2\2\u019e\u019f\3\2\2\2\u019f\u01a5\7>\2\2\u01a0\u01a2\7\66"+
                    "\2\2\u01a1\u01a0\3\2\2\2\u01a1\u01a2\3\2\2\2\u01a2\u01a3\3\2\2\2\u01a3"+
                    "\u01a5\7@\2\2\u01a4\u0181\3\2\2\2\u01a4\u0185\3\2\2\2\u01a4\u0189\3\2"+
                    "\2\2\u01a4\u018d\3\2\2\2\u01a4\u0191\3\2\2\2\u01a4\u0195\3\2\2\2\u01a4"+
                    "\u0199\3\2\2\2\u01a4\u019d\3\2\2\2\u01a4\u01a1\3\2\2\2\u01a5\63\3\2\2"+
                    "\2\u01a6\u01ab\5\66\34\2\u01a7\u01a8\7\7\2\2\u01a8\u01aa\5\66\34\2\u01a9"+
                    "\u01a7\3\2\2\2\u01aa\u01ad\3\2\2\2\u01ab\u01a9\3\2\2\2\u01ab\u01ac\3\2"+
                    "\2\2\u01ac\65\3\2\2\2\u01ad\u01ab\3\2\2\2\u01ae\u01b2\7A\2\2\u01af\u01b2"+
                    "\58\35\2\u01b0\u01b2\5<\37\2\u01b1\u01ae\3\2\2\2\u01b1\u01af\3\2\2\2\u01b1"+
                    "\u01b0\3\2\2\2\u01b2\67\3\2\2\2\u01b3\u01b4\7B\2\2\u01b49\3\2\2\2\u01b5"+
                    "\u01ba\5\64\33\2\u01b6\u01b7\7\4\2\2\u01b7\u01b9\5\64\33\2\u01b8\u01b6"+
                    "\3\2\2\2\u01b9\u01bc\3\2\2\2\u01ba\u01b8\3\2\2\2\u01ba\u01bb\3\2\2\2\u01bb"+
                    ";\3\2\2\2\u01bc\u01ba\3\2\2\2\u01bd\u01be\t\5\2\2\u01be=\3\2\2\2\u01bf"+
                    "\u01c0\5\62\32\2\u01c0?\3\2\2\2\u01c1\u01c2\5\62\32\2\u01c2A\3\2\2\2\u01c3"+
                    "\u01c4\t\6\2\2\u01c4C\3\2\2\2\64OR_t\u009d\u00a8\u00ac\u00b0\u00bc\u00c1"+
                    "\u00c5\u00d1\u00d6\u00dc\u00df\u00e5\u00e8\u00eb\u00fb\u0109\u010b\u0113"+
                    "\u0119\u0120\u012a\u0133\u0138\u013c\u0140\u014a\u014f\u0153\u0158\u015f"+
                    "\u0164\u016d\u0179\u0181\u0185\u0189\u018d\u0191\u0195\u0199\u019d\u01a1"+
                    "\u01a4\u01ab\u01b1\u01ba";
    public static final ATN _ATN =
            new ATNDeserializer().deserialize(_serializedATN.toCharArray());
    static {
        _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
        for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
            _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
        }
    }
}
