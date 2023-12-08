// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
package org.apache.spark.sql.catalyst.parser.extensions;

import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.ATN;
import org.antlr.v4.runtime.atn.ATNDeserializer;
import org.antlr.v4.runtime.atn.LexerATNSimulator;
import org.antlr.v4.runtime.atn.PredictionContextCache;
import org.antlr.v4.runtime.dfa.DFA;

public class LakeSoulSqlExtensionsLexer extends Lexer {
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
    public static String[] channelNames = {
            "DEFAULT_TOKEN_CHANNEL", "HIDDEN"
    };

    public static String[] modeNames = {
            "DEFAULT_MODE"
    };

    private static String[] makeRuleNames() {
        return new String[] {
                "T__0", "T__1", "T__2", "T__3", "T__4", "ADD", "ALTER", "AS", "ASC",
                "BRANCH", "BY", "CALL", "DAYS", "DESC", "DISTRIBUTED", "DROP", "EXISTS",
                "FIELD", "FIELDS", "FIRST", "HOURS", "IF", "LAST", "LOCALLY", "MINUTES",
                "MONTHS", "CREATE", "NOT", "NULLS", "OF", "OR", "ORDERED", "PARTITION",
                "REPLACE", "RETAIN", "RETENTION", "IDENTIFIER_KW", "SET", "SNAPSHOT",
                "SNAPSHOTS", "TABLE", "TAG", "UNORDERED", "VERSION", "WITH", "WRITE",
                "TRUE", "FALSE", "MAP", "ARRAY", "PLUS", "MINUS", "STRING", "BIGINT_LITERAL",
                "SMALLINT_LITERAL", "TINYINT_LITERAL", "INTEGER_VALUE", "EXPONENT_VALUE",
                "DECIMAL_VALUE", "FLOAT_LITERAL", "DOUBLE_LITERAL", "BIGDECIMAL_LITERAL",
                "IDENTIFIER", "BACKQUOTED_IDENTIFIER", "DECIMAL_DIGITS", "EXPONENT",
                "DIGIT", "LETTER", "SIMPLE_COMMENT", "BRACKETED_COMMENT", "WS", "UNRECOGNIZED"
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


    /**
     * Verify whether current token is a valid decimal token (which contains dot).
     * Returns true if the character that follows the token is not a digit or letter or underscore.
     *
     * For example:
     * For char stream "2.3", "2." is not a valid decimal token, because it is followed by digit '3'.
     * For char stream "2.3_", "2.3" is not a valid decimal token, because it is followed by '_'.
     * For char stream "2.3W", "2.3" is not a valid decimal token, because it is followed by 'W'.
     * For char stream "12.0D 34.E2+0.12 "  12.0D is a valid decimal token because it is followed
     * by a space. 34.E2 is a valid decimal token because it is followed by symbol '+'
     * which is not a digit or letter or underscore.
     */
    public boolean isValidDecimal() {
        int nextChar = _input.LA(1);
        if (nextChar >= 'A' && nextChar <= 'Z' || nextChar >= '0' && nextChar <= '9' ||
                nextChar == '_') {
            return false;
        } else {
            return true;
        }
    }

    /**
     * This method will be called when we see '/*' and try to match it as a bracketed comment.
     * If the next character is '+', it should be parsed as hint later, and we cannot match
     * it as a bracketed comment.
     *
     * Returns true if the next character is '+'.
     */
    public boolean isHint() {
        int nextChar = _input.LA(1);
        if (nextChar == '+') {
            return true;
        } else {
            return false;
        }
    }


    public LakeSoulSqlExtensionsLexer(CharStream input) {
        super(input);
        _interp = new LexerATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
    }

    @Override
    public String getGrammarFileName() { return "LakeSoulSqlExtensions.g4"; }

    @Override
    public String[] getRuleNames() { return ruleNames; }

    @Override
    public String getSerializedATN() { return _serializedATN; }

    @Override
    public String[] getChannelNames() { return channelNames; }

    @Override
    public String[] getModeNames() { return modeNames; }

    @Override
    public ATN getATN() { return _ATN; }

    @Override
    public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
        switch (ruleIndex) {
            case 57:
                return EXPONENT_VALUE_sempred((RuleContext)_localctx, predIndex);
            case 58:
                return DECIMAL_VALUE_sempred((RuleContext)_localctx, predIndex);
            case 59:
                return FLOAT_LITERAL_sempred((RuleContext)_localctx, predIndex);
            case 60:
                return DOUBLE_LITERAL_sempred((RuleContext)_localctx, predIndex);
            case 61:
                return BIGDECIMAL_LITERAL_sempred((RuleContext)_localctx, predIndex);
            case 69:
                return BRACKETED_COMMENT_sempred((RuleContext)_localctx, predIndex);
        }
        return true;
    }
    private boolean EXPONENT_VALUE_sempred(RuleContext _localctx, int predIndex) {
        switch (predIndex) {
            case 0:
                return isValidDecimal();
        }
        return true;
    }
    private boolean DECIMAL_VALUE_sempred(RuleContext _localctx, int predIndex) {
        switch (predIndex) {
            case 1:
                return isValidDecimal();
        }
        return true;
    }
    private boolean FLOAT_LITERAL_sempred(RuleContext _localctx, int predIndex) {
        switch (predIndex) {
            case 2:
                return isValidDecimal();
        }
        return true;
    }
    private boolean DOUBLE_LITERAL_sempred(RuleContext _localctx, int predIndex) {
        switch (predIndex) {
            case 3:
                return isValidDecimal();
        }
        return true;
    }
    private boolean BIGDECIMAL_LITERAL_sempred(RuleContext _localctx, int predIndex) {
        switch (predIndex) {
            case 4:
                return isValidDecimal();
        }
        return true;
    }
    private boolean BRACKETED_COMMENT_sempred(RuleContext _localctx, int predIndex) {
        switch (predIndex) {
            case 5:
                return !isHint();
        }
        return true;
    }

    public static final String _serializedATN =
            "\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\2F\u0297\b\1\4\2\t"+
                    "\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"+
                    "\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
                    "\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
                    "\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
                    "\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t+\4"+
                    ",\t,\4-\t-\4.\t.\4/\t/\4\60\t\60\4\61\t\61\4\62\t\62\4\63\t\63\4\64\t"+
                    "\64\4\65\t\65\4\66\t\66\4\67\t\67\48\t8\49\t9\4:\t:\4;\t;\4<\t<\4=\t="+
                    "\4>\t>\4?\t?\4@\t@\4A\tA\4B\tB\4C\tC\4D\tD\4E\tE\4F\tF\4G\tG\4H\tH\4I"+
                    "\tI\3\2\3\2\3\3\3\3\3\4\3\4\3\5\3\5\3\5\3\6\3\6\3\7\3\7\3\7\3\7\3\b\3"+
                    "\b\3\b\3\b\3\b\3\b\3\t\3\t\3\t\3\n\3\n\3\n\3\n\3\13\3\13\3\13\3\13\3\13"+
                    "\3\13\3\13\3\f\3\f\3\f\3\r\3\r\3\r\3\r\3\r\3\16\3\16\3\16\3\16\3\16\3"+
                    "\17\3\17\3\17\3\17\3\17\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3\20\3"+
                    "\20\3\20\3\20\3\21\3\21\3\21\3\21\3\21\3\22\3\22\3\22\3\22\3\22\3\22\3"+
                    "\22\3\23\3\23\3\23\3\23\3\23\3\23\3\24\3\24\3\24\3\24\3\24\3\24\3\24\3"+
                    "\25\3\25\3\25\3\25\3\25\3\25\3\26\3\26\3\26\3\26\3\26\3\26\3\27\3\27\3"+
                    "\27\3\30\3\30\3\30\3\30\3\30\3\31\3\31\3\31\3\31\3\31\3\31\3\31\3\31\3"+
                    "\32\3\32\3\32\3\32\3\32\3\32\3\32\3\32\3\33\3\33\3\33\3\33\3\33\3\33\3"+
                    "\33\3\34\3\34\3\34\3\34\3\34\3\34\3\34\3\35\3\35\3\35\3\35\3\36\3\36\3"+
                    "\36\3\36\3\36\3\36\3\37\3\37\3\37\3 \3 \3 \3!\3!\3!\3!\3!\3!\3!\3!\3\""+
                    "\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3#\3#\3#\3#\3#\3#\3#\3#\3$\3$\3$"+
                    "\3$\3$\3$\3$\3%\3%\3%\3%\3%\3%\3%\3%\3%\3%\3&\3&\3&\3&\3&\3&\3&\3&\3&"+
                    "\3&\3&\3\'\3\'\3\'\3\'\3(\3(\3(\3(\3(\3(\3(\3(\3(\3)\3)\3)\3)\3)\3)\3"+
                    ")\3)\3)\3)\3*\3*\3*\3*\3*\3*\3+\3+\3+\3+\3,\3,\3,\3,\3,\3,\3,\3,\3,\3"+
                    ",\3-\3-\3-\3-\3-\3-\3-\3-\3.\3.\3.\3.\3.\3/\3/\3/\3/\3/\3/\3\60\3\60\3"+
                    "\60\3\60\3\60\3\61\3\61\3\61\3\61\3\61\3\61\3\62\3\62\3\62\3\62\3\63\3"+
                    "\63\3\63\3\63\3\63\3\63\3\64\3\64\3\65\3\65\3\66\3\66\3\66\3\66\7\66\u01c1"+
                    "\n\66\f\66\16\66\u01c4\13\66\3\66\3\66\3\66\3\66\3\66\7\66\u01cb\n\66"+
                    "\f\66\16\66\u01ce\13\66\3\66\5\66\u01d1\n\66\3\67\6\67\u01d4\n\67\r\67"+
                    "\16\67\u01d5\3\67\3\67\38\68\u01db\n8\r8\168\u01dc\38\38\39\69\u01e2\n"+
                    "9\r9\169\u01e3\39\39\3:\6:\u01e9\n:\r:\16:\u01ea\3;\6;\u01ee\n;\r;\16"+
                    ";\u01ef\3;\3;\3;\3;\3;\3;\5;\u01f8\n;\3<\3<\3<\3=\6=\u01fe\n=\r=\16=\u01ff"+
                    "\3=\5=\u0203\n=\3=\3=\3=\3=\5=\u0209\n=\3=\3=\3=\5=\u020e\n=\3>\6>\u0211"+
                    "\n>\r>\16>\u0212\3>\5>\u0216\n>\3>\3>\3>\3>\5>\u021c\n>\3>\3>\3>\5>\u0221"+
                    "\n>\3?\6?\u0224\n?\r?\16?\u0225\3?\5?\u0229\n?\3?\3?\3?\3?\3?\5?\u0230"+
                    "\n?\3?\3?\3?\3?\3?\5?\u0237\n?\3@\3@\3@\6@\u023c\n@\r@\16@\u023d\3A\3"+
                    "A\3A\3A\7A\u0244\nA\fA\16A\u0247\13A\3A\3A\3B\6B\u024c\nB\rB\16B\u024d"+
                    "\3B\3B\7B\u0252\nB\fB\16B\u0255\13B\3B\3B\6B\u0259\nB\rB\16B\u025a\5B"+
                    "\u025d\nB\3C\3C\5C\u0261\nC\3C\6C\u0264\nC\rC\16C\u0265\3D\3D\3E\3E\3"+
                    "F\3F\3F\3F\3F\3F\7F\u0272\nF\fF\16F\u0275\13F\3F\5F\u0278\nF\3F\5F\u027b"+
                    "\nF\3F\3F\3G\3G\3G\3G\3G\3G\7G\u0285\nG\fG\16G\u0288\13G\3G\3G\3G\3G\3"+
                    "G\3H\6H\u0290\nH\rH\16H\u0291\3H\3H\3I\3I\3\u0286\2J\3\3\5\4\7\5\t\6\13"+
                    "\7\r\b\17\t\21\n\23\13\25\f\27\r\31\16\33\17\35\20\37\21!\22#\23%\24\'"+
                    "\25)\26+\27-\30/\31\61\32\63\33\65\34\67\359\36;\37= ?!A\"C#E$G%I&K\'"+
                    "M(O)Q*S+U,W-Y.[/]\60_\61a\62c\63e\64g\65i\66k\67m8o9q:s;u<w=y>{?}@\177"+
                    "A\u0081B\u0083\2\u0085\2\u0087\2\u0089\2\u008bC\u008dD\u008fE\u0091F\3"+
                    "\2\n\4\2))^^\4\2$$^^\3\2bb\4\2--//\3\2\62;\3\2C\\\4\2\f\f\17\17\5\2\13"+
                    "\f\17\17\"\"\2\u02bb\2\3\3\2\2\2\2\5\3\2\2\2\2\7\3\2\2\2\2\t\3\2\2\2\2"+
                    "\13\3\2\2\2\2\r\3\2\2\2\2\17\3\2\2\2\2\21\3\2\2\2\2\23\3\2\2\2\2\25\3"+
                    "\2\2\2\2\27\3\2\2\2\2\31\3\2\2\2\2\33\3\2\2\2\2\35\3\2\2\2\2\37\3\2\2"+
                    "\2\2!\3\2\2\2\2#\3\2\2\2\2%\3\2\2\2\2\'\3\2\2\2\2)\3\2\2\2\2+\3\2\2\2"+
                    "\2-\3\2\2\2\2/\3\2\2\2\2\61\3\2\2\2\2\63\3\2\2\2\2\65\3\2\2\2\2\67\3\2"+
                    "\2\2\29\3\2\2\2\2;\3\2\2\2\2=\3\2\2\2\2?\3\2\2\2\2A\3\2\2\2\2C\3\2\2\2"+
                    "\2E\3\2\2\2\2G\3\2\2\2\2I\3\2\2\2\2K\3\2\2\2\2M\3\2\2\2\2O\3\2\2\2\2Q"+
                    "\3\2\2\2\2S\3\2\2\2\2U\3\2\2\2\2W\3\2\2\2\2Y\3\2\2\2\2[\3\2\2\2\2]\3\2"+
                    "\2\2\2_\3\2\2\2\2a\3\2\2\2\2c\3\2\2\2\2e\3\2\2\2\2g\3\2\2\2\2i\3\2\2\2"+
                    "\2k\3\2\2\2\2m\3\2\2\2\2o\3\2\2\2\2q\3\2\2\2\2s\3\2\2\2\2u\3\2\2\2\2w"+
                    "\3\2\2\2\2y\3\2\2\2\2{\3\2\2\2\2}\3\2\2\2\2\177\3\2\2\2\2\u0081\3\2\2"+
                    "\2\2\u008b\3\2\2\2\2\u008d\3\2\2\2\2\u008f\3\2\2\2\2\u0091\3\2\2\2\3\u0093"+
                    "\3\2\2\2\5\u0095\3\2\2\2\7\u0097\3\2\2\2\t\u0099\3\2\2\2\13\u009c\3\2"+
                    "\2\2\r\u009e\3\2\2\2\17\u00a2\3\2\2\2\21\u00a8\3\2\2\2\23\u00ab\3\2\2"+
                    "\2\25\u00af\3\2\2\2\27\u00b6\3\2\2\2\31\u00b9\3\2\2\2\33\u00be\3\2\2\2"+
                    "\35\u00c3\3\2\2\2\37\u00c8\3\2\2\2!\u00d4\3\2\2\2#\u00d9\3\2\2\2%\u00e0"+
                    "\3\2\2\2\'\u00e6\3\2\2\2)\u00ed\3\2\2\2+\u00f3\3\2\2\2-\u00f9\3\2\2\2"+
                    "/\u00fc\3\2\2\2\61\u0101\3\2\2\2\63\u0109\3\2\2\2\65\u0111\3\2\2\2\67"+
                    "\u0118\3\2\2\29\u011f\3\2\2\2;\u0123\3\2\2\2=\u0129\3\2\2\2?\u012c\3\2"+
                    "\2\2A\u012f\3\2\2\2C\u0137\3\2\2\2E\u0141\3\2\2\2G\u0149\3\2\2\2I\u0150"+
                    "\3\2\2\2K\u015a\3\2\2\2M\u0165\3\2\2\2O\u0169\3\2\2\2Q\u0172\3\2\2\2S"+
                    "\u017c\3\2\2\2U\u0182\3\2\2\2W\u0186\3\2\2\2Y\u0190\3\2\2\2[\u0198\3\2"+
                    "\2\2]\u019d\3\2\2\2_\u01a3\3\2\2\2a\u01a8\3\2\2\2c\u01ae\3\2\2\2e\u01b2"+
                    "\3\2\2\2g\u01b8\3\2\2\2i\u01ba\3\2\2\2k\u01d0\3\2\2\2m\u01d3\3\2\2\2o"+
                    "\u01da\3\2\2\2q\u01e1\3\2\2\2s\u01e8\3\2\2\2u\u01f7\3\2\2\2w\u01f9\3\2"+
                    "\2\2y\u020d\3\2\2\2{\u0220\3\2\2\2}\u0236\3\2\2\2\177\u023b\3\2\2\2\u0081"+
                    "\u023f\3\2\2\2\u0083\u025c\3\2\2\2\u0085\u025e\3\2\2\2\u0087\u0267\3\2"+
                    "\2\2\u0089\u0269\3\2\2\2\u008b\u026b\3\2\2\2\u008d\u027e\3\2\2\2\u008f"+
                    "\u028f\3\2\2\2\u0091\u0295\3\2\2\2\u0093\u0094\7*\2\2\u0094\4\3\2\2\2"+
                    "\u0095\u0096\7.\2\2\u0096\6\3\2\2\2\u0097\u0098\7+\2\2\u0098\b\3\2\2\2"+
                    "\u0099\u009a\7?\2\2\u009a\u009b\7@\2\2\u009b\n\3\2\2\2\u009c\u009d\7\60"+
                    "\2\2\u009d\f\3\2\2\2\u009e\u009f\7C\2\2\u009f\u00a0\7F\2\2\u00a0\u00a1"+
                    "\7F\2\2\u00a1\16\3\2\2\2\u00a2\u00a3\7C\2\2\u00a3\u00a4\7N\2\2\u00a4\u00a5"+
                    "\7V\2\2\u00a5\u00a6\7G\2\2\u00a6\u00a7\7T\2\2\u00a7\20\3\2\2\2\u00a8\u00a9"+
                    "\7C\2\2\u00a9\u00aa\7U\2\2\u00aa\22\3\2\2\2\u00ab\u00ac\7C\2\2\u00ac\u00ad"+
                    "\7U\2\2\u00ad\u00ae\7E\2\2\u00ae\24\3\2\2\2\u00af\u00b0\7D\2\2\u00b0\u00b1"+
                    "\7T\2\2\u00b1\u00b2\7C\2\2\u00b2\u00b3\7P\2\2\u00b3\u00b4\7E\2\2\u00b4"+
                    "\u00b5\7J\2\2\u00b5\26\3\2\2\2\u00b6\u00b7\7D\2\2\u00b7\u00b8\7[\2\2\u00b8"+
                    "\30\3\2\2\2\u00b9\u00ba\7E\2\2\u00ba\u00bb\7C\2\2\u00bb\u00bc\7N\2\2\u00bc"+
                    "\u00bd\7N\2\2\u00bd\32\3\2\2\2\u00be\u00bf\7F\2\2\u00bf\u00c0\7C\2\2\u00c0"+
                    "\u00c1\7[\2\2\u00c1\u00c2\7U\2\2\u00c2\34\3\2\2\2\u00c3\u00c4\7F\2\2\u00c4"+
                    "\u00c5\7G\2\2\u00c5\u00c6\7U\2\2\u00c6\u00c7\7E\2\2\u00c7\36\3\2\2\2\u00c8"+
                    "\u00c9\7F\2\2\u00c9\u00ca\7K\2\2\u00ca\u00cb\7U\2\2\u00cb\u00cc\7V\2\2"+
                    "\u00cc\u00cd\7T\2\2\u00cd\u00ce\7K\2\2\u00ce\u00cf\7D\2\2\u00cf\u00d0"+
                    "\7W\2\2\u00d0\u00d1\7V\2\2\u00d1\u00d2\7G\2\2\u00d2\u00d3\7F\2\2\u00d3"+
                    " \3\2\2\2\u00d4\u00d5\7F\2\2\u00d5\u00d6\7T\2\2\u00d6\u00d7\7Q\2\2\u00d7"+
                    "\u00d8\7R\2\2\u00d8\"\3\2\2\2\u00d9\u00da\7G\2\2\u00da\u00db\7Z\2\2\u00db"+
                    "\u00dc\7K\2\2\u00dc\u00dd\7U\2\2\u00dd\u00de\7V\2\2\u00de\u00df\7U\2\2"+
                    "\u00df$\3\2\2\2\u00e0\u00e1\7H\2\2\u00e1\u00e2\7K\2\2\u00e2\u00e3\7G\2"+
                    "\2\u00e3\u00e4\7N\2\2\u00e4\u00e5\7F\2\2\u00e5&\3\2\2\2\u00e6\u00e7\7"+
                    "H\2\2\u00e7\u00e8\7K\2\2\u00e8\u00e9\7G\2\2\u00e9\u00ea\7N\2\2\u00ea\u00eb"+
                    "\7F\2\2\u00eb\u00ec\7U\2\2\u00ec(\3\2\2\2\u00ed\u00ee\7H\2\2\u00ee\u00ef"+
                    "\7K\2\2\u00ef\u00f0\7T\2\2\u00f0\u00f1\7U\2\2\u00f1\u00f2\7V\2\2\u00f2"+
                    "*\3\2\2\2\u00f3\u00f4\7J\2\2\u00f4\u00f5\7Q\2\2\u00f5\u00f6\7W\2\2\u00f6"+
                    "\u00f7\7T\2\2\u00f7\u00f8\7U\2\2\u00f8,\3\2\2\2\u00f9\u00fa\7K\2\2\u00fa"+
                    "\u00fb\7H\2\2\u00fb.\3\2\2\2\u00fc\u00fd\7N\2\2\u00fd\u00fe\7C\2\2\u00fe"+
                    "\u00ff\7U\2\2\u00ff\u0100\7V\2\2\u0100\60\3\2\2\2\u0101\u0102\7N\2\2\u0102"+
                    "\u0103\7Q\2\2\u0103\u0104\7E\2\2\u0104\u0105\7C\2\2\u0105\u0106\7N\2\2"+
                    "\u0106\u0107\7N\2\2\u0107\u0108\7[\2\2\u0108\62\3\2\2\2\u0109\u010a\7"+
                    "O\2\2\u010a\u010b\7K\2\2\u010b\u010c\7P\2\2\u010c\u010d\7W\2\2\u010d\u010e"+
                    "\7V\2\2\u010e\u010f\7G\2\2\u010f\u0110\7U\2\2\u0110\64\3\2\2\2\u0111\u0112"+
                    "\7O\2\2\u0112\u0113\7Q\2\2\u0113\u0114\7P\2\2\u0114\u0115\7V\2\2\u0115"+
                    "\u0116\7J\2\2\u0116\u0117\7U\2\2\u0117\66\3\2\2\2\u0118\u0119\7E\2\2\u0119"+
                    "\u011a\7T\2\2\u011a\u011b\7G\2\2\u011b\u011c\7C\2\2\u011c\u011d\7V\2\2"+
                    "\u011d\u011e\7G\2\2\u011e8\3\2\2\2\u011f\u0120\7P\2\2\u0120\u0121\7Q\2"+
                    "\2\u0121\u0122\7V\2\2\u0122:\3\2\2\2\u0123\u0124\7P\2\2\u0124\u0125\7"+
                    "W\2\2\u0125\u0126\7N\2\2\u0126\u0127\7N\2\2\u0127\u0128\7U\2\2\u0128<"+
                    "\3\2\2\2\u0129\u012a\7Q\2\2\u012a\u012b\7H\2\2\u012b>\3\2\2\2\u012c\u012d"+
                    "\7Q\2\2\u012d\u012e\7T\2\2\u012e@\3\2\2\2\u012f\u0130\7Q\2\2\u0130\u0131"+
                    "\7T\2\2\u0131\u0132\7F\2\2\u0132\u0133\7G\2\2\u0133\u0134\7T\2\2\u0134"+
                    "\u0135\7G\2\2\u0135\u0136\7F\2\2\u0136B\3\2\2\2\u0137\u0138\7R\2\2\u0138"+
                    "\u0139\7C\2\2\u0139\u013a\7T\2\2\u013a\u013b\7V\2\2\u013b\u013c\7K\2\2"+
                    "\u013c\u013d\7V\2\2\u013d\u013e\7K\2\2\u013e\u013f\7Q\2\2\u013f\u0140"+
                    "\7P\2\2\u0140D\3\2\2\2\u0141\u0142\7T\2\2\u0142\u0143\7G\2\2\u0143\u0144"+
                    "\7R\2\2\u0144\u0145\7N\2\2\u0145\u0146\7C\2\2\u0146\u0147\7E\2\2\u0147"+
                    "\u0148\7G\2\2\u0148F\3\2\2\2\u0149\u014a\7T\2\2\u014a\u014b\7G\2\2\u014b"+
                    "\u014c\7V\2\2\u014c\u014d\7C\2\2\u014d\u014e\7K\2\2\u014e\u014f\7P\2\2"+
                    "\u014fH\3\2\2\2\u0150\u0151\7T\2\2\u0151\u0152\7G\2\2\u0152\u0153\7V\2"+
                    "\2\u0153\u0154\7G\2\2\u0154\u0155\7P\2\2\u0155\u0156\7V\2\2\u0156\u0157"+
                    "\7K\2\2\u0157\u0158\7Q\2\2\u0158\u0159\7P\2\2\u0159J\3\2\2\2\u015a\u015b"+
                    "\7K\2\2\u015b\u015c\7F\2\2\u015c\u015d\7G\2\2\u015d\u015e\7P\2\2\u015e"+
                    "\u015f\7V\2\2\u015f\u0160\7K\2\2\u0160\u0161\7H\2\2\u0161\u0162\7K\2\2"+
                    "\u0162\u0163\7G\2\2\u0163\u0164\7T\2\2\u0164L\3\2\2\2\u0165\u0166\7U\2"+
                    "\2\u0166\u0167\7G\2\2\u0167\u0168\7V\2\2\u0168N\3\2\2\2\u0169\u016a\7"+
                    "U\2\2\u016a\u016b\7P\2\2\u016b\u016c\7C\2\2\u016c\u016d\7R\2\2\u016d\u016e"+
                    "\7U\2\2\u016e\u016f\7J\2\2\u016f\u0170\7Q\2\2\u0170\u0171\7V\2\2\u0171"+
                    "P\3\2\2\2\u0172\u0173\7U\2\2\u0173\u0174\7P\2\2\u0174\u0175\7C\2\2\u0175"+
                    "\u0176\7R\2\2\u0176\u0177\7U\2\2\u0177\u0178\7J\2\2\u0178\u0179\7Q\2\2"+
                    "\u0179\u017a\7V\2\2\u017a\u017b\7U\2\2\u017bR\3\2\2\2\u017c\u017d\7V\2"+
                    "\2\u017d\u017e\7C\2\2\u017e\u017f\7D\2\2\u017f\u0180\7N\2\2\u0180\u0181"+
                    "\7G\2\2\u0181T\3\2\2\2\u0182\u0183\7V\2\2\u0183\u0184\7C\2\2\u0184\u0185"+
                    "\7I\2\2\u0185V\3\2\2\2\u0186\u0187\7W\2\2\u0187\u0188\7P\2\2\u0188\u0189"+
                    "\7Q\2\2\u0189\u018a\7T\2\2\u018a\u018b\7F\2\2\u018b\u018c\7G\2\2\u018c"+
                    "\u018d\7T\2\2\u018d\u018e\7G\2\2\u018e\u018f\7F\2\2\u018fX\3\2\2\2\u0190"+
                    "\u0191\7X\2\2\u0191\u0192\7G\2\2\u0192\u0193\7T\2\2\u0193\u0194\7U\2\2"+
                    "\u0194\u0195\7K\2\2\u0195\u0196\7Q\2\2\u0196\u0197\7P\2\2\u0197Z\3\2\2"+
                    "\2\u0198\u0199\7Y\2\2\u0199\u019a\7K\2\2\u019a\u019b\7V\2\2\u019b\u019c"+
                    "\7J\2\2\u019c\\\3\2\2\2\u019d\u019e\7Y\2\2\u019e\u019f\7T\2\2\u019f\u01a0"+
                    "\7K\2\2\u01a0\u01a1\7V\2\2\u01a1\u01a2\7G\2\2\u01a2^\3\2\2\2\u01a3\u01a4"+
                    "\7V\2\2\u01a4\u01a5\7T\2\2\u01a5\u01a6\7W\2\2\u01a6\u01a7\7G\2\2\u01a7"+
                    "`\3\2\2\2\u01a8\u01a9\7H\2\2\u01a9\u01aa\7C\2\2\u01aa\u01ab\7N\2\2\u01ab"+
                    "\u01ac\7U\2\2\u01ac\u01ad\7G\2\2\u01adb\3\2\2\2\u01ae\u01af\7O\2\2\u01af"+
                    "\u01b0\7C\2\2\u01b0\u01b1\7R\2\2\u01b1d\3\2\2\2\u01b2\u01b3\7C\2\2\u01b3"+
                    "\u01b4\7T\2\2\u01b4\u01b5\7T\2\2\u01b5\u01b6\7C\2\2\u01b6\u01b7\7[\2\2"+
                    "\u01b7f\3\2\2\2\u01b8\u01b9\7-\2\2\u01b9h\3\2\2\2\u01ba\u01bb\7/\2\2\u01bb"+
                    "j\3\2\2\2\u01bc\u01c2\7)\2\2\u01bd\u01c1\n\2\2\2\u01be\u01bf\7^\2\2\u01bf"+
                    "\u01c1\13\2\2\2\u01c0\u01bd\3\2\2\2\u01c0\u01be\3\2\2\2\u01c1\u01c4\3"+
                    "\2\2\2\u01c2\u01c0\3\2\2\2\u01c2\u01c3\3\2\2\2\u01c3\u01c5\3\2\2\2\u01c4"+
                    "\u01c2\3\2\2\2\u01c5\u01d1\7)\2\2\u01c6\u01cc\7$\2\2\u01c7\u01cb\n\3\2"+
                    "\2\u01c8\u01c9\7^\2\2\u01c9\u01cb\13\2\2\2\u01ca\u01c7\3\2\2\2\u01ca\u01c8"+
                    "\3\2\2\2\u01cb\u01ce\3\2\2\2\u01cc\u01ca\3\2\2\2\u01cc\u01cd\3\2\2\2\u01cd"+
                    "\u01cf\3\2\2\2\u01ce\u01cc\3\2\2\2\u01cf\u01d1\7$\2\2\u01d0\u01bc\3\2"+
                    "\2\2\u01d0\u01c6\3\2\2\2\u01d1l\3\2\2\2\u01d2\u01d4\5\u0087D\2\u01d3\u01d2"+
                    "\3\2\2\2\u01d4\u01d5\3\2\2\2\u01d5\u01d3\3\2\2\2\u01d5\u01d6\3\2\2\2\u01d6"+
                    "\u01d7\3\2\2\2\u01d7\u01d8\7N\2\2\u01d8n\3\2\2\2\u01d9\u01db\5\u0087D"+
                    "\2\u01da\u01d9\3\2\2\2\u01db\u01dc\3\2\2\2\u01dc\u01da\3\2\2\2\u01dc\u01dd"+
                    "\3\2\2\2\u01dd\u01de\3\2\2\2\u01de\u01df\7U\2\2\u01dfp\3\2\2\2\u01e0\u01e2"+
                    "\5\u0087D\2\u01e1\u01e0\3\2\2\2\u01e2\u01e3\3\2\2\2\u01e3\u01e1\3\2\2"+
                    "\2\u01e3\u01e4\3\2\2\2\u01e4\u01e5\3\2\2\2\u01e5\u01e6\7[\2\2\u01e6r\3"+
                    "\2\2\2\u01e7\u01e9\5\u0087D\2\u01e8\u01e7\3\2\2\2\u01e9\u01ea\3\2\2\2"+
                    "\u01ea\u01e8\3\2\2\2\u01ea\u01eb\3\2\2\2\u01ebt\3\2\2\2\u01ec\u01ee\5"+
                    "\u0087D\2\u01ed\u01ec\3\2\2\2\u01ee\u01ef\3\2\2\2\u01ef\u01ed\3\2\2\2"+
                    "\u01ef\u01f0\3\2\2\2\u01f0\u01f1\3\2\2\2\u01f1\u01f2\5\u0085C\2\u01f2"+
                    "\u01f8\3\2\2\2\u01f3\u01f4\5\u0083B\2\u01f4\u01f5\5\u0085C\2\u01f5\u01f6"+
                    "\6;\2\2\u01f6\u01f8\3\2\2\2\u01f7\u01ed\3\2\2\2\u01f7\u01f3\3\2\2\2\u01f8"+
                    "v\3\2\2\2\u01f9\u01fa\5\u0083B\2\u01fa\u01fb\6<\3\2\u01fbx\3\2\2\2\u01fc"+
                    "\u01fe\5\u0087D\2\u01fd\u01fc\3\2\2\2\u01fe\u01ff\3\2\2\2\u01ff\u01fd"+
                    "\3\2\2\2\u01ff\u0200\3\2\2\2\u0200\u0202\3\2\2\2\u0201\u0203\5\u0085C"+
                    "\2\u0202\u0201\3\2\2\2\u0202\u0203\3\2\2\2\u0203\u0204\3\2\2\2\u0204\u0205"+
                    "\7H\2\2\u0205\u020e\3\2\2\2\u0206\u0208\5\u0083B\2\u0207\u0209\5\u0085"+
                    "C\2\u0208\u0207\3\2\2\2\u0208\u0209\3\2\2\2\u0209\u020a\3\2\2\2\u020a"+
                    "\u020b\7H\2\2\u020b\u020c\6=\4\2\u020c\u020e\3\2\2\2\u020d\u01fd\3\2\2"+
                    "\2\u020d\u0206\3\2\2\2\u020ez\3\2\2\2\u020f\u0211\5\u0087D\2\u0210\u020f"+
                    "\3\2\2\2\u0211\u0212\3\2\2\2\u0212\u0210\3\2\2\2\u0212\u0213\3\2\2\2\u0213"+
                    "\u0215\3\2\2\2\u0214\u0216\5\u0085C\2\u0215\u0214\3\2\2\2\u0215\u0216"+
                    "\3\2\2\2\u0216\u0217\3\2\2\2\u0217\u0218\7F\2\2\u0218\u0221\3\2\2\2\u0219"+
                    "\u021b\5\u0083B\2\u021a\u021c\5\u0085C\2\u021b\u021a\3\2\2\2\u021b\u021c"+
                    "\3\2\2\2\u021c\u021d\3\2\2\2\u021d\u021e\7F\2\2\u021e\u021f\6>\5\2\u021f"+
                    "\u0221\3\2\2\2\u0220\u0210\3\2\2\2\u0220\u0219\3\2\2\2\u0221|\3\2\2\2"+
                    "\u0222\u0224\5\u0087D\2\u0223\u0222\3\2\2\2\u0224\u0225\3\2\2\2\u0225"+
                    "\u0223\3\2\2\2\u0225\u0226\3\2\2\2\u0226\u0228\3\2\2\2\u0227\u0229\5\u0085"+
                    "C\2\u0228\u0227\3\2\2\2\u0228\u0229\3\2\2\2\u0229\u022a\3\2\2\2\u022a"+
                    "\u022b\7D\2\2\u022b\u022c\7F\2\2\u022c\u0237\3\2\2\2\u022d\u022f\5\u0083"+
                    "B\2\u022e\u0230\5\u0085C\2\u022f\u022e\3\2\2\2\u022f\u0230\3\2\2\2\u0230"+
                    "\u0231\3\2\2\2\u0231\u0232\7D\2\2\u0232\u0233\7F\2\2\u0233\u0234\3\2\2"+
                    "\2\u0234\u0235\6?\6\2\u0235\u0237\3\2\2\2\u0236\u0223\3\2\2\2\u0236\u022d"+
                    "\3\2\2\2\u0237~\3\2\2\2\u0238\u023c\5\u0089E\2\u0239\u023c\5\u0087D\2"+
                    "\u023a\u023c\7a\2\2\u023b\u0238\3\2\2\2\u023b\u0239\3\2\2\2\u023b\u023a"+
                    "\3\2\2\2\u023c\u023d\3\2\2\2\u023d\u023b\3\2\2\2\u023d\u023e\3\2\2\2\u023e"+
                    "\u0080\3\2\2\2\u023f\u0245\7b\2\2\u0240\u0244\n\4\2\2\u0241\u0242\7b\2"+
                    "\2\u0242\u0244\7b\2\2\u0243\u0240\3\2\2\2\u0243\u0241\3\2\2\2\u0244\u0247"+
                    "\3\2\2\2\u0245\u0243\3\2\2\2\u0245\u0246\3\2\2\2\u0246\u0248\3\2\2\2\u0247"+
                    "\u0245\3\2\2\2\u0248\u0249\7b\2\2\u0249\u0082\3\2\2\2\u024a\u024c\5\u0087"+
                    "D\2\u024b\u024a\3\2\2\2\u024c\u024d\3\2\2\2\u024d\u024b\3\2\2\2\u024d"+
                    "\u024e\3\2\2\2\u024e\u024f\3\2\2\2\u024f\u0253\7\60\2\2\u0250\u0252\5"+
                    "\u0087D\2\u0251\u0250\3\2\2\2\u0252\u0255\3\2\2\2\u0253\u0251\3\2\2\2"+
                    "\u0253\u0254\3\2\2\2\u0254\u025d\3\2\2\2\u0255\u0253\3\2\2\2\u0256\u0258"+
                    "\7\60\2\2\u0257\u0259\5\u0087D\2\u0258\u0257\3\2\2\2\u0259\u025a\3\2\2"+
                    "\2\u025a\u0258\3\2\2\2\u025a\u025b\3\2\2\2\u025b\u025d\3\2\2\2\u025c\u024b"+
                    "\3\2\2\2\u025c\u0256\3\2\2\2\u025d\u0084\3\2\2\2\u025e\u0260\7G\2\2\u025f"+
                    "\u0261\t\5\2\2\u0260\u025f\3\2\2\2\u0260\u0261\3\2\2\2\u0261\u0263\3\2"+
                    "\2\2\u0262\u0264\5\u0087D\2\u0263\u0262\3\2\2\2\u0264\u0265\3\2\2\2\u0265"+
                    "\u0263\3\2\2\2\u0265\u0266\3\2\2\2\u0266\u0086\3\2\2\2\u0267\u0268\t\6"+
                    "\2\2\u0268\u0088\3\2\2\2\u0269\u026a\t\7\2\2\u026a\u008a\3\2\2\2\u026b"+
                    "\u026c\7/\2\2\u026c\u026d\7/\2\2\u026d\u0273\3\2\2\2\u026e\u026f\7^\2"+
                    "\2\u026f\u0272\7\f\2\2\u0270\u0272\n\b\2\2\u0271\u026e\3\2\2\2\u0271\u0270"+
                    "\3\2\2\2\u0272\u0275\3\2\2\2\u0273\u0271\3\2\2\2\u0273\u0274\3\2\2\2\u0274"+
                    "\u0277\3\2\2\2\u0275\u0273\3\2\2\2\u0276\u0278\7\17\2\2\u0277\u0276\3"+
                    "\2\2\2\u0277\u0278\3\2\2\2\u0278\u027a\3\2\2\2\u0279\u027b\7\f\2\2\u027a"+
                    "\u0279\3\2\2\2\u027a\u027b\3\2\2\2\u027b\u027c\3\2\2\2\u027c\u027d\bF"+
                    "\2\2\u027d\u008c\3\2\2\2\u027e\u027f\7\61\2\2\u027f\u0280\7,\2\2\u0280"+
                    "\u0281\3\2\2\2\u0281\u0286\6G\7\2\u0282\u0285\5\u008dG\2\u0283\u0285\13"+
                    "\2\2\2\u0284\u0282\3\2\2\2\u0284\u0283\3\2\2\2\u0285\u0288\3\2\2\2\u0286"+
                    "\u0287\3\2\2\2\u0286\u0284\3\2\2\2\u0287\u0289\3\2\2\2\u0288\u0286\3\2"+
                    "\2\2\u0289\u028a\7,\2\2\u028a\u028b\7\61\2\2\u028b\u028c\3\2\2\2\u028c"+
                    "\u028d\bG\2\2\u028d\u008e\3\2\2\2\u028e\u0290\t\t\2\2\u028f\u028e\3\2"+
                    "\2\2\u0290\u0291\3\2\2\2\u0291\u028f\3\2\2\2\u0291\u0292\3\2\2\2\u0292"+
                    "\u0293\3\2\2\2\u0293\u0294\bH\2\2\u0294\u0090\3\2\2\2\u0295\u0296\13\2"+
                    "\2\2\u0296\u0092\3\2\2\2+\2\u01c0\u01c2\u01ca\u01cc\u01d0\u01d5\u01dc"+
                    "\u01e3\u01ea\u01ef\u01f7\u01ff\u0202\u0208\u020d\u0212\u0215\u021b\u0220"+
                    "\u0225\u0228\u022f\u0236\u023b\u023d\u0243\u0245\u024d\u0253\u025a\u025c"+
                    "\u0260\u0265\u0271\u0273\u0277\u027a\u0284\u0286\u0291\3\2\3\2";
    public static final ATN _ATN =
            new ATNDeserializer().deserialize(_serializedATN.toCharArray());
    static {
        _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
        for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
            _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
        }
    }
}
