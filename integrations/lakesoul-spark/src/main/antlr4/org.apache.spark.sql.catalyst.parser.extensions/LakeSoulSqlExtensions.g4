// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

grammar LakeSoulSqlExtensions;

@lexer::members {
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
}

singleStatement
    : statement EOF
    ;

statement
    : CALL multipartIdentifier '(' (callArgument (',' callArgument)*)? ')'                  #call
    ;

callArgument
    : expression                    #positionalArgument
    | identifier '=>' expression    #namedArgument
    ;

expression
    : constant
    | stringMap
    | stringArray
    ;

constant
    : number                          #numericLiteral
    | booleanValue                    #booleanLiteral
    | STRING+                         #stringLiteral
    | identifier STRING               #typeConstructor
    ;

stringMap
    : MAP '(' constant (',' constant)* ')'
    ;

stringArray
    : ARRAY '(' constant (',' constant)* ')'
    ;

booleanValue
    : TRUE | FALSE
    ;

number
    : MINUS? EXPONENT_VALUE           #exponentLiteral
    | MINUS? DECIMAL_VALUE            #decimalLiteral
    | MINUS? INTEGER_VALUE            #integerLiteral
    | MINUS? BIGINT_LITERAL           #bigIntLiteral
    | MINUS? SMALLINT_LITERAL         #smallIntLiteral
    | MINUS? TINYINT_LITERAL          #tinyIntLiteral
    | MINUS? DOUBLE_LITERAL           #doubleLiteral
    | MINUS? FLOAT_LITERAL            #floatLiteral
    | MINUS? BIGDECIMAL_LITERAL       #bigDecimalLiteral
    ;

multipartIdentifier
    : parts+=identifier ('.' parts+=identifier)*
    ;

identifier
    : IDENTIFIER              #unquotedIdentifier
    | quotedIdentifier        #quotedIdentifierAlternative
    | nonReserved             #unquotedIdentifier
    ;

quotedIdentifier
    : BACKQUOTED_IDENTIFIER
    ;

fieldList
    : fields+=multipartIdentifier (',' fields+=multipartIdentifier)*
    ;

nonReserved
    : ADD | ALTER | AS | ASC | BRANCH | BY | CALL | CREATE | DAYS | DESC | DROP | EXISTS | FIELD | FIRST | HOURS | IF | LAST | NOT | NULLS | OF | OR | ORDERED | PARTITION | TABLE | WRITE
    | DISTRIBUTED | LOCALLY | MINUTES | MONTHS | UNORDERED | REPLACE | RETAIN | RETENTION | VERSION | WITH | IDENTIFIER_KW | FIELDS | SET | SNAPSHOT | SNAPSHOTS
    | TRUE | FALSE
    | MAP
    ;

ADD: 'ADD';
ALTER: 'ALTER';
AS: 'AS';
ASC: 'ASC';
BRANCH: 'BRANCH';
BY: 'BY';
CALL: 'CALL';
CREATE: 'CREATE';
DAYS: 'DAYS';
DESC: 'DESC';
DISTRIBUTED: 'DISTRIBUTED';
DROP: 'DROP';
EXISTS: 'EXISTS';
FIELD: 'FIELD';
FIELDS: 'FIELDS';
FIRST: 'FIRST';
HOURS: 'HOURS';
IF : 'IF';
LAST: 'LAST';
LOCALLY: 'LOCALLY';
MINUTES: 'MINUTES';
MONTHS: 'MONTHS';
NOT: 'NOT';
NULLS: 'NULLS';
OF: 'OF';
OR: 'OR';
ORDERED: 'ORDERED';
PARTITION: 'PARTITION';
REPLACE: 'REPLACE';
RETAIN: 'RETAIN';
RETENTION: 'RETENTION';
IDENTIFIER_KW: 'IDENTIFIER';
SET: 'SET';
SNAPSHOT: 'SNAPSHOT';
SNAPSHOTS: 'SNAPSHOTS';
TABLE: 'TABLE';
UNORDERED: 'UNORDERED';
VERSION: 'VERSION';
WITH: 'WITH';
WRITE: 'WRITE';

TRUE: 'TRUE';
FALSE: 'FALSE';

MAP: 'MAP';
ARRAY: 'ARRAY';

PLUS: '+';
MINUS: '-';

STRING
    : '\'' ( ~('\''|'\\') | ('\\' .) )* '\''
    | '"' ( ~('"'|'\\') | ('\\' .) )* '"'
    ;

BIGINT_LITERAL
    : DIGIT+ 'L'
    ;

SMALLINT_LITERAL
    : DIGIT+ 'S'
    ;

TINYINT_LITERAL
    : DIGIT+ 'Y'
    ;

INTEGER_VALUE
    : DIGIT+
    ;

EXPONENT_VALUE
    : DIGIT+ EXPONENT
    | DECIMAL_DIGITS EXPONENT {isValidDecimal()}?
    ;

DECIMAL_VALUE
    : DECIMAL_DIGITS {isValidDecimal()}?
    ;

FLOAT_LITERAL
    : DIGIT+ EXPONENT? 'F'
    | DECIMAL_DIGITS EXPONENT? 'F' {isValidDecimal()}?
    ;

DOUBLE_LITERAL
    : DIGIT+ EXPONENT? 'D'
    | DECIMAL_DIGITS EXPONENT? 'D' {isValidDecimal()}?
    ;

BIGDECIMAL_LITERAL
    : DIGIT+ EXPONENT? 'BD'
    | DECIMAL_DIGITS EXPONENT? 'BD' {isValidDecimal()}?
    ;

IDENTIFIER
    : (LETTER | DIGIT | '_')+
    ;

BACKQUOTED_IDENTIFIER
    : '`' ( ~'`' | '``' )* '`'
    ;

fragment DECIMAL_DIGITS
    : DIGIT+ '.' DIGIT*
    | '.' DIGIT+
    ;

fragment EXPONENT
    : 'E' [+-]? DIGIT+
    ;

fragment DIGIT
    : [0-9]
    ;

fragment LETTER
    : [A-Z]
    ;

SIMPLE_COMMENT
    : '--' ('\\\n' | ~[\r\n])* '\r'? '\n'? -> channel(HIDDEN)
    ;

BRACKETED_COMMENT
    : '/*' {!isHint()}? (BRACKETED_COMMENT|.)*? '*/' -> channel(HIDDEN)
    ;

WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;

// Catch-all for anything we can't recognize.
// We use this to be able to ignore and recover all the text
// when splitting statements with DelimiterLexer
UNRECOGNIZED
    : .
    ;
