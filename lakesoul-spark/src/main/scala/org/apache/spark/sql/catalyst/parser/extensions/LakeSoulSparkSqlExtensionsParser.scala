// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0
package org.apache.spark.sql.catalyst.parser.extensions

import org.apache.parquet.util.DynConstructors
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.ExplainCommand
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.internal.VariableSubstitution
import org.antlr.v4.runtime._
import org.antlr.v4.runtime.atn.PredictionMode
import org.antlr.v4.runtime.misc.{Interval, ParseCancellationException}
import org.antlr.v4.runtime.tree.TerminalNodeImpl
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.parser.extensions.LakeSoulSqlExtensionsParser.{NonReservedContext, QuotedIdentifierContext}

import java.util.Locale

class LakeSoulSparkSqlExtensionsParser(delegate: ParserInterface) extends ParserInterface {
  private lazy val substitutor = new VariableSubstitution
  private lazy val astBuilder = new LakeSoulSqlExtensionsAstBuilder(delegate)

  override def parsePlan(sqlText: String): LogicalPlan = {
    val sqlTextAfter = substitutor.substitute(sqlText)
    if(isLakeSoulCommand(sqlTextAfter)){
      parse(sqlTextAfter) { parser => astBuilder.visit(parser.singleStatement()) }.asInstanceOf[LogicalPlan]

    }else{
      delegate.parsePlan(sqlText)
    }
  }
  protected def parse[T](command: String)(toResult: LakeSoulSqlExtensionsParser => T): T = {
    val lexer = new LakeSoulSqlExtensionsLexer(new UpperCaseCharStream(CharStreams.fromString(command)))
    lexer.removeErrorListeners()
    lexer.addErrorListener(LakeSoulParseErrorListener)
    val tokenStream = new CommonTokenStream(lexer)
    val parser = new LakeSoulSqlExtensionsParser(tokenStream)
    parser.addParseListener(LakeSoulSqlExtensionsPostProcessor)
    parser.removeErrorListeners()
    parser.addErrorListener(LakeSoulParseErrorListener)
    try {
      try {
        // first, try parsing with potentially faster SLL mode
        parser.getInterpreter.setPredictionMode(PredictionMode.SLL)
        toResult(parser)
      }
      catch {
        case _: ParseCancellationException =>
          // if we fail, parse with LL mode
          tokenStream.seek(0) // rewind input stream
          parser.reset()

          // Try Again.
          parser.getInterpreter.setPredictionMode(PredictionMode.LL)
          toResult(parser)
      }
    }
    catch {
      case e: LakeSoulParseException if e.command.isDefined =>
        throw e
      case e: LakeSoulParseException =>
        throw e.withCommand(command)
      case e: AnalysisException =>
        val position = Origin(e.line, e.startPosition)
        throw new LakeSoulParseException(Option(command), e.message, position, position)
    }
  }

  /**
   * Parse a string to a DataType.
   */
  override def parseDataType(sqlText: String): DataType = {
    delegate.parseDataType(sqlText)
  }

  /**
   * Parse a string to a raw DataType without CHAR/VARCHAR replacement.
   */
  def parseRawDataType(sqlText: String): DataType = throw new UnsupportedOperationException()

  /**
   * Parse a string to an Expression.
   */
  override def parseExpression(sqlText: String): Expression = {
    delegate.parseExpression(sqlText)
  }

  /**
   * Parse a string to a TableIdentifier.
   */
  override def parseTableIdentifier(sqlText: String): TableIdentifier = {
    delegate.parseTableIdentifier(sqlText)
  }

  /**
   * Parse a string to a FunctionIdentifier.
   */
  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier = {
    delegate.parseFunctionIdentifier(sqlText)
  }

  /**
   * Parse a string to a multi-part identifier.
   */
  override def parseMultipartIdentifier(sqlText: String): Seq[String] = {
    delegate.parseMultipartIdentifier(sqlText)
  }

  /**
   * Creates StructType for a given SQL string, which is a comma separated list of field
   * definitions which will preserve the correct Hive metadata.
   */
  override def parseTableSchema(sqlText: String): StructType = {
    delegate.parseTableSchema(sqlText)
  }


  override def parseQuery(sqlText: String): LogicalPlan = {
    delegate.parseQuery(sqlText)
  }


  private def isLakeSoulCommand(sqlText: String): Boolean = {
    val normalized = sqlText.toLowerCase(Locale.ROOT).trim()
      // Strip simple SQL comments that terminate a line, e.g. comments starting with `--` .
      .replaceAll("--.*?\\n", " ")
      // Strip newlines.
      .replaceAll("\\s+", " ")
      // Strip comments of the form  /* ... */. This must come after stripping newlines so that
      // comments that span multiple lines are caught.
      .replaceAll("/\\*.*?\\*/", " ")
      .trim()
    normalized.startsWith("call")
  }
}
case object LakeSoulSqlExtensionsPostProcessor extends LakeSoulSqlExtensionsListener {

  /** Remove the back ticks from an Identifier. */
  override def exitQuotedIdentifier(ctx: QuotedIdentifierContext): Unit = {
    replaceTokenByIdentifier(ctx, 1) { token =>
      // Remove the double back ticks in the string.
      token.setText(token.getText.replace("``", "`"))
      token
    }
  }

  /** Treat non-reserved keywords as Identifiers. */
  override def exitNonReserved(ctx: NonReservedContext): Unit = {
    replaceTokenByIdentifier(ctx, 0)(identity)
  }

  private def replaceTokenByIdentifier(
                                        ctx: ParserRuleContext,
                                        stripMargins: Int)(
                                        f: CommonToken => CommonToken = identity): Unit = {
    val parent = ctx.getParent
    parent.removeLastChild()
    val token = ctx.getChild(0).getPayload.asInstanceOf[Token]
    val newToken = new CommonToken(
      new org.antlr.v4.runtime.misc.Pair(token.getTokenSource, token.getInputStream),
      LakeSoulSqlExtensionsParser.IDENTIFIER,
      token.getChannel,
      token.getStartIndex + stripMargins,
      token.getStopIndex - stripMargins)
    parent.addChild(new TerminalNodeImpl(f(newToken)))
  }
}

class UpperCaseCharStream(wrapped: CodePointCharStream) extends CharStream {
  override def consume(): Unit = wrapped.consume
  override def getSourceName(): String = wrapped.getSourceName
  override def index(): Int = wrapped.index
  override def mark(): Int = wrapped.mark
  override def release(marker: Int): Unit = wrapped.release(marker)
  override def seek(where: Int): Unit = wrapped.seek(where)
  override def size(): Int = wrapped.size

  override def getText(interval: Interval): String = wrapped.getText(interval)

  // scalastyle:off
  override def LA(i: Int): Int = {
    val la = wrapped.LA(i)
    if (la == 0 || la == IntStream.EOF) la
    else Character.toUpperCase(la)
  }
  // scalastyle:on
}
class LakeSoulParseException(
                             val command: Option[String],
                             message: String,
                             val start: Origin,
                             val stop: Origin) extends AnalysisException(message, start.line, start.startPosition) {

  def this(message: String, ctx: ParserRuleContext) = {
    this(Option(LakeSoulParserUtils.command(ctx)),
      message,
      LakeSoulParserUtils.position(ctx.getStart),
      LakeSoulParserUtils.position(ctx.getStop))
  }

  override def getMessage: String = {
    val builder = new StringBuilder
    builder ++= "\n" ++= message
    start match {
      case Origin(Some(l), Some(p),Some(t),Some(m),Some(n),Some(s),Some(o)) =>
        builder ++= s"(line $l, pos $p)\n"
        command.foreach { cmd =>
          val (above, below) = cmd.split("\n").splitAt(l)
          builder ++= "\n== SQL ==\n"
          above.foreach(builder ++= _ += '\n')
          builder ++= (0 until p).map(_ => "-").mkString("") ++= "^^^\n"
          below.foreach(builder ++= _ += '\n')
        }
      case _ =>
        command.foreach { cmd =>
          builder ++= "\n== SQL ==\n" ++= cmd
        }
    }
    builder.toString
  }

  def withCommand(cmd: String): LakeSoulParseException = {
    new LakeSoulParseException(Option(cmd), message, start, stop)
  }
}
case object LakeSoulParseErrorListener extends BaseErrorListener {
  override def syntaxError(
                            recognizer: Recognizer[_, _],
                            offendingSymbol: scala.Any,
                            line: Int,
                            charPositionInLine: Int,
                            msg: String,
                            e: RecognitionException): Unit = {
    val (start, stop) = offendingSymbol match {
      case token: CommonToken =>
        val start = Origin(Some(line), Some(token.getCharPositionInLine))
        val length = token.getStopIndex - token.getStartIndex + 1
        val stop = Origin(Some(line), Some(token.getCharPositionInLine + length))
        (start, stop)
      case _ =>
        val start = Origin(Some(line), Some(charPositionInLine))
        (start, start)
    }
    throw new LakeSoulParseException(None, msg, start, stop)
  }
}