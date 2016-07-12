package com.actio

/**
 * Created by mauri on 28/06/2016.
 */

import scala.util.parsing.combinator._

object TemplateParser extends RegexParsers {

  def functionParams = "(" ~ "[a-zA-Z]+".r ~ rep("," ~ expression) ~ ")"

  def mix = "[^@]*".r ~ "@{" ~ expression ~ "}" ~ template

  def literal = "[^@]*".r

  def dataSetGet: Parser[List[FindCriteria]] = rep1("." ~ "[a-zA-Z0-9_\\*]+".r) ^^ { list =>
    list.map({
      case "." ~ prop =>
        if (prop.headOption.exists(_.isDigit)) {
          FindOrd(prop.toInt)
        }
        else if (prop == "*") {
          FindAll
        }
        else {
          FindLabel(prop)
        }
    })
  }

  def constInt: Parser[Constant[Int]] = "[0-9]+".r ^^ { str => Constant(str.toInt) }

  def constString: Parser[Constant[String]] = "'" ~ "[^']+".r ~ "'" ^^ { case "'" ~ text ~ "'" => Constant(text) }

  def variable: Parser[Variable] = "[a-zA-Z0-9]+".r ^^ { str => Variable(str) }

  def array: Parser[ExprArray] = """\[""".r ~ this. repsep(expression, ",") ~ """\]""".r ^^ {
    case "[" ~ list ~ "]" => ExprArray(list)
  }

  def template: Parser[Template] = (mix | literal) ^^
    {
      case (text: String) ~ "@{" ~ (expr: Expression) ~ "}" ~ (temp: Template) => Mix(Literal(text), expr, temp)
      case (text: String) => Literal(text)
    }

  def function: Parser[Function] = functionParams ^^ {
    case "(" ~ name ~ list ~ ")" => Function(name, list.map({
      case "," ~ expr => expr
    }))
  }

  def arrayWithGet: Parser[Expression] = (array ~ dataSetGet) ^^ {
    case expr ~ dataSetGet => PropsGet(expr, dataSetGet)
  }

  def variableWithGet: Parser[Expression] = (variable ~ dataSetGet) ^^ {
    case expr ~ dataSetGet => PropsGet(expr, dataSetGet)
  }

  def functionWithGet: Parser[Expression] = (function ~ dataSetGet) ^^ {
    case expr ~ dataSetGet => PropsGet(expr, dataSetGet)
  }

  def expression: Parser[Expression] = constInt | constString | variableWithGet | variable | functionWithGet | function | arrayWithGet | array

  def apply(code: String): Template = parseAll(template, code) match {
    case Success(s, _) => s
    case Failure(f, _) => Literal(code)
    case Error(e, _) => Literal(code)
  }

}