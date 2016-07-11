package com.actio

import scala.util.{ Failure, Success, Try }

/**
 * Created by mauri on 19/06/2016.
 */

abstract class Expression
case class Function(name: String, param: List[Expression]) extends Expression
case class Variable(name: String) extends Expression
case class Constant[C <: Any](value: C) extends Expression
case class PropsGet(expr: Expression, props: List[FindCriteria]) extends Expression
case class ForEach(expr: Expression, lambdaFunction: LambdaFunction) extends Expression

case class LambdaFunction(varName: String, expr: Expression) // (x: Data) => Data

abstract class Template extends Expression
case class Literal(text: String) extends Template
case class Mix(left: Literal, expression: Expression, right: Template) extends Template

object TemplateEngine {

  import scala.collection.JavaConversions._

  def apply(expr: Expression, ds: DataSet): DataSet = eval(expr, Map("d" -> ds))

  def eval(expr: Expression, scope: Map[String, DataSet]): DataSet = expr match {
    case Constant(i: String) => DataString(i)
    case Constant(i: Int) => DataNumeric(i)
    case Variable(name) => scope(name)
    case PropsGet(leftExpr, props) =>
      if (props.contains(FindAll)) {
        DataArray("", eval(leftExpr, scope).find(props).toList)
      } else {
        eval(leftExpr, scope).find(props).toList.headOption.getOrElse(Nothin())
      }
    case Function(name, params) =>
      UtilityFunctions.execute(name, params.map(p => eval(p, scope).asInstanceOf[Any])) // reflection invoke
    case Literal(text) => DataString(text)
    case Mix(left, middle, right) =>
      DataString(left.text +
        eval(middle, scope).stringOption.getOrElse("") +
        eval(right, scope).stringOption.getOrElse(""))
    case ForEach(list, lambda) =>
      DataArray(eval(list, scope).elems.map(e => callLambdaFunction(lambda, e, scope)).toList)
  }

  def callLambdaFunction(lambda: LambdaFunction, data: DataSet, scope: Map[String, DataSet]): DataSet = eval(lambda.expr, scope + (lambda.varName -> data))
}