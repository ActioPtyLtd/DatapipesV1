package com.actio

import scala.meta.Term.Arg.Repeated
import scala.meta._

/**
 * Created by mauri on 23/07/2016.
 */
object TestMeta extends App {

  val ds = DataRecord("", List(DataString("key", "abc"), DataDate("key2", new java.util.Date(2000-1900,1,1))))
  val text = "ds => ds.key + \"def\""

  val term = text.parse[Term]

  println(term.get.structure)

  val res = eval(ds, term.get)

  println(res)

  def eval(ds: DataSet, text: String): DataSet = eval(ds, text.parse[Term].get)

  def evalTemplate(ds: DataSet, text: String): DataSet = eval(ds, interpolate(text))

  def interpolate(str: String): String = "s\"\"\"" + str + "\"\"\""

  def eval(ds: DataSet, t: Term): DataSet = t match {
    case Term.Function(Seq(Term.Param(_, Term.Name(name), _, _)), body) => eval(body, Map(name -> ds))
    case _ => eval(t, ds.elems.map(e => (e.label -> e)).toList.toMap)
  }

  def eval(t: AnyRef, scope: Map[String, AnyRef]): DataSet = t match {
    case Lit(str: String) => DataString(str)
    case Lit(int: Int) => DataNumeric(int)
    case Term.Name(name) => scope(name) match { case ds: DataSet => ds case term => eval(term, scope) }
    case Term.Select(q, Term.Placeholder()) => eval(q, scope)("")
    case Term.Select(Term.Select(q, Term.Name("*")), Term.Name(n)) => DataArray("", eval(q, scope).elems.map(i => i(n)).toList)
    case Term.Select(q, Term.Name(n)) => eval(q, scope)(n)
    case Term.Apply(Term.Name(fName), args) if !scope.contains(fName) => UtilityFunctions.execute(fName, args.map(eval(_, scope)).toList)
    case Term.Apply(q, Seq(Lit(num: Int))) => eval(q, scope)(num)
    case Term.Apply(q, Seq(Lit(str: String))) => eval(q, scope)(str)
    case Term.Apply(Term.Name("DataArray"), args) => DataArray(args.map(eval(_, scope)).toList)
    case Term.Apply(
      Term.Select(s, Term.Name("filter")),
      Seq(Term.Function(Seq(Term.Param(Nil, Term.Name(tn), None, None)), rem))) =>
      DataArray(eval(s, scope).elems.filter(f => eval(rem, scope + (tn -> f)).asInstanceOf[DataBoolean].bool).toList)
    case Term.Apply(
    Term.Select(s, Term.Name("filterNot")),
    Seq(Term.Function(Seq(Term.Param(Nil, Term.Name(tn), None, None)), rem))) =>
      DataArray(eval(s, scope).elems.filterNot(f => eval(rem, scope + (tn -> f)).asInstanceOf[DataBoolean].bool).toList)
    case Term.If(cond, thenp, elsep) =>
      if (eval(cond, scope) match {
        case DataBoolean(_, bool) => bool
        case _ => false
      }) eval(thenp, scope) else eval(elsep, scope)
    case Term.Interpolate(_, strings, terms) => DataString((strings zip terms).map(p => p._1.toString() + eval(p._2, scope).stringOption.getOrElse("")).mkString + strings.last.toString)
    case Term.ApplyInfix(l, Term.Name(">="), Nil, Seq(r)) => eval(l, scope) match {
      case d: DataDate => DataBoolean(d.date.compareTo(eval(r, scope).asInstanceOf[DataDate].date) >= 0)
      case _ => DataBoolean(false)}
    case Term.ApplyInfix(l, Term.Name("<"), Nil, Seq(r)) => eval(l, scope) match {
      case d: DataDate => DataBoolean(d.date.compareTo(eval(r, scope).asInstanceOf[DataDate].date) < 0)
      case _ => DataBoolean(false)}
    case Term.ApplyInfix(l, Term.Name("+"), Nil, Seq(r)) => DataString(eval(l, scope).stringOption.getOrElse("") + eval(r, scope).stringOption.getOrElse(""))
  }
}
