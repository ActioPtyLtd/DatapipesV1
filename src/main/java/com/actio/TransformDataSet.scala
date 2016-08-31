package com.actio

import scala.meta.Term.Arg.Repeated
import scala.meta._

/**
 * Created by mauri on 23/07/2016.
 */
object MetaTerm extends App {

  val ds = DataRecord("", List(DataString("key", "abc"), DataDate("key2", new java.util.Date(2000 - 1900, 1, 1))))
  val text = "ds => ds.key + \"def\""

  val term = text.parse[Term]

  println(term.get.structure)

  val res = eval(ds, term.get)

  println(res)

  def eval(ds: DataSet, text: String): DataSet = eval(ds, text.parse[Term].get)

  def evalTemplate(ds: DataSet, text: String): DataSet = eval(ds, interpolate(text))

  def interpolate(str: String): String = "s\"\"\"" + str + "\"\"\""

  def eval(ds: DataSet, t: Term): DataSet = t match {

    // evaluate lambda expression
    case Term.Function(Seq(Term.Param(_, Term.Name(name), _, _)), body) => eval(body, Map(name -> ds))

    // evaluate expression, add to scope top level attributes of the DataSet
    case _ => eval(t, ds.elems.map(e => (e.label -> e)).toList.toMap)
  }

  def eval(t: AnyRef, scope: Map[String, AnyRef]): DataSet = t match {

    // construct literal string
    case Lit(str: String) => DataString(str)

    // construct literal numeric
    case Lit(int: Int) => DataNumeric(int)

    // get variable in scope
    case Term.Name(name) => scope(name) match { case ds: DataSet => ds case term => eval(term, scope) }

    // evaluate conditional block
    case Term.If(cond, thenp, elsep) =>
      if (eval(cond, scope) match {
        case DataBoolean(_, bool) => bool
        case _ => false
      }) {
        eval(thenp, scope)
      } else {
        eval(elsep, scope)
      }

    // evaluate templates
    case Term.Interpolate(_, strings, terms) => DataString(
      (strings zip terms)
        .map(p =>
          p._1.toString() + eval(p._2, scope).stringOption.getOrElse("")).mkString +
        strings.last.toString)

    // support dot notation to access DataSets
    case select: Term.Select => evalSelect(select, scope)

    // call functions
    case apply: Term.Apply => evalApply(apply, scope)

    // evaluate infix operations
    case infix: Term.ApplyInfix => evalApplyInfix(infix, scope)
  }

  def evalSelect(t: Term.Select, scope: Map[String, AnyRef]): DataSet = t match {

    // Placeholder '_' will evaluate to accessing a DataSet record by empty label (maybe obsolete)
    case Term.Select(q, Term.Placeholder()) => eval(q, scope)("")

    // Astrix should be treated as iteration
    case Term.Select(Term.Select(q, Term.Name("*")), Term.Name(n)) => DataArray("", eval(q, scope).elems.map(i => i(n)).toList)

    // Look for the DataSet with label
    case Term.Select(q, Term.Name(n)) => eval(q, scope)(n)
  }

  def evalApply(t: Term.Apply, scope: Map[String, AnyRef]): DataSet = t match {

    // dynamically call function, evaluating parameters before execution
    case Term.Apply(Term.Name(fName), args) if !scope.contains(fName) => UtilityFunctions.execute(fName, args.map(eval(_, scope)).toList)

    // get DataSet by ordinal
    case Term.Apply(q, Seq(Lit(num: Int))) => eval(q, scope)(num)

    // get DataSet by name
    case Term.Apply(q, Seq(Lit(str: String))) => eval(q, scope)(str)

    // construct a fixed size DataArray
    case Term.Apply(Term.Name("DataArray"), args) => DataArray(args.map(eval(_, scope)).toList)

    // iterate through DataSet and include elements matching condition
    case Term.Apply(
      Term.Select(s, Term.Name("filter")),
      Seq(Term.Function(Seq(Term.Param(Nil, Term.Name(tn), None, None)), rem))) =>
      DataArray(eval(s, scope).elems.filter(f => eval(rem, scope + (tn -> f)).asInstanceOf[DataBoolean].bool).toList)

    // iterate through DataSet and exclude elements matching condition
    case Term.Apply(
      Term.Select(s, Term.Name("filterNot")),
      Seq(Term.Function(Seq(Term.Param(Nil, Term.Name(tn), None, None)), rem))) =>
      DataArray(eval(s, scope).elems.filterNot(f => eval(rem, scope + (tn -> f)).asInstanceOf[DataBoolean].bool).toList)
  }

  def evalApplyInfix(t: Term.ApplyInfix, scope: Map[String, AnyRef]): DataSet = t match {

    // currently can compare dates only
    case Term.ApplyInfix(l, Term.Name(">="), Nil, Seq(r)) => eval(l, scope) match {
      case d: DataDate => DataBoolean(d.date.compareTo(eval(r, scope).asInstanceOf[DataDate].date) >= 0)
      case n: DataNumeric => DataBoolean(n.num >= eval(r, scope).asInstanceOf[DataNumeric].num)
      case _ => DataBoolean(false)
    }

    // currently can compare dates only
    case Term.ApplyInfix(l, Term.Name("<"), Nil, Seq(r)) => eval(l, scope) match {
      case d: DataDate => DataBoolean(d.date.compareTo(eval(r, scope).asInstanceOf[DataDate].date) < 0)
      case n: DataNumeric => DataBoolean(n.num < eval(r, scope).asInstanceOf[DataNumeric].num)
      case _ => DataBoolean(false)
    }

    // currently, addition appends strings only
    case Term.ApplyInfix(l, Term.Name("+"), Nil, Seq(r)) => DataString(eval(l, scope).stringOption.getOrElse("") + eval(r, scope).stringOption.getOrElse(""))

    // currently, equality will do a string comparison
    case Term.ApplyInfix(l, Term.Name("=="), Nil, Seq(r)) => {
      val ls = eval(l, scope).stringOption
      val rs = eval(r, scope).stringOption

      DataBoolean(ls.isDefined && rs.isDefined && ls.get == rs.get)
    }
  }

}
