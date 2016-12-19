package com.actio

import scala.meta.Term.Arg.Repeated
import scala.meta._

/**
 * Created by mauri on 23/07/2016.
 */
object MetaTerm {

  def eval(ds: DataSet, text: String): DataSet = eval(ds, text.parse[Term].get)

  def evalTemplate(ds: DataSet, text: String): DataSet = eval(ds, interpolate(text))

  def interpolate(str: String): String = "s\"\"\"" + str + "\"\"\""

  def eval(ds: DataSet, t: Term): DataSet = t match {

    // evaluate lambda expression
    case Term.Function(Seq(Term.Param(_, Term.Name(name), _, _)), body) =>
      eval(body, Map(name -> ds))

    // evaluate expression, add to scope top level attributes of the DataSet
    case _ => eval(t, ds.elems.map(e => e.label -> e).toList.toMap + ("this" -> ds))
  }

  def eval(t: AnyRef, scope: Map[String, AnyRef]): DataSet = t match {

    // construct literal string
    case Lit(str: String) => DataString(str)

    // construct literal numeric
    case Lit(int: Int) => DataNumeric(int)

    // tuple for dataset construction

    case Term.Tuple(Term.Name(label) +: tail) => DataRecord(label,
      tail.map(e => eval(e, scope)).toList)

    case Term.Tuple(s) => DataRecord(eval(s.head, scope).stringOption.getOrElse(""),
      s.tail.map(e => eval(e, scope)).toList)

    // for when you want to reference original top level dataset
    case Term.This(_) => scope("this")  match {
      case ds: DataSet => ds
      case term => eval(term, scope)
    }

    // get variable in scope
    case Term.Name(name) => scope(name) match {
      case ds: DataSet => ds
      case term => eval(term, scope)
    }

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
        .map(p => p._1.toString() + eval(p._2, scope).stringOption.getOrElse(""))
        .mkString + strings.last.toString)

    // support dot notation to access DataSets
    case select: Term.Select => evalSelect(select, scope)

    // call functions
    case apply: Term.Apply => evalApply(apply, scope)

    // evaluate infix operations
    case infix: Term.ApplyInfix => evalApplyInfix(infix, scope)

    // evaluate unary operations
    case unary: Term.ApplyUnary => evalApplyUnary(unary, scope)
  }

  def evalSelect(t: Term.Select, scope: Map[String, AnyRef]): DataSet = t match {

    // Placeholder '_' will evaluate to accessing a
    // DataSet record by empty label (maybe obsolete)
    case Term.Select(q, Term.Placeholder()) => eval(q, scope)("")

    // Astrix should be treated as iteration
    case Term.Select(Term.Select(q, Term.Name("*")), Term.Name(n)) =>
      DataArray(eval(q, scope).elems.map(i => i(n)).toList)

    // Look for the DataSet with label
    case Term.Select(q, Term.Name(n)) => eval(q, scope)(n)
  }

  def evalApply(t: Term.Apply, scope: Map[String, AnyRef]): DataSet = t match {

    // construct a fixed size DataArray
    case Term.Apply(Term.Name("DataArray"), args) =>
      DataArray(args.map(eval(_, scope)).toList)

    // dynamically call function, evaluating parameters before execution
    case Term.Apply(Term.Name(fName), args)
      if !scope.contains(fName) =>
        UtilityFunctions.execute(fName, args.map(eval(_, scope)).toList)

    // get DataSet by ordinal
    case Term.Apply(q, Seq(Lit(num: Int))) => eval(q, scope)(num)

    // get DataSet by name
    case Term.Apply(q, Seq(Lit(str: String))) => eval(q, scope)(str)

    // iterate through DataSet and include elements matching condition
    case Term.Apply(
      Term.Select(s, Term.Name("filter")),
      Seq(Term.Function(Seq(Term.Param(Nil, Term.Name(tn), None, None)), rem))) =>
        DataArray(
          eval(s, scope)
            .elems
            .filter(f => eval(rem, scope + (tn -> f)).asInstanceOf[DataBoolean].bool)
            .toList)

    // iterate through DataSet and exclude elements matching condition
    case Term.Apply(
      Term.Select(s, Term.Name("filterNot")),
      Seq(Term.Function(Seq(Term.Param(Nil, Term.Name(tn), None, None)), rem))) =>
        DataArray(
          eval(s, scope)
            .elems
            .filterNot(f => eval(rem, scope + (tn -> f)).asInstanceOf[DataBoolean].bool)
            .toList)

    // iterate through DataSet and apply function to each element
    case Term.Apply(
      Term.Select(s, Term.Name("map")),
      Seq(Term.Function(Seq(Term.Param(Nil, Term.Name(tn), None, None)), rem))) =>
        DataArray(
          eval(s, scope)
            .elems
            .map(f => eval(rem, scope + (tn -> f)))
            .toList)

    // iterate through DataSet and apply function to each element
    case Term.Apply(
      Term.Select(s, Term.Name("flatMap")),
      Seq(Term.Function(Seq(Term.Param(Nil, Term.Name(tn), None, None)), rem))) =>
        DataArray(
          eval(s, scope)
            .elems
            .flatMap(f => eval(rem, scope + (tn -> f)).elems)
            .toList)

    case Term.Apply(
      Term.Select(s, Term.Name("groupBy")),
      Seq(Term.Function(Seq(Term.Param(Nil, Term.Name(tn), None, None)), rem))) =>
        DataRecord(
          eval(s, scope)
            .elems
            .toList
            .groupBy(k => eval(rem, scope + (tn -> k)).stringOption.getOrElse(""))
            .map(p => DataArray(p._1, p._2))
            .toList)

    case Term.Apply(
      Term.Select(s, Term.Name("find")),
      Seq(Term.Function(Seq(Term.Param(Nil, Term.Name(tn), None, None)), rem))) =>
        eval(s, scope)
          .elems
          .toList
          .find(f =>
            eval(rem, scope + (tn -> f)) match {
              case DataBoolean(_, bool) => bool
              case _ => false})
          .getOrElse(Nothin())

    case Term.Apply(
      Term.Select(s, Term.Name("reduceLeft")),
      Seq(Term.Function(Seq(Term.Param(Nil, Term.Name(ta), None, None), Term.Param(Nil, Term.Name(tb), None, None)), rem))) =>
        eval(s, scope)
          .elems
          .toList
          .reduceLeft((a,b) => eval(rem, scope + (ta -> a) + (tb -> b)))



  }

  def evalApplyUnary(t: Term.ApplyUnary, scope: Map[String, AnyRef]): DataSet = t match {
    case Term.ApplyUnary(Term.Name("-"), r) => DataNumeric(-eval(r, scope).asInstanceOf[DataNumeric].num)
  }

  def evalApplyInfix(t: Term.ApplyInfix, scope: Map[String, AnyRef]): DataSet = t match {

    // >=
    case Term.ApplyInfix(l, Term.Name(">="), Nil, Seq(r)) =>
      eval(l, scope) match {
        case d: DataDate =>
          DataBoolean(d.date.compareTo(eval(r, scope).asInstanceOf[DataDate].date) >= 0)
        case n: DataNumeric =>
          DataBoolean(n.num >= eval(r, scope).asInstanceOf[DataNumeric].num)
        case _ =>
          DataBoolean(false)
    }

    // >
    case Term.ApplyInfix(l, Term.Name(">"), Nil, Seq(r)) =>
      eval(l, scope) match {
        case d: DataDate =>
          DataBoolean(d.date.compareTo(eval(r, scope).asInstanceOf[DataDate].date) > 0)
        case n: DataNumeric =>
          DataBoolean(n.num > eval(r, scope).asInstanceOf[DataNumeric].num)
        case _ =>
          DataBoolean(false)
    }

    // <
    case Term.ApplyInfix(l, Term.Name("<"), Nil, Seq(r)) => eval(l, scope) match {
      case d: DataDate =>
        DataBoolean(d.date.compareTo(eval(r, scope).asInstanceOf[DataDate].date) < 0)
      case n: DataNumeric =>
        DataBoolean(n.num < eval(r, scope).asInstanceOf[DataNumeric].num)
      case _ =>
        DataBoolean(false)
    }

    // <=
    case Term.ApplyInfix(l, Term.Name("<="), Nil, Seq(r)) => eval(l, scope) match {
      case d: DataDate =>
        DataBoolean(d.date.compareTo(eval(r, scope).asInstanceOf[DataDate].date) <= 0)
      case n: DataNumeric =>
        DataBoolean(n.num <= eval(r, scope).asInstanceOf[DataNumeric].num)
      case _ =>
        DataBoolean(false)
    }

    // string concat and numeric addition
    case Term.ApplyInfix(l, Term.Name("+"), Nil, Seq(r)) => (eval(l, scope),eval(r, scope)) match {
      case (left: DataNumeric, right: DataNumeric) => DataNumeric(left.num + right.num)
      case (left,right) => DataString(left.stringOption.getOrElse("") +
                                  right.stringOption.getOrElse(""))
    }

    // subtract numeric
    case Term.ApplyInfix(l, Term.Name("-"), Nil, Seq(r)) => (eval(l, scope),eval(r, scope)) match {
      case (left: DataNumeric, right: DataNumeric) => DataNumeric(
        left.num -
        right.num)
      }

    // multiply numeric
    case Term.ApplyInfix(l, Term.Name("*"), Nil, Seq(r)) => (eval(l, scope),eval(r, scope)) match {
      case (left: DataNumeric, right: DataNumeric) => DataNumeric(
        left.num *
        right.num)
      }

    // divide numeric
    case Term.ApplyInfix(l, Term.Name("/"), Nil, Seq(r)) => (eval(l, scope),eval(r, scope)) match {
      case (left: DataNumeric, right: DataNumeric) => DataNumeric(
        left.num /
        right.num)
      }

    // currently, equality will do a string comparison
    case Term.ApplyInfix(l, Term.Name("=="), Nil, Seq(r)) => {
      val ls = eval(l, scope).stringOption
      val rs = eval(r, scope).stringOption

      DataBoolean(ls.isDefined && rs.isDefined && ls.get == rs.get)
    }
    case Term.ApplyInfix(l, Term.Name("!="), Nil, Seq(r)) => {
      val ls = eval(l, scope).stringOption
      val rs = eval(r, scope).stringOption

      DataBoolean(ls.isDefined && rs.isDefined && ls.get != rs.get)
    }
    // AND logic
    case Term.ApplyInfix(l, Term.Name("&&"), Nil, Seq(r)) => {
      val ls = eval(l, scope).asInstanceOf[DataBoolean]

      if(ls.bool) {
        eval(r, scope)
      } else {
        DataBoolean(false)
      }
    }

    // OR logic
    case Term.ApplyInfix(l, Term.Name("||"), Nil, Seq(r)) => {
      val ls = eval(l, scope).asInstanceOf[DataBoolean]

      if(ls.bool) {
        DataBoolean(true)
      } else {
        eval(r, scope)
      }
    }

    case Term.ApplyInfix(l, Term.Name("mergeLeft"), Nil, Seq(r)) => {
      DataSetOperations.mergeLeft(eval(l, scope), eval(r,scope))
    }

    case Term.ApplyInfix(Term.Name(key), Term.Name("->"), Nil, Seq(r)) => {
      val value = eval(r, scope)
      value match {
        case DataString(_, v) => DataString(key, v)
        case DataNumeric(_, v) => DataNumeric(key, v)
        case _ => DataString(key,"")
      }
    }

    case Term.ApplyInfix(l, Term.Name("->"), Nil, Seq(r)) => {
      val key = eval(l, scope).stringOption.getOrElse("")
      val value = eval(r, scope)
      value match {
        case DataString(_, v) => DataString(key, v)
        case DataNumeric(_, v) => DataNumeric(key, v)
        case _ => DataString(key,"")
      }
    }


  }

}
