package com.actio

import com.actio.dpsystem.Logging
import java.io.StringWriter
import java.io.PrintWriter
import scala.meta.Term.Arg.Repeated
import scala.meta._
/**
  * Created by mauri on 23/07/2016.
  */

object MetaTermStructureCache {
  private val lookup = scala.collection.mutable.HashMap[String,Term]()

  def get(text: String): Term = {
    val opt = lookup.get(text)

    if(opt.isDefined) {
      opt.get
    } else {
      val term = text.parse[Term].get
      lookup += (text -> term)
      term
    }
  }
    
}

object MetaTerm extends Logging {

  def eval(ds: DataSet, text: String): DataSet = eval(ds, text.parse[Term].get)

  def evalLambdas(text: String, ds1: Seq[DataSet]): DataSet = evalLambdas(MetaTermStructureCache.get(text), ds1)

  def evalTemplate(ds: DataSet, text: String): DataSet = eval(ds, interpolate(text))

  def interpolate(str: String): String = "s\"\"\"" + str + "\"\"\""

  def eval(ds: DataSet, t: Term): DataSet = {
    try {
      return t match {

        // evaluate lambda expression
        case Term.Function(Seq(Term.Param(_, Term.Name(name), _, _)), body) =>
          eval(body, Map(name -> ds))

        // evaluate expression, add to scope top level attributes of the DataSet
        case _ => eval(t, ds.elems.map(e => e.label -> e).toList.toMap + ("this" -> ds))
      }
    } catch {
      case e : Throwable =>
        logger.error(" MetaTerm Error:: Exception e "+e.toString)
        val sw = new StringWriter
        e.printStackTrace(new PrintWriter(sw))
        logger.error(sw.toString)
        dumpTerm(t)
        throw(e)
    }
  }

  def evalLambdas(t: Term, ds: Seq[DataSet]): DataSet = t match {
    case Term.Function(seq: Seq[Term.Param], body) => eval(body, (seq.map {
      case Term.Param(Nil, Term.Name(name), None, None) => name
      case _ => ""
    } zip ds).map(m => (m._1 -> m._2)).toMap)
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
    case Term.This(_) => scope("this") match {
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

    case e => throw new Exception(e.toString)
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
            case _ => false
          })
        .getOrElse(Nothin())

    case Term.Apply(
    Term.Select(s, Term.Name("reduceLeft")),
    Seq(Term.Function(Seq(Term.Param(Nil, Term.Name(ta), None, None), Term.Param(Nil, Term.Name(tb), None, None)), rem))) => {
      val array = eval(s, scope)

      if (array.elems.toList.length == 0)
        array
      else
        array
          .elems
          .toList
          .reduceLeft((a, b) => eval(rem, scope + (ta -> a) + (tb -> b)))
    }

    // get DataSet by name
    case Term.Apply(q, Seq(t)) => eval(q, scope)(eval(t, scope).stringOption.getOrElse(""))

  }

  // reverse sign only if the term evaluates to a numeric
  def evalApplyUnary(t: Term.ApplyUnary, scope: Map[String, AnyRef]): DataSet = t match {

    case Term.ApplyUnary(Term.Name("-"), r) => eval(r, scope) match {
      case DataNumeric(label, num) => DataNumeric(label, -num)
      case _ => Nothin()
    }

    case Term.ApplyUnary(Term.Name("!"), r) => eval(r, scope) match {
      case DataBoolean(label, b) => DataBoolean(label,b)
      case _ => Nothin()
    }
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
    case Term.ApplyInfix(l, Term.Name("+"), Nil, Seq(r)) => (eval(l, scope), eval(r, scope)) match {
      case (left: DataNumeric, right: DataNumeric) => DataNumeric(left.num + right.num)
      case (left, right) => DataString(left.stringOption.getOrElse("") +
        right.stringOption.getOrElse(""))
    }

    // subtract numeric
    case Term.ApplyInfix(l, Term.Name("-"), Nil, Seq(r)) => (eval(l, scope), eval(r, scope)) match {
      case (left: DataNumeric, right: DataNumeric) => DataNumeric(
        left.num -
          right.num)
    }

    // multiply numeric
    case Term.ApplyInfix(l, Term.Name("*"), Nil, Seq(r)) => (eval(l, scope), eval(r, scope)) match {
      case (left: DataNumeric, right: DataNumeric) => DataNumeric(
        left.num *
          right.num)
      case _ => Nothin()
    }

    // divide numeric
    case Term.ApplyInfix(l, Term.Name("/"), Nil, Seq(r)) => (eval(l, scope), eval(r, scope)) match {
      case (left: DataNumeric, right: DataNumeric) => DataNumeric(
        left.num /
          right.num)
      case _ => Nothin()
    }

    // currently, equality will do a string comparison
    case Term.ApplyInfix(l, Term.Name("=="), Nil, Seq(r)) => (eval(l, scope), eval(r, scope)) match {
      case (ls: DataString, rs: DataString) => DataBoolean(ls.str == rs.str)
      case (ls: DataNumeric, rs: DataNumeric) => DataBoolean(ls.num == rs.num)
      case (ls: DataBoolean, rs: DataBoolean) => DataBoolean(ls.bool == rs.bool)
      case (ls: DataDate, rs: DataDate) => DataBoolean(ls.date == rs.date)
      case (ls: DataSet, rs: DataSet) => DataBoolean(ls.stringOption.isDefined && rs.stringOption.isDefined && ls.stringOption.get == rs.stringOption.get)
    }

      // shameless copy & paste
    case Term.ApplyInfix(l, Term.Name("!="), Nil, Seq(r)) => (eval(l, scope), eval(r, scope)) match {
      case (ls: DataString, rs: DataString) => DataBoolean(ls.str != rs.str)
      case (ls: DataNumeric, rs: DataNumeric) => DataBoolean(ls.num != rs.num)
      case (ls: DataBoolean, rs: DataBoolean) => DataBoolean(ls.bool != rs.bool)
      case (ls: DataDate, rs: DataDate) => DataBoolean(ls.date != rs.date)
      case (ls: DataSet, rs: DataSet) => DataBoolean(ls.stringOption.isDefined && rs.stringOption.isDefined && ls.stringOption.get != rs.stringOption.get)
    }
    // AND logic
    case Term.ApplyInfix(l, Term.Name("&&"), Nil, Seq(r)) => {
      val ls = eval(l, scope).asInstanceOf[DataBoolean]

      if (ls.bool) {
        eval(r, scope)
      } else {
        DataBoolean(false)
      }
    }

    // OR logic
    case Term.ApplyInfix(l, Term.Name("||"), Nil, Seq(r)) => {
      val ls = eval(l, scope).asInstanceOf[DataBoolean]

      if (ls.bool) {
        DataBoolean(true)
      } else {
        eval(r, scope)
      }
    }

    case Term.ApplyInfix(l, Term.Name("mergeLeft"), Nil, Seq(r)) => {
      DataSetOperations.mergeLeft(eval(l, scope), eval(r, scope))
    }

    case Term.ApplyInfix(Term.Name(key), Term.Name("->"), Nil, Seq(r)) => {
      val value = eval(r, scope)
      value match {
        case DataString(_, v) => DataString(key, v)
        case DataNumeric(_, v) => DataNumeric(key, v)
        case _ => DataString(key, "")
      }
    }

    case Term.ApplyInfix(l, Term.Name("->"), Nil, Seq(r)) => {
      val key = eval(l, scope).stringOption.getOrElse("")
      val value = eval(r, scope)
      value match {
        case DataString(_, v) => DataString(key, v)
        case DataNumeric(_, v) => DataNumeric(key, v)
        case _ => DataString(key, "")
      }
    }


  }

  def dumpTerm(t: Term): Unit = {
    logger.info("TERM==" + t.toString())
  }


  override def clazz: Class[_] = MetaTerm.getClass()

}
