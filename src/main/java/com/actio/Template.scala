package com.actio

/**
  * Created by mauri on 19/06/2016.
  */
object Template  {
  //val template = "{test: [%{repeat(o,data){productid: @o.product_id@}%}]}"
  //val data = DataArray("order_items",List(DataRecord("",List(DataString("product_id","0"))), DataRecord("",List(DataString("product_id","1")))))

  def merge(template: String, d: Data) = expand(template, Map("data" -> d))

  def expand(template: String, map: Map[String, Data]): String = {
    val s = split(template)

    if(template.isEmpty)
      ""
    else if(s.isEmpty)
      replaceVariables(template,map)
    else {
      val pair = getPair(s.get._2)
      val list = evalExpression(pair._2, map).elems.toList
      val rest = s.get._2.substring(s.get._2.indexOf(")")+1)
      expand(s.get._1, map) + list.map(i => expand(rest, map + (pair._1 -> i))).mkString(",") + expand(s.get._3, map)
    }
  }

  def replaceVariables(template: String, map: Map[String, Data]): String = {
    val ra = "@(.*?)@".r.findAllMatchIn(template).map(_.group(1)).toList
    val res = ra.foldLeft[String](template)((t,e) => t.replace("@"+e+"@",evalExpression(e, map).stringOption.getOrElse("")))
    res
  }

  def evalExpression(expr: String, map: Map[String, Data]) =
    if(expr.indexOf(".") >0)
      map(expr.substring(0,expr.indexOf("."))).value(expr.substring(expr.indexOf(".")+1))
    else
      map(expr)

  def getPair(str: String) = (str.substring(7,str.indexOf(",")), str.substring(str.indexOf(",")+1,str.indexOf(")")),"")

  def split(template: String) = {
    val left = template.indexOf("%{")
    val right = indexOfMatchingBracket(template.substring(left+2),0,0)

    if(left == -1 || right == -1)
      None
    else
      Some((template.substring(0,left),template.substring(left+2,left+right+2),template.substring(left+right+4)))
  }
  def indexOfMatchingBracket(str: String, count: Int, add: Int): Int =
    {
      val open = "%{"
      val close = "%}"
      val indexOfOpen = str.indexOf(open)
      val indexOfClosed = str.indexOf(close)

      if(indexOfOpen>=0 && indexOfOpen < indexOfClosed)
        indexOfMatchingBracket(str.substring(indexOfOpen)+2, count + 1, add + indexOfOpen)
      else if (indexOfClosed >= 0 && count == 0)
        add + indexOfClosed
      else if (indexOfClosed >= 0 && count > 0)
        indexOfMatchingBracket(str.substring(indexOfClosed)+2, count - 1, add + indexOfClosed)
      else
        -1
    }

}
