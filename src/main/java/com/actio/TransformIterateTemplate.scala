package com.actio

/**
  * Created by mauri on 15/07/2016.
  * This can probably be moved to a transform function when the function parameters can be parsed more effectively (handle templates)
  */
class TransformIterateTemplate extends TaskTransform {

  override def execute(): Unit = {

    dataSet = if(breakUp) {
      res
    }
    else {
      new DataSetFixedData(
        SchemaArray("items", SchemaRecord("row", List(SchemaString("template", 0)))),
        res)
    }
  }

  def res: DataSet = DataArray("items", dataSet.find(iterate).map(ds =>
    DataRecord("row", List(TemplateEngine.eval(TemplateParser(template), Map("d" -> (() => ExprDataSet(splitGlobalAndLocal(dataSet, ds)))))))).toList)

  lazy val iterate = config.getString("iterate")
  lazy val template = config.getString("template")
  def breakUp = config.hasPath("breakup")

  private def splitGlobalAndLocal(dsGlobal: DataSet, dsLocal: DataSet) =
    DataRecord("", List(DataRecord("local", dsLocal.elems.toList), DataRecord("global", dsGlobal.elems.toList)))
}
