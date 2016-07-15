package com.actio

/**
  * Created by mauri on 15/07/2016.
  * This can probably be moved to a transform function when the function parameters can be parsed more effectively (handle templates)
  */
class TransformIterateTemplate extends TaskTransform {

  override def execute(): Unit = {
    val tp = TemplateParser(template)

    dataSet = new DataSetFixedData(SchemaArray("items", SchemaRecord("row", List(SchemaString("", 0)))) ,DataArray("items", dataSet.find(iterate).map(ds =>
      DataRecord("row", List(TemplateEngine.eval(tp, Map("d" -> (() => ExprDataSet(splitGlobalAndLocal(dataSet, ds)))))))).toList))
  }

  lazy val iterate = config.getString("iterate")
  lazy val template = config.getString("template")

  private def splitGlobalAndLocal(dsGlobal: DataSet, dsLocal: DataSet) =
    DataRecord("", List(DataRecord("local", dsLocal.elems.toList), DataRecord("global", dsGlobal.elems.toList)))
}
