package com.actio

import com.typesafe.config.{Config, ConfigValueFactory}

/**
  * Created by maurice on 19/04/17.
  */

trait SimpleIterator[T] {
  def headOption(): Option[T]
}
class DataSetIterator(it: SimpleIterator[DataSet]) extends DataSet {

  override def label: String = "iterator"

  override lazy val elems: Iterator[DataSet] = new Iterator[DataSet]() {
    private var hNext = it.headOption()
    var t: Option[DataSet] = None
    var moved = false

    override def hasNext: Boolean = {
      hNext.isDefined
    }

    override def next(): DataSet = {
      val temp = hNext.get
      hNext = it.headOption()
      temp
    }
  }
}


class DataSourcePaging(src: DataSource, stopCondition: DataSet => Boolean) extends SimpleIterator[DataSet] {
  var ds: DataSet = DataRecord()
  var stop: Boolean = false
  var index: Int = 0
  val originalConfig = src.config

  def headOption(): Option[DataSet] = {
    if(stop) {
      None
    }
    else {
      // override all query elements
      val init = DataRecord(DataNumeric("index", index))
      val nds = DataSetOperations.mergeLeft(ds, init)

      src.config = evalTemplateConfig(originalConfig, nds, "query.read").withoutPath("iterate")

      ds = src.read(nds)
      stop = stopCondition(ds)
      index = index + 1
      Some(ds(0))
    }
  }

  def evalTemplateConfig(config: Config, ds: DataSet, path: String): Config = {
    import scala.collection.JavaConverters._

    config.getObject(path)
      .asScala
      .foldLeft(config)((c, v) =>
        c.withValue(path + "." + v._1, ConfigValueFactory.fromAnyRef(
          MetaTerm.evalTemplate(ds, config.getString(path + "." + v._1))
            .stringOption
            .getOrElse(""))))
  }

}
