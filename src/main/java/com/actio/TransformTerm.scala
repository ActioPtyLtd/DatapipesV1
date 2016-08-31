package com.actio

import com.actio.dpsystem.{ DPSystemConfig, DPFnNode }

/**
 * Created by mauri on 2/08/2016.
 */
class TransformTerm extends Task {

  override def execute(): Unit = { dataSet = MetaTerm.eval(dataSet, term) }

  override def load(): Unit = ???

  override def extract(): Unit = ???

  lazy val term = config.getString("term")

  override def setNode(_node: DPFnNode, _sysconf: DPSystemConfig): Unit = {
    sysconf = _sysconf
    super.setConfig(sysconf.getTaskConfig(_node.getName).toConfig, sysconf.getMasterConfig)
    node = _node
  }
}
