package org.module.init

import org.eztl.core.conf.tDatamartConf
import org.rogach.scallop.ScallopConf

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) with tDatamartConf {

  override val master = opt[String](required = true)
  override val curatedZonePath = opt[String](required = true)
  override val pipeline = opt[String](required = true)
  verify()

}
