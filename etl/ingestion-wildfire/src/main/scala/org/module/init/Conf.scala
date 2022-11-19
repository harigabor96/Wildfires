package org.module.init

import org.eztl.core.conf.IngestionConf
import org.rogach.scallop.ScallopConf

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) with IngestionConf {

  override val master = opt[String](required = true)
  override val rawZonePath = opt[String]()
  override val curatedZonePath = opt[String](required = true)
  override val pipeline = opt[String](required = true)
  override val ingestPreviousDays = opt[Int](default = None)
  override val terminateAfterMs = opt[Int](default = None)
  verify()

}
