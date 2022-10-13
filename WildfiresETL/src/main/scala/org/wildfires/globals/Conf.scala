package org.wildfires.globals

import org.rogach.scallop.ScallopConf

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {

  val master = opt[String](required = true)
  val rawZonePath = opt[String](required = true)
  val curatedZonePath = opt[String](required = true)
  val pipeline = opt[String](required = true)
  val ingestPreviousDays = opt[Int]()
  verify()

}
