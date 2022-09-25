package org.wildfires

import org.rogach.scallop.{ScallopConf, ScallopOption}

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  val master = opt[String](required = true)
  val rawZonePath = opt[String](required = true)
  val curatedZonePath = opt[String](required = true)
  val pipeline = opt[String](required = true)
  verify()
  }

