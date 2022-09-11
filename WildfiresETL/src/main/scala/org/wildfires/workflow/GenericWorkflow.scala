package org.wildfires.workflow

trait GenericWorkflow {

  def execute(): Unit = {
    runBronzePipelines()
    runSilverPipelines()
    runGoldPipelines()
  }

  def runBronzePipelines(): Unit

  def runSilverPipelines(): Unit

  def runGoldPipelines(): Unit
}