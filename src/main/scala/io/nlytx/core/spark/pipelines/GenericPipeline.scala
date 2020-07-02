package io.nlytx.core.spark.pipelines

import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.sql.DataFrame

trait GenericPipeline {

  val stages:Array[PipelineStage] = Array()

  lazy val pipeline:Pipeline = new Pipeline().setStages(stages)

  def getPipeline:Pipeline = pipeline

  def getModel(dataframe:DataFrame):PipelineModel = pipeline.fit(dataframe)

}
