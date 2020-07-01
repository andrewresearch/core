package io.nlytx.core.spark

import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.{DataFrame, SparkSession}

case class File(basePath:String)(implicit val spark:SparkSession) {

  def loadDataFrame(fileName:String) =
    spark.read.load(s"${basePath}/${fileName}.dataframe")

  def saveDataFrame(df:DataFrame,fileName:String,overwrite:Boolean = false) = {
    val saveMode = if (overwrite) "overwrite" else "ErrorIfExists"
    df.write.mode(saveMode)
      .save(s"${basePath}/${fileName}.dataframe")
  }

  def loadPipelineModel(fileName:String) =
    PipelineModel.load(s"${basePath}/${fileName}.pipelinemodel")

  def savePipelineModel(model:PipelineModel, fileName:String, overwrite:Boolean = false) = {
    val saveMode = if (overwrite)
      model.write.overwrite.save(s"${basePath}/${fileName}.pipelinemodel")
    else
      model.write.save(s"${basePath}/${fileName}.pipelinemodel")
  }
}
