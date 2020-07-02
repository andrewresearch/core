package io.nlytx.core.spark.pipelines

import com.johnsnowlabs.nlp.annotator.{SentenceEmbeddings, SentimentDLModel}
import org.apache.spark.ml.PipelineStage

case class Doc2Sentiment(modelPath:String, sentimentModel:String = "sentimentdl_glove_imdb_en_2.5.0_2.4_1588682682507") extends GenericPipeline {

  private val sentEmbed = new SentenceEmbeddings()
    .setInputCols(Array("document", "glove"))
    .setStorageRef("glove_100d")
    .setOutputCol("sentence_embeddings")
    .setPoolingStrategy("AVERAGE")

  private val sentiment = SentimentDLModel.load(modelPath+sentimentModel+"/")
    .setInputCols("sentence_embeddings")
    .setOutputCol("sentiment")
  
  override val stages:Array[PipelineStage] = Array(sentEmbed, sentiment)

}
