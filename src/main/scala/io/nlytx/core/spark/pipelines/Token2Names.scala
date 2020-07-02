package io.nlytx.core.spark.pipelines

import com.johnsnowlabs.nlp.annotator.{NerCrfModel, WordEmbeddingsModel}
import org.apache.spark.ml.PipelineStage

case class Token2Names(modelPath:String, gloveModel:String = "glove_100d_en_2.4.0_2.4_1579690104032",
                       nerModel:String = "ner_crf_en_2.4.0_2.4_1580237286004") extends GenericPipeline {

  private val glove = WordEmbeddingsModel.load(modelPath+gloveModel+"/")
    .setInputCols("sentence","token")
    .setOutputCol("glove")
    .setCaseSensitive(false)

  private val ner = NerCrfModel.load(modelPath+nerModel+"/")
    .setInputCols("sentence","token","pos","glove")
    .setOutputCol("ner")
    .setIncludeConfidence(true)

  override val stages:Array[PipelineStage] = Array(glove, ner)


}
