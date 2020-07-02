package io.nlytx.core.spark.pipelines

import com.johnsnowlabs.nlp.DocumentAssembler
import com.johnsnowlabs.nlp.annotator.{SentenceDetector, Tokenizer}
import com.johnsnowlabs.nlp.annotators.Normalizer
import io.nlytx.core.spark.transformers.AnnotationCounter
import org.apache.spark.ml.PipelineStage

case class Doc2Token() extends GenericPipeline {

  private val docAssembler = new DocumentAssembler()
    .setInputCol("text")
    .setOutputCol("document")

  private val sentDetect = new SentenceDetector()
    .setInputCols("document")
    .setOutputCol("sentence")

  private val sentCounter:AnnotationCounter = new AnnotationCounter()
    .setInputCol("sentence")
    .setOutputCol("sentence_count")

  private val tokenizer = new Tokenizer()
    .setInputCols("sentence")
    .setOutputCol("token")

//  val normalizer = new Normalizer()
//    .setInputCols(Array("token"))
//    .setOutputCol("norm")

  private val tokenCounter:AnnotationCounter = new AnnotationCounter()
    .setInputCol("token")
    .setOutputCol("token_count")

  override val stages:Array[PipelineStage] = Array(docAssembler, sentDetect, sentCounter, tokenizer, tokenCounter)


}
