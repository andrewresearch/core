package io.nlytx.core.spark.pipelines

import com.johnsnowlabs.nlp.annotator.Chunker
import com.johnsnowlabs.nlp.annotators.pos.perceptron.PerceptronModel
import io.nlytx.core.spark.transformers.{AnnotationCounter, RatioCalculator}
import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.feature.VectorAssembler


case class Token2Features(patterns:Map[String,String],modelPath:String,posModel:String = "pos_anc_en_2") extends GenericPipeline {

  private val tokenFeatures = patterns.keys.toList

  private val posTagger:PipelineStage = PerceptronModel.load(modelPath+posModel+"/")
    .setInputCols("sentence","token")
    .setOutputCol("pos")

  private val patternChunkers:List[Chunker] = patterns.toList.map{ p =>
    new Chunker()
      .setInputCols("sentence", "pos")
      .setOutputCol(p._1)
      .addRegexParser(p._2)
  }

  private val featureCounters:List[AnnotationCounter] = tokenFeatures.map { f =>
    new AnnotationCounter()
      .setInputCol(f)
      .setOutputCol(f+"_count")
  }

  private val featureRatios:List[RatioCalculator] = tokenFeatures.map { f =>
    new RatioCalculator()
      .setInputCol(f+"_count")
      .setOutputCol(f+"_ratio")
  }

  private val featureVectors:List[VectorAssembler] = tokenFeatures.map { f =>
    new VectorAssembler()
      .setInputCols(Array(f+"_ratio"))
      .setOutputCol(f+"_vec")
  }

  override val stages:Array[PipelineStage] = Array(posTagger) ++ patternChunkers ++
      featureCounters ++ featureRatios ++ featureVectors


}
