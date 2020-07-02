package io.nlytx.core.spark.transformers

import com.johnsnowlabs.nlp.Annotation
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{udf,col}
import org.apache.spark.sql.types.{ArrayType, IntegerType, StructField, StructType}

import scala.collection.mutable

case class AnnotationCounter(override val uid: String) extends Transformer
  with HasInputCol with HasOutputCol with DefaultParamsWritable {

  def this() = this(uid = Identifiable.randomUID("annotationCounter"))
  def setInputCol(input: String): this.type = set(inputCol, input)
  def setOutputCol(output: String): this.type = set(outputCol, output)

  // Private function that does the work.
  private val annotationCounter: mutable.WrappedArray[Annotation] => Int = (annotations:mutable.WrappedArray[Annotation]) => {
    annotations.toList.length
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    val acUdf = udf(annotationCounter)
    dataset.withColumn($(outputCol),acUdf(col($(inputCol))))
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    require(schema($(inputCol)).dataType.isInstanceOf[ArrayType],
      s"Input column must be of type StructType but got ${schema($(inputCol)).dataType}")
    val inputFields = schema.fields
    require(!inputFields.exists(_.name == $(outputCol)),
      s"Output column ${$(outputCol)} already exists.")
    StructType(schema.fields :+ StructField($(outputCol), IntegerType))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
}
