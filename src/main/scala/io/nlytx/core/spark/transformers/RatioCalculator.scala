package io.nlytx.core.spark.transformers

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

case class RatioCalculator(override val uid: String) extends Transformer
  with HasInputCol with HasOutputCol with DefaultParamsWritable {

  def this() = this(uid = Identifiable.randomUID("ratioCalculator"))
  def setInputCol(input: String): this.type = set(inputCol, input)
  def setOutputCol(output: String): this.type = set(outputCol, output)

  // Private function that does the work.
  private val ratioCalc: (Int,Int) => Double = (feature:Int,divisor:Int) => {
    println("Feature"+feature.toString)
    println("Divisor"+divisor.toString)
    feature.toDouble / divisor.toDouble
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    val rcUdf = udf(ratioCalc)
    dataset.withColumn($(outputCol),rcUdf(col($(inputCol)),col("token_count")))
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    val inputFields = schema.fields
    StructType(schema.fields :+ StructField($(outputCol), DoubleType))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)
}
