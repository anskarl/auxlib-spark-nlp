package auxlib.spark.nlp

import opennlp.tools.stemmer.snowball.SnowballStemmer
import opennlp.tools.stemmer.snowball.SnowballStemmer.ALGORITHM
import opennlp.tools.stemmer.{PorterStemmer, Stemmer => ONLPStemmer}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap, Params}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row}


class Stemmer(
  override val uid: String,
  stemmerBuilder: () => ONLPStemmer) extends Transformer with Params  {

  final val inCol =
    new Param[String](this, "tokens", "The name of the input tokens column")

  final val outCol =
    new Param[String](this, "stemmed_tokens", "The name of the output stemmed_tokens column")

  final val lowercaseFirst =
    new Param[Boolean](this, "lowercase_tokens", "Lowercase tokens before applying stemming (default is false)")

  def this(stemmerBuilder: () => ONLPStemmer) = this(Identifiable.randomUID("stemmer"), stemmerBuilder)

  def this() = this(Identifiable.randomUID("stemmer"), () => new PorterStemmer())

  setDefault(inCol, "tokens")
  setDefault(outCol, "stemmed_tokens")
  setDefault(lowercaseFirst, false)

  def setInputCol(value: String): this.type  = set(inCol, value)

  def setOutputCol(value: String): this.type  = set(outCol, value)

  def setLowercaseFirst(value: Boolean): this.type = set(lowercaseFirst, value)

  override def transform(dataset: Dataset[_]): DataFrame = {
    val spark = dataset.sparkSession
    import spark.implicits._

    val newSchema = transformSchema(dataset.schema)

    val inputRdd = dataset.select(col("*")).rdd

    val resultingRdd = inputRdd.mapPartitions{ iter =>
      val stemmer = stemmerBuilder()

      iter.map{ row =>
        val tokens =
          if(${lowercaseFirst}) row.getAs[Seq[String]](inCol.name).map(_.toLowerCase)
          else row.getAs[Seq[String]](inCol.name)

        val result = tokens.map(token => if(token != null) stemmer.stem(token) else null)

        Row.merge(row, Row(result))
      }
    }

    spark.createDataFrame(resultingRdd, newSchema)
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    val inputColType = schema.fields(schema.fieldIndex(${inCol}))

    val expectedType = ArrayType(StringType, containsNull = true)
    require(inputColType.dataType == expectedType,
      s"Input type '${inputColType.dataType}' of column '${inputColType.name}' did not match input type '${expectedType}'")

    schema.add {
      StructField(
        name = ${outCol},
        dataType = ArrayType(elementType = StringType, containsNull = true),
        nullable = true)

    }
  }
}

object Stemmer {

  def create(builder: () => ONLPStemmer) = new Stemmer(builder)

  object Porter {
    def create(): Stemmer = new Stemmer()
  }

  object Snowball {
    def create(algorithm: ALGORITHM, repeat: Int = 1): Stemmer = {
      new Stemmer(() => new SnowballStemmer(algorithm, repeat))
    }
  }

}
