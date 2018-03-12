package auxlib.spark.nlp

import com.optimaize.langdetect.ngram.NgramExtractors
import com.optimaize.langdetect.profiles.LanguageProfileReader
import com.optimaize.langdetect.{LanguageDetector, LanguageDetectorBuilder}
import com.optimaize.langdetect.text.CommonTextObjectFactories
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap, Params}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._


class LangDetector(
  override val uid: String,
  langDetectBuilder: () => LanguageDetector
) extends Transformer with Params {

  final val inCol =
    new Param[String](this, "text", "The name of the input column")

  final val outCol =
    new Param[String](this, "language", "The name of the output language column")

  def this() = this(Identifiable.randomUID("language-detector"), LangDetector.defaultLangBuilder)
  def this(langDetectorBuilder: () => LanguageDetector) = this(Identifiable.randomUID("language-detector"), langDetectorBuilder)

  setDefault(inCol, "text")
  setDefault(outCol, "language")

  def setInputColumn(value: String): this.type = set(inCol, value)

  def setOutputColumn(value: String): this.type = set(outCol, value)

  override def transform(dataset: Dataset[_]): DataFrame ={
    val spark = dataset.sparkSession
    import spark.implicits._

    val newSchema = transformSchema(dataset.schema)

    val inputRDD = dataset.select(col("*")).rdd



    val resultingRdd = inputRDD.mapPartitions{ iter =>

      val textObjectFactory = CommonTextObjectFactories.forDetectingOnLargeText()

      val languageDetector = langDetectBuilder()

        iter.map{ row =>

          val result: Option[String] = Option(row.getAs[String](${inCol})).flatMap{ text =>
            val textObject = textObjectFactory.forText(text)
            languageDetector.detect(textObject).asScala.map(l => l.getLanguage.toUpperCase)
          }

          Row.merge(row, result.map(Row.apply(_)).getOrElse(Row.empty))

      }
    }

    // return the resulting data-frame with the additional column
    spark.createDataFrame(resultingRdd, newSchema)
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  @throws[IllegalArgumentException](cause = "When an input column does not exists or does not have the expected type")
  override def transformSchema(schema: StructType): StructType = {
    val inputColType = schema.fields(schema.fieldIndex(${inCol}))

    require(inputColType.dataType == StringType,
      s"Input type '${inputColType.dataType}' of column '${inputColType.name}' did not match input type 'StringType'")


    schema.add(StructField(name = ${outCol}, dataType = StringType, nullable = true))
  }
}

object LangDetector {

  def apply(): LangDetector = new LangDetector()

  def create(): LangDetector = new LangDetector()

  def create(builder: LanguageDetectorBuilder => LanguageDetector): LangDetector = {
    new LangDetector(() => builder(LanguageDetectorBuilder.create(NgramExtractors.standard())))
  }

  protected def defaultLangBuilder: () => LanguageDetector = {
    () => LanguageDetectorBuilder
      .create(NgramExtractors.standard())
      .withProfiles(new LanguageProfileReader().readAllBuiltIn())
      .build()
  }
}
