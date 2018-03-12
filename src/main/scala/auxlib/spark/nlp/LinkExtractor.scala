package auxlib.spark.nlp


import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap, Params}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.nibor.autolink.{LinkType, LinkExtractor => AutoLinkExtractor}
import scala.collection.JavaConverters._

class LinkExtractor(
  override val uid: String,
  linkTypes: Set[LinkType] = Set(LinkType.EMAIL, LinkType.URL, LinkType.WWW)
) extends Transformer with Params {

  final val inCol =
    new Param[String](this, "text", "The name of the input column")

  final val outCol =
    new Param[String](this, "links", "The name of the output links column")

  def this() = this(Identifiable.randomUID("link-extractor"))
  def this(linkTypes: Set[LinkType]) = this(Identifiable.randomUID("link-extractor"), linkTypes)

  setDefault(inCol, "text")
  setDefault(outCol, "links")

  def setInputColumn(value: String): this.type = set(inCol, value)

  def setOutputColumn(value: String): this.type = set(outCol, value)

  override def transform(dataset: Dataset[_]): DataFrame = {
    val spark = dataset.sparkSession
    import spark.implicits._

    val newSchema = transformSchema(dataset.schema)

    val inputRdd = dataset.select(col("*")).rdd

    val resultingRdd = inputRdd.mapPartitions{ iter =>
      val linkExtractor = new SimpleLinkExtractor(linkTypes)

      iter.map{ row =>
        val result: Option[Seq[Row]] = Option(row.getAs[String](${inCol}))
          .flatMap(text => linkExtractor.extractFrom(text))
          .map(ls => ls.map(_.toRow))


        Row.merge(row, Row(result.orNull))
      }

    }

    spark.createDataFrame(resultingRdd, newSchema)
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    val inputColType = schema.fields(schema.fieldIndex(${inCol}))

    require(inputColType.dataType == StringType,
      s"Input type '${inputColType.dataType}' of column '${inputColType.name}' did not match input type 'StringType'")

    schema.add{
      StructField(
        name = ${outCol},
        dataType = ArrayType(Link.encoder.schema, containsNull = true),
        nullable = true)
    }
  }
}

private[nlp] class SimpleLinkExtractor(linkTypes: Set[LinkType] = Set(LinkType.EMAIL, LinkType.URL, LinkType.WWW)){

  private val extractor = AutoLinkExtractor.builder().linkTypes(linkTypes.asJava).build()


  def extractFrom(text: String): Option[Seq[Link]] ={
    val links = extractor.extractLinks(text).asScala.toArray

    if(links.isEmpty)
      Option.empty[Seq[Link]]
    else
      Some(links.map(ls => Link(text.substring(ls.getBeginIndex, ls.getEndIndex), ls.getType.toString, ls.getBeginIndex, ls.getEndIndex)))
  }
}

case class Link(value: String, linkType: String, beginIndex: Int, endIndex: Int) {
  def toRow: Row = Row.fromTuple(this) //Row.fromSeq(Seq(value,linkType,beginIndex,endIndex))
}

object Link{
  final val encoder = Encoders.product[Link]
}