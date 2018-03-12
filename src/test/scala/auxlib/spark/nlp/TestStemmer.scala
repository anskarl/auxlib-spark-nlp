package auxlib.spark.nlp

import auxlib.spark.SparkTestHelper
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.{RegexTokenizer, Tokenizer}
import org.scalatest.{FlatSpec, Inspectors, Matchers}

class TestStemmer extends FlatSpec with SparkTestHelper with Matchers with Inspectors {
  import TestStemmer._
  import spark.implicits._

  "Stemmer with default settings (Porter Stemmer)" should "transform the given tokens into stemmed tokens" in{
    val df = spark.sparkContext.parallelize(data).toDF("id", "text")

    val tokenizer = new RegexTokenizer()
      .setPattern("\\W")
      .setInputCol("text")
      .setOutputCol("tokens")

    val stemmer = Stemmer.Porter
      .create()
      .setInputCol("tokens")

    val tokenized = tokenizer.transform(df)

    val resultDF = stemmer
      .transform(tokenized)
      .select('id, 'stemmed_tokens).as[(Int, Seq[String])]

    val result: Array[(Int, Seq[String])] = resultDF.collect().sortBy{case (id, _) => id}

    forAll(result.zip(groundTruthPorter)){
      case (r, g) => r shouldEqual g
    }


  }



}


private[nlp] object TestStemmer{

  val data = Seq(
    (1, "Life is a characteristic that distinguishes physical entities that do have biological processes, such as signaling and self-sustaining processes, from those that do not, either because such functions have ceased, or because they never had such functions and are classified as inanimate."),
    (2, "Various forms of life exist, such as plants, animals, fungi, protists, archaea, and bacteria."),
    (3, "The criteria can at times be ambiguous and may or may not define viruses, viroids, or potential artificial life as living."),
    (4, "Biology is the primary science concerned with the study of life, although many other sciences are involved.")
  )

  val groundTruthPorter: Array[(Int, Seq[String])] = Array(
    (1,"life,is,a,characterist,that,distinguish,physic,entiti,that,do,have,biolog,process,such,as,signal,and,self,sustain,process,from,those,that,do,not,either,becaus,such,function,have,ceas,or,becaus,thei,never,had,such,function,and,ar,classifi,as,inanim".split(",")),
    (2,"variou,form,of,life,exist,such,as,plant,anim,fungi,protist,archaea,and,bacteria".split(",")),
    (3,"the,criteria,can,at,time,be,ambigu,and,mai,or,mai,not,defin,virus,viroid,or,potenti,artifici,life,as,live".split(",")),
    (4,"biologi,is,the,primari,scienc,concern,with,the,studi,of,life,although,mani,other,scienc,ar,involv".split(","))
  )


}
