package auxlib.spark.nlp

import auxlib.spark.SparkTestHelper
import org.nibor.autolink.LinkType
import org.scalatest._


class TestLinkExtractor extends FlatSpec with SparkTestHelper with Matchers with Inspectors {
  import TestLinkExtractor._
  import spark.implicits._


  "A simple link extractor with default settings" should "extract email, url and www link types" in {
    val extractor = new SimpleLinkExtractor()

    val result: Seq[Option[Seq[Link]]] = data.map{ case (_, text) => extractor.extractFrom(text)}

    forAll(result.zip(groundTruthAll)) {
      case (r, g) => r shouldEqual g
    }

  }


  "A simple link extractor with email only settings" should "extract only email types" in {
    val extractor = new SimpleLinkExtractor(Set(LinkType.EMAIL))

    val result: Seq[Option[Seq[Link]]] = data.map{ case (_, text) => extractor.extractFrom(text)}

    forAll(result.zip(groundTruthEmailOnly)) {
      case (r, g) => r shouldEqual g
    }

  }


  "A simple link extractor with URL only settings" should "extract only URL types" in {
    val extractor = new SimpleLinkExtractor(Set(LinkType.URL))

    val result: Seq[Option[Seq[Link]]] = data.map{ case (_, text) => extractor.extractFrom(text)}

    forAll(result.zip(groundTruthURLOnly)) {
      case (r, g) => r shouldEqual g
    }

  }


  "A simple link extractor with WWW only settings" should "extract only WWW types" in {
    val extractor = new SimpleLinkExtractor(Set(LinkType.WWW))

    val result: Seq[Option[Seq[Link]]] = data.map{ case (_, text) => extractor.extractFrom(text)}

    forAll(result.zip(groundTruthWWWOnly)) {
      case (r, g) => r shouldEqual g
    }

  }



  "A link extractor with default settings" should "extract email, url and www link types" in {

    val dataframe = spark.sparkContext.parallelize(data).toDF("id", "text")

    val linkE = new LinkExtractor()

    val resultDF = linkE.transform(dataframe)

    val result = resultDF.select('id, 'links).as[(Int, Option[Seq[Link]])]
      .collect()
      .sortBy{case (id, _) => id}
      .map{case (_, links) => links}

    forAll(result.zip(groundTruthAll)) {
      case (r, g) => r shouldEqual g
    }

  }


  "A link extractor with email only settings" should "extract only email types" in {

    val dataframe = spark.sparkContext.parallelize(data).toDF("id", "text")

    val linkE = new LinkExtractor(linkTypes = Set(LinkType.EMAIL)).setOutputColumn("links_email")

    val resultDF = linkE.transform(dataframe)

    val result = resultDF.select('id, 'links_email).as[(Int, Option[Seq[Link]])]
      .collect()
      .sortBy{case (id, _) => id}
      .map{case (_, links) => links}

    forAll(result.zip(groundTruthEmailOnly)) {
      case (r, g) => r shouldEqual g
    }

  }


  "A link extractor with URL only settings" should "extract only URL types" in {

    val dataframe = spark.sparkContext.parallelize(data).toDF("id", "text")

    val linkE = new LinkExtractor(linkTypes = Set(LinkType.URL)).setOutputColumn("links_url")

    val resultDF = linkE.transform(dataframe)

    val result = resultDF.select('id, 'links_url).as[(Int, Option[Seq[Link]])]
      .collect()
      .sortBy{case (id, _) => id}
      .map{case (_, links) => links}

    forAll(result.zip(groundTruthURLOnly)) {
      case (r, g) => r shouldEqual g
    }

  }


  "A link extractor with WWW only settings" should "extract only WWW types" in {

    val dataframe = spark.sparkContext.parallelize(data).toDF("id", "text")

    val linkE = new LinkExtractor(linkTypes = Set(LinkType.WWW)).setOutputColumn("links_www")

    val resultDF = linkE.transform(dataframe)

    val result = resultDF.select('id, 'links_www).as[(Int, Option[Seq[Link]])]
      .collect()
      .sortBy{case (id, _) => id}
      .map{case (_, links) => links}

    forAll(result.zip(groundTruthWWWOnly)) {
      case (r, g) => r shouldEqual g
    }

  }

}

private[nlp] object TestLinkExtractor{
  final val data: Seq[(Int, String)] = Seq(
    (1, "Lorem ipsum dolor sit amet, consectetur adipiscing elit (someone@mail.com), sed do eiusmod tempor incididunt (someone.else@mail.com) ut labore et dolore magna aliqua."),
    (2, "Ut enim ad minim veniam (mailto:John.Doe@example.com), quis nostrud exercitation (ftp://ftp.is.co.za/rfc/rfc1808.txt) ullamco (www.website.com) laboris nisi ut aliquip ex ea commodo consequat. "),
    (3, "Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur."),
    (4, "Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum."),
    (5, "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua."),
    (6, "Ut enim ad minim veniam, quis nostrud exercitation (ldap://[2001:db8::7]/c=GB?objectClass?one) ullamco laboris nisi ut aliquip ex ea commodo consequat."),
    (7, "Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur."),
    (8, "Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt (telnet://192.0.2.16:80/) mollit anim id est laborum.")
  )

  final val groundTruthAll: Seq[Option[Seq[Link]]] = Seq(
    Some(Seq(Link("someone@mail.com","EMAIL",57,73), Link("someone.else@mail.com","EMAIL",110,131))),
    Some(Seq(Link("John.Doe@example.com","EMAIL",32,52), Link("ftp://ftp.is.co.za/rfc/rfc1808.txt","URL",82,116), Link("www.website.com","WWW",127,142))),
    None,
    None,
    None,
    Some(Seq(Link("ldap://[2001:db8::7]/c=GB?objectClass?one","URL",52,93))),
    None,
    Some(Seq(Link("telnet://192.0.2.16:80/","URL",84,107)))
  )


  final val groundTruthEmailOnly: Seq[Option[Seq[Link]]] = Seq(
    Some(Seq(Link("someone@mail.com","EMAIL",57,73), Link("someone.else@mail.com","EMAIL",110,131))),
    Some(Seq(Link("John.Doe@example.com","EMAIL",32,52))),
    None,
    None,
    None,
    None,
    None,
    None
  )


  final val groundTruthWWWOnly: Seq[Option[Seq[Link]]] = Seq(
    None,
    Some(Seq(Link("www.website.com","WWW",127,142))),
    None,
    None,
    None,
    None,
    None,
    None
  )


  final val groundTruthURLOnly: Seq[Option[Seq[Link]]] = Seq(
    None,
    Some(Seq(Link("ftp://ftp.is.co.za/rfc/rfc1808.txt","URL",82,116))),
    None,
    None,
    None,
    Some(Seq(Link("ldap://[2001:db8::7]/c=GB?objectClass?one","URL",52,93))),
    None,
    Some(Seq(Link("telnet://192.0.2.16:80/","URL",84,107)))
  )
}
