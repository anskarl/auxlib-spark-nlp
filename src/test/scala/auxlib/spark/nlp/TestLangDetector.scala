package auxlib.spark.nlp

import com.optimaize.langdetect.profiles.LanguageProfileReader
import auxlib.spark.SparkTestHelper
import org.scalatest._

class TestLangDetector extends FlatSpec with SparkTestHelper with Matchers with Inspectors {
  import spark.implicits._
  import TestLangDetector._


  "A LangDetector with default settings" should "detect correct the language of the given texts" in {
    val df = spark.sparkContext.parallelize(data).toDF("id", "text")

    val langDetector = new LangDetector()

    val resultDF = langDetector.transform(df)

    val result: Array[String] = resultDF.select('id, 'language).as[(Int, String)]
      .collect()
      .sortBy{case (id, _) => id}
      .map{ case (_, lang) => lang}

    forAll(result.zip(groundTruth)){
      case (r, g) => r shouldEqual g
    }

  }

  "A LangDetector with custom settings" should "detect correct the language of the given texts" in {
    val df = spark.sparkContext.parallelize(data).toDF("id", "text")

    val langDetector = LangDetector.create{ builder =>
      builder
        .minimalConfidence(0.8999d)
        .probabilityThreshold(0.3)
        .withProfiles(new LanguageProfileReader().readAllBuiltIn())
        .build()
    }

    val resultDF = langDetector.transform(df)

    val result: Array[String] = resultDF.select('id, 'language).as[(Int, String)]
      .collect()
      .sortBy{case (id, _) => id}
      .map{ case (_, lang) => lang}

    forAll(result.zip(groundTruth)){
      case (r, g) => r shouldEqual g
    }
  }
}

private[nlp] object TestLangDetector {

  val data = Seq(
    (1, "Computer science is the study of the theory, experimentation, and engineering that form the basis for the design and use of computers. It is the scientific and practical approach to computation and its applications and the systematic study of the feasibility, structure, expression, and mechanization of the methodical procedures (or algorithms) that underlie the acquisition, representation, processing, storage, communication of, and access to, information. An alternate, more succinct definition of computer science is the study of automating algorithmic processes that scale. A computer scientist specializes in the theory of computation and the design of computational systems"),
    (2, "Informatik ist die Wissenschaft von der systematischen Darstellung, Speicherung, Verarbeitung und Übertragung von Informationen, besonders der automatischen Verarbeitung mithilfe von Digitalrechnern. Historisch hat sich die Informatik einerseits als Formalwissenschaft aus der Mathematik entwickelt, andererseits als Ingenieursdisziplin aus dem praktischen Bedarf nach der schnellen und insbesondere automatischen Ausführung von Berechnungen."),
    (3, "Επιστήμη υπολογιστών ονομάζεται η θετική και εφαρμοσμένη επιστήμη η οποία ερευνά τα θεωρητικά θεμέλια και τη φύση των δομών δεδομένων, των αλγορίθμων και των υπολογισμών, από τη σκοπιά της σχεδίασης, της ανάπτυξης, της υλοποίησης, της διερεύνησης, της ανάλυσης και της προδιαγραφής τους. Ασχολείται με τη συστηματική μελέτη της σκοπιμότητας, της δομής, της έκφρασης και του μηχανισμού των μεθοδικών διεργασιών (ή αλγορίθμων) που αποτελούν την επεξεργασία, την αποθήκευση, την επικοινωνία και την πρόσβαση στα δεδομένα. Ένας εναλλακτικός ορισμός της επιστήμης των υπολογιστών είναι η μελέτη της αυτοματοποίησης αλγοριθμικών διεργασιών που κλιμακώνονται. Ένας επιστήμονας υπολογιστών ειδικεύεται στη θεωρία της υπολογιστικής ισχύος και το σχεδιασμό των υπολογιστικών συστημάτων"),
    (4, "L'informatique est un domaine d'activité scientifique, technique et industriel concernant le traitement automatique de l'information par l'exécution de programmes informatiques par des machines : des systèmes embarqués, des ordinateurs, des robots, des automates, etc. Ces champs d'application peuvent être séparés en deux branches, l'une, de nature théorique, qui concerne la définition de concepts et modèles, et l'autre, de nature pratique, qui s'intéresse aux techniques concrètes de mise en œuvre. Certains domaines de l'informatique peuvent être très abstraits, comme la complexité algorithmique, et d'autres peuvent être plus proches d'un public profane. Ainsi, la théorie des langages demeure un domaine davantage accessible aux professionnels formés (description des ordinateurs et méthodes de programmation), tandis que les métiers liés aux interfaces homme-machine sont accessibles à un plus large public."),
    (5, "L'informatica è la scienza applicata che si occupa del trattamento dell'informazione mediante procedure automatizzate. In particolare ha per oggetto lo studio dei fondamenti teorici dell'informazione, della sua computazione a livello logico e delle tecniche pratiche per la sua implementazione e applicazione in sistemi elettronici automatizzati detti quindi sistemi informatici. Come tale è una disciplina fortemente connessa con l'automatica, l'elettronica ed anche l'elettromeccanica. ")
  )

  val groundTruth = Seq(
    "EN",
    "DE",
    "EL",
    "FR",
    "IT"
  )
}
