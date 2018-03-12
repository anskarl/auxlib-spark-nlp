package auxlib.spark

import org.apache.spark.sql.SparkSession

trait SparkTestHelper {


  System.setProperty("hadoop.home.dir", System.getProperty("java.io.tmpdir"))

  lazy val spark: SparkSession = {
    val sparkSessionInstance = SparkSession
      .builder()
      .appName("test")
      .master("local[2]")
      .config("spark.driver.memory","1G")
      .config("spark.kryoserializer.buffer.max", "200M")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    sys.addShutdownHook(sparkSessionInstance.stop())

    sparkSessionInstance
  }
}
