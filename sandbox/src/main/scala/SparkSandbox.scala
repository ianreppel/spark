/**
  * Databaseline code repository
  *
  * Code for post: A Quickie on Reading JSON Resource Files in Apache Spark with Scala
  * Compatibility: Apache Spark 2.1.0 and above
  * Base URL:      https://databaseline.bitbucket.io
  * Author:        Ian Hellström
  *
  * Notes:         In IntelliJ IDEA:
  *                1. From the main menu: Run > Edit Configurations... > 'Use classpath and SDK of module' and set it
  *                1. From the main menu: Run > Run 'Scala Console'
  *                2. Select the code to be run from inside the SparkSandbox object
  *                3. Right click and choose 'Send Selection to Scala Console'
  *
  * Pro tip: collapse the 'Setup' region to execute the entire section with a single selection
  */
object SparkSandbox extends App {

  //region Setup
  // Imports usually go outside of the trait/class/object, but for the sandbox it's is easier to copy-paste
  import org.apache.spark._
  import org.apache.spark.sql._
  import org.apache.spark.rdd.RDD

  // Setup of Spark environment
  implicit val spark = SparkSession.builder().master("local[*]").appName("Spark Sandbox").getOrCreate()
  implicit val sc = spark.sparkContext
  import spark.implicits._

  /**
    * Reads a JSON file from the resources directory as a [[scala.Predef.String]]
    *
    * @param file File name
    * @return String with the contents of the entire JSON file (without any leading, trailing, or repeated whitespace)
    */
  def readJsonResource(file: String): String = {
    // In Scala 2.12 and above, use scala.io.Source.fromResource(file) instead, i.e. without initial forward slash
    val stream = getClass.getResourceAsStream(s"/$file")
    scala.io.Source.fromInputStream(stream)
      .getLines
      .toList
      .mkString(" ")
      .trim
      .replaceAll("\\s+", " ")
  }

  /**
    * Reads a JSON file from the resources directory as an [[org.apache.spark.rdd.RDD]]
    *
    * @param file      File name
    * @param sparkCtxt An in-scope implicit instance of [[org.apache.spark.SparkContext]]
    * @return An with the contents of the entire JSON file (without any leading, trailing, or repeated whitespace)
    */
  def readJsonResourceAsRDD(file: String)(implicit sparkCtxt: SparkContext): RDD[String] =
    sparkCtxt.parallelize(List(readJsonResource(file)))

  /**
    * Reads a JSON file from the resources directory as a [[org.apache.spark.sql.DataFrame]]
    *
    * @param file    File name
    * @param sparkSession An in-scope implicit instance of [[org.apache.spark.sql.SQLContext]]
    * @return A DataFrame with the contents of the entire JSON file
    *         (without any leading, trailing, or repeated whitespace)
    */
  def readJsonResourceAsDataset(file: String)(implicit sparkSession: SparkSession): Dataset[Row] =
    sparkSession.read.json(readJsonResourceAsRDD(file)(sparkSession.sparkContext))

  //endregion

  // Custom sandbox logic
  val ds = readJsonResourceAsDataset("example.json")
  ds.printSchema()
  ds.show()

  //  spark.stop()
}
