package com.spark.assignment2

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers

import scala.concurrent.duration._

class Assignment2Test extends AnyFunSuite with Matchers with BeforeAndAfterEach {

  /**
   * Set this value to 'true' to halt after execution so you can view the Spark UI at localhost:4040.
   * NOTE: If you use this, you must terminate your test manually.
   * OTHER NOTE: You should only use this if you run a test individually.
   */
  val BLOCK_ON_COMPLETION = false;

  // Paths to dour data.
  val OCT_DATA_CSV_PATH = "data/2019-Oct.csv"
  val NOV_DATA_CSV_PATH = "data/2019-Nov.csv"
  val TECIN_DATA_CSV_PATH = "data/TechnologyIndex.csv"

  /**
   * Create a SparkSession that runs locally on our laptop.
   */
  val spark =
    SparkSession
      .builder()
      .appName("Assignment 2")
      .master("local[*]") // Spark runs in 'local' mode using all cores
      .getOrCreate()

  /**
   * Encoders to assist converting a csv records into Case Classes.
   * They are 'implicit', meaning they will be picked up by implicit arguments,
   * which are hidden from view but automatically applied.
   */
  implicit val octEncoder: Encoder[oct] = Encoders.product[oct]
  implicit val novEncoder: Encoder[nov] = Encoders.product[nov]
  implicit val tecinEncoder: Encoder[tecin] = Encoders.product[tecin]

  /**
   * Let Spark infer the data types. Tell Spark this CSV has a header line.
   */
  val csvReadOptions =
    Map("inferSchema" -> true.toString, "header" -> true.toString)

  /**
   * Create oct Spark collections
   */
  def octDataDS: Dataset[oct] = spark.read.options(csvReadOptions).csv(OCT_DATA_CSV_PATH).as[oct]
  def octDF: DataFrame = octDataDS.toDF()

  /**
    * Create nov Spark collections
    */
  def novDataDS: Dataset[nov] = spark.read.options(csvReadOptions).csv(NOV_DATA_CSV_PATH).as[nov]
  def novDF: DataFrame = novDataDS.toDF()
  /**
    * Create tecin Spark collections
    */
  def tecinDataDS: Dataset[tecin] = spark.read.options(csvReadOptions).csv(TECIN_DATA_CSV_PATH).as[tecin]
  def tecinDF: DataFrame = tecinDataDS.toDF()


  /**
   * Keep the Spark Context running so the Spark UI can be viewed after the test has completed.
   * This is enabled by setting `BLOCK_ON_COMPLETION = true` above.
   */
  override def afterEach: Unit = {
    if (BLOCK_ON_COMPLETION) {
      // open SparkUI at http://localhost:4040
      Thread.sleep(5.minutes.toMillis)
    }
  }

  def timed[E](name: String, work: => E): E = {
    val start = System.currentTimeMillis()
    val result = work
    val end = System.currentTimeMillis()
    val span = end - start
    println(s"$name ran for $span milliseconds")
    result
  }
  /**
   * This test to to find the distinct event types in the oct 2019 dataframe and
    *  to find the count of records having the same event type
   */
  test("find distinct event types and count")
  {timed("test1",{
    val result = Assignment2.Problem1(octDF.toDF())
    val expectedData = Seq(
      Row("purchase",17296 ),
      Row("view",1016239),
      Row("cart",15040)

    )})}

  /**
    * This test to to find the distinct product categories in the oct 2019 dataframe
    *  since there are many we shall check for the existence of three of the product categories
    */
  test("What are the unique product categories available in the online store?")
  {timed("test2",{
    val result = Assignment2.Problem2(octDF.toDF())
    val expectedData = Seq(
      Row("[stationery.cartrige]"),
      Row("[computers.ebooks]"),
      Row("[apparel.jeans]"))}
  )
  }
  /**
    * We need to find the most expensive product in the entire online store.
    * Making use of Oct 2019 DataFrame.
    */
  test("Which product is the most expensive in the entire online store?")
  {timed("test3",{
    Assignment2.Problem3(octDF) must equal("[electronics.clocks]")}
  )
  }
  /**
    * We need to find the count of customers who just viewed a product in October 2019 and bought the same product in November 2019
    * we need to make sure the customer is buying the same product and not any other product.
    */

  test("Count of customers who just viewed the product in OCT-2019 and made purchases in NOV-2019")
  {timed("test4",{
    Assignment2.Problem4(octDF,novDF) must equal(2049)}
  )}
  /**
    * This test helps to find if the cost of the ipad has remainined same or not in a span of one month.
    * The result is boolean.
    */
  test("compare post of ipad between oct 2019 and nov 2019")
  {timed("test5",{
    val result = Assignment2.Problem5(octDF,novDF)
    result must equal(false)}
  )
  }
  /**
    * This test helps to find which country sold the most expensive ps4 in 2016.
    */

  test("Which country sold the costliest PS4 in 2016") { timed("test6",{
    Assignment2.Problem6(tecinDF) must equal ( "[Angola]")}
  )
  }
  /**
    * this test would help us find if the cost of macbook has changed since 2016.
    * result would be a boolean
    */
  test("has the MacBook cost increased since the 2016?")
  { timed("test7",{
    val result = Assignment2.Problem7(novDF,tecinDF)
    result must equal(false)})

  }





}
