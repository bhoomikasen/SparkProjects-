package org.SparkProj
import org.SparkProj.AmazonReviewAnalysis.logger
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.expressions.Window.orderBy
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.yetus.audience.InterfaceAudience.Public

import scala.io.Source

trait SparkJOb{
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.spark-project").setLevel(Level.WARN)
  val logger = LogManager.getLogger(this.getClass.getName)
  val spark = SparkSession.builder().appName("Amazon Review Analysis")
    .config("hive.metastore.uris", "thrift://localhost:9083")
    .enableHiveSupport()
    .master("local[*]")
    .getOrCreate()
  logger.info("SparkSession created...........")
}

object AmazonReviewAnalysis extends App with SparkJOb { //extending App, so that the body of object becomes your main function. "args" is a supplied value, you can just use it.

  import spark.implicits._

  val dataBase = args(0)
  val mysqlDB = args(0)

  //reading property file
  Source.fromFile("C:\\Users\\Asus\\IdeaProjects\\AmazonReviewAnalysis\\src\\main\\resources\\system.properties").getLines()
  var configMap = Source.fromFile("C:\\Users\\Asus\\IdeaProjects\\AmazonReviewAnalysis\\src\\main\\resources\\system.properties").getLines()
    .filter(line => line.contains("=")).map { line =>
    val tkns = line.split("=")
    if (tkns.size == 1) {
      (tkns(0) -> "")
    } else {
      (tkns(0) -> tkns(1))
    }
  }.toMap

  try {
    //reading hive table
    val amazonreviewsDF = spark.read.table(dataBase + ".amazonreviews").drop("id", "asins", "keys") //.cache()//.show(3)
    // amazonreviewsDF.select("reviews_rating","reviews_text").show(false)

    //Most Reviewed product
    val MostReviewed = amazonreviewsDF.select("name") //joining main table and MostReviewd to get required columns to be selected
      .groupBy("name").count()
    val joinDF = MostReviewed.join(amazonreviewsDF, "name")
      .select(col("name"),
        col("primarycategories"),
        col("manufacturer"),
        col("brand"),
        col("count"),
        col("reviews_date"),
        col("reviews_username"),
        col("reviews_rating"),
        lower(col("reviews_title")).as("reviews_title"),
        lower(col("reviews_text")).as("reviews_text"))
      .distinct()
      .orderBy(desc("count")) //.show()
    saveToMysql(joinDF, mysqlDB + ".MostReviewed")

    //sentiment Analysis
    val windowSpec1 = Window.partitionBy("name").orderBy("reviews_rating")
    val sentiments = joinDF.withColumn("sentiment", when(col("reviews_rating") === 1 or col("reviews_rating") === 2, "negative")
      .when(col("reviews_rating") === 4 or col("reviews_rating") === 5, "positive").otherwise("Neutral")).cache() //assigning sentiment value based on rating

    //text cleaning
    textClean(sentiments, "negative")

    //What are the initial and current number of customer reviews for each product?
    val initial_ReviewsDate = joinDF.withColumn("reviews_date1", to_date(to_timestamp(col("reviews_date")), "MM/dd/yyyy"))
      .select("*").groupBy("name", "reviews_date1")
      .count().orderBy("name", "reviews_date1")
    val windowSpec = Window.partitionBy("name").orderBy("reviews_date1")
    val totalInitialFinalReviews = initial_ReviewsDate.withColumn("total_reviews", sum("count").over(windowSpec)) //.show()
    totalInitialFinalReviews.createOrReplaceTempView("totalInitialFinalReviews")
    val FinalInitialFinalRewviews = spark.sql("SELECT name,min(reviews_date1) as FirstRevDate" +
      ",max(reviews_date1) as lastRevdate," +
      "min(total_reviews) as InitialRevCount," +
      "max(total_reviews) as FinalRevCount  FROM totalInitialFinalReviews " +
      "group by name order by FinalRevCount desc")
    saveToMysql(FinalInitialFinalRewviews, mysqlDB + ".AmazonreviewAnalysis")
    logger.info("Stopping SparkSession...........")
    spark.stop()
  } //End of try
  catch {
    case e: Exception =>
      val error_msg = e.getMessage
      println("[FAILED]:" + error_msg)
  }


  def saveToMysql(DF: DataFrame, table: String): Unit = {
    val prop = new java.util.Properties()
    prop.put("user", configMap("userName"))
    prop.put("password", configMap("password"))
    DF.write.mode("Overwrite").jdbc(configMap("mysqlurl"), s"""${table}""", prop)
    println("data saved to MYSQL............")
  }

  def percentage(DFname: String, givennumber: Long, TotalNumber: Long): Unit = {
    val totalPercentage = (givennumber.toFloat / TotalNumber.toFloat) //*100
    println(f" ${DFname} : ${givennumber} reviews ${totalPercentage * 100}%.0f%%")
  }

  def lower_clean_str(x: Column) = {
    val punc = """[\p{Punct}&&[^.]]"""
    var lowercased_str = x.toString().toLowerCase()
    for (ch <- 0 to lowercased_str.length - 1) {
      lowercased_str
      = lowercased_str.replace(punc(ch), ' ')
    }
  }

  def textClean(sentimentsDF: DataFrame, sentimentVal: String): Unit = {
    val removePuctuations = sentimentsDF.select("reviews_text", "sentiment")
      .withColumn("reviews_text", explode(split(regexp_replace(col("reviews_text"), "[,.!?:;*()\"\'/~?$]", ""), " ")))
    val wordCountOfSentiment = removePuctuations.filter(col("sentiment") === sentimentVal)
      .groupBy("reviews_text").count() //.show()  replacing punctuation and extra other marks
    val stop_words = List("a", "an", "at", "the", "this", "that", "is", "it", "to", "and", "i", "for", "of", "as", "in", "on", "have", "so", "these", "them", "has", "we", "had", "if", "they", "my", "with", "you", "not", "was", "its", "are", "can", "be", "were", "would", "will") //stop words to be removed
    val removeStopWords = wordCountOfSentiment.select("reviews_text").map(f => f.getString(0))
      .collect.filter(!stop_words.contains(_)).toSeq //Filtering out stop words from DF anf converting it to Seq
    val finalremoveStopWords = removeStopWords.toDF("words")
    val finalDF = finalremoveStopWords.join(wordCountOfSentiment, wordCountOfSentiment("reviews_text") === finalremoveStopWords("words"))
      .select("words", "count")
      .orderBy(desc("count")) //joinig finalremoveStopWords with original DF to get the counts of words
    if (sentimentVal == "positive") {
      saveToMysql(finalDF, mysqlDB + ".postiveWords")
    }
    else {
      saveToMysql(finalDF, mysqlDB + ".negativeWords")
    }
  }
}
