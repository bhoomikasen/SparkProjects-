import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, date_format, desc, format_number, round, to_date, to_timestamp, year}

object WalmartStocks {
  def main(args:Array[String]): Unit ={
    val spark=SparkSession.builder().appName("Walmart Stocks Analysis")
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")// to restore the behavior before Spark 3.
      .master("local").getOrCreate()

    val walmartStocks=spark.read
      .option("inferSchema","true")
      .option("header","true")
      .csv("C:\\data\\Walmart.csv").cache()
      walmartStocks.show()
    //walmartStocks.printSchema()
   // val descWalmart=walmartStocks.describe()//.show()


    // Round the "value" column to two decimal places
    /*    There are too many decimal places for mean and stddev in the describe() dataframe. Format the
    numbers to just show up to two decimal places. Pay careful attention to the datatypes that
    .describe() returns,*/
    // val df=descWalmart.select(col("summary"),format_number(col("Open").cast("float"),2)).show()

    /*Create a new dataframe with a column called HV Ratio that is the ratio of the High Price versus
      volume of stock traded for a day.*/
    val hvRatio=walmartStocks.withColumn("HV_ration",walmartStocks("High")/walmartStocks("Volume"))//.show()

    /*What day had the Peak High in Price?*/
    val peakHighPrice=walmartStocks.orderBy(desc("High"))//.show()

   /* What is the max High per year?*/
    val maxHIghperYear=walmartStocks.withColumn("year",date_format(to_date(col("Date"),"MM/dd/yyyy"),"yyyy"))
      .groupBy("year").max("High")
      .orderBy(desc("max(High)"))//.show()

  /*  What is the average Close for each Calendar Month?*/
   val avgCloseperYear=walmartStocks.withColumn("month",date_format(to_date(col("Date"),"MM/dd/yyyy"),"M"))
    .groupBy("month").avg("close").show()



  }


}
