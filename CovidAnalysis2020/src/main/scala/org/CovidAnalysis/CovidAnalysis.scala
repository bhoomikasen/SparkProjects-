package org.CovidAnalysis

import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.functions.{max, sum}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructType}

import scala.io.Source

object CovidAnalysis {

  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.spark-project").setLevel(Level.WARN)
  val logger = LogManager.getLogger(this.getClass.getName)

  //reading connections urls  from prop file
  Source.fromFile("C:\\Users\\Asus\\IdeaProjects\\CovidAnalysis2020\\src\\main\\scala\\org\\CovidAnalysis\\system.properties").getLines()
  var configMap=Source.fromFile("C:\\Users\\Asus\\IdeaProjects\\CovidAnalysis2020\\src\\main\\scala\\org\\CovidAnalysis\\system.properties").getLines()
    .filter(line => line.contains("=")).map{ line => val tkns=line.split("=")
    if(tkns.size==1){
      (tkns(0) -> "" )
    }else{
      (tkns(0) ->  tkns(1))
    }
  }.toMap
  println(configMap("url"))


  def main(args:Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Covid Analysis")
      .config("hive.metastore.uris", "jdbc:mysql://localhost/metastore")
      .master("local[1]")
      .getOrCreate()
    logger.info("spark Session created............")
    try {
      val schema = new StructType().add("Date", StringType, true)
        .add("Country", StringType, true)
        .add("Confirmed", IntegerType, true)
        .add("Recovered", IntegerType, true)
        .add("Deaths", IntegerType, true)

//reading hdfs data
      val DF = spark.read.option("header", "true").schema(schema).csv(configMap("hdfsurl"))//.show()
      logger.info("data loaded to dataframe...............")
      val Confirmedcases = DF.groupBy("Country").agg(max("Date").as("TillDate"),max("Confirmed").as("Confirmed"),max("Recovered").as("Recovered"),max("Deaths").as("Deaths"))//.show()



//connecting to mysql
/*      val jdbcDF = spark.read
  .format("jdbc")
  .option("url","jdbc:mysql://localhost/emp")
  .option("dbtable", "(create table temp(id int,Tname char(10)) as t)")
  .option("user", "root")
  .option("password", "bhoomi38")
  .load()
      jdbcDF.show()*/
      //val table=jdbcDF.registerTempTable("orders")

//writing data back to mysql table
     val prop=new java.util.Properties()
      prop.put("user",configMap("userName"))
      prop.put("password",configMap("password"))
      logger.info("connected to mysql.................")
      val SaveToMysql=Confirmedcases.write.mode(SaveMode.Overwrite).jdbc(configMap("url"),"CovidAnalysis.covidanalysis2019",prop)
      logger.info("data saved to mysql table--------------")

    }

    catch{
      case e:Exception=>val errorMsg=e.getMessage
        println("failed------"+errorMsg)


    }
    spark.stop()
  }

}
