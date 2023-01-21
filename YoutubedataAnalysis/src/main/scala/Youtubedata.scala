import org.apache.commons.httpclient.URI
import org.apache.directory.api.util.DateUtils.getDate
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

import java.text.SimpleDateFormat
import java.util.Date
import scala.io.Source
import scala.reflect.io.File
import sys.process._
import scala.xml.Properties

case class SparkProperties(spark:SparkSession)

object Youtubedata {
  val rootLogger = Logger.getRootLogger()
  rootLogger.setLevel(Level.ERROR)
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
  Logger.getLogger("org.spark-project").setLevel(Level.WARN)
  val logger = LogManager.getLogger(this.getClass.getName)

  var sparkConfig:SparkProperties= _
  var dateFormatter=new SimpleDateFormat("yyyyMMdd")
  var dateFormatter2=new SimpleDateFormat("yyyy")
  var submittedDateConvert=new Date()
  var execution_date=dateFormatter.format(submittedDateConvert)
  var execution_year=dateFormatter2.format(submittedDateConvert)
  println(execution_date)

  //reading property file
  Source.fromFile("C:\\Users\\Asus\\IdeaProjects\\YoutubedataAnalysis\\src\\property\\system.properties").getLines()
  var configMap=Source.fromFile("C:\\Users\\Asus\\IdeaProjects\\YoutubedataAnalysis\\src\\property\\system.properties").getLines()
    .filter(line => line.contains("=")).map{ line => val tkns=line.split("=")
    if(tkns.size==1){
      (tkns(0) -> "" )
    }else{
      (tkns(0) ->  tkns(1))
    }
  }.toMap

  def main(args:Array[String]): Unit ={
    try {
      val spark = SparkSession.builder()
        .appName("Youtube data analysis")
        //.config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse")
        //.config("spark.sql.hive.hiveserver2.jdbc.url",configMap("hiveServer2url"))
        .config("hive.metastore.uris", configMap("hivemetaStoreurl"))
        .master("local")
        .enableHiveSupport()
        .getOrCreate()
      logger.info("SparkSession Created................")
    sparkConfig=SparkProperties(spark)
      import spark.implicits._

      logger.info("Creating DF from  table youtubedb.invideos...........")
      val table = spark.read.table(configMap("Database")+".invideos") //whole data from csv
      val fileHeader = table.head.getString(0) //get first col 1 of row 1 from table
      val dfRealData = table.filter(col(table.columns(0)) =!= fileHeader) //workaround to remove header from first row

      logger.info("Reading data from .json data")
      val category_id_temp = spark.read //.option("mode", "DROPMALFORMED")
        .json(configMap("hdfsurl"))//data from json to get id etc

      // category_id_temp.printSchema()
      val category_id_temp2 = category_id_temp.withColumn("items2", explode(category_id_temp.col("items"))).drop("items") //convert array to row
      val category_id_temp3 = category_id_temp2.select("etag", "kind", "items2.*") //fetch struct colmns
      val category_id_temp4 = category_id_temp3.select("id", "snippet.*")
      val category_id_temp4Renamecol = category_id_temp4.withColumnRenamed("title", "Category_Title").drop("assignable")

      //joining the csv and json datas based on category_id
      logger.info("joining the csv and json datas based on category_id")
      val joinedDF = dfRealData.join(category_id_temp4Renamecol, dfRealData("category_id") === category_id_temp4Renamecol("id")).orderBy(col("views").desc)
      logger.info("Dataframe top 10 most viewed videos")
      val top10rated = joinedDF.orderBy($"likes".desc)

      // Find out the top 5 categories in which the most number of videos are uploaded.
      logger.info("dataframe for top 5 categories in which the most number of videos are uploaded.")
      val top5UploadsCategory = top10rated.groupBy("Category_Title").count().orderBy($"count".desc)//.cache()

      //Find out top 10 videos in each category
      logger.info("dataframe for top 10 videos in each category")
      val windowSpec = Window.partitionBy("Category_Title").orderBy(col("views").desc)
      val top10inEachCategory = joinedDF.withColumn("rownum", row_number.over(windowSpec)).where($"rownum" <= 10)
      //logger.info("saving top10inEachCategory data to hive table..............")
     // top10inEachCategory.write.mode("overwrite").saveAsTable("youtubedb.top10inEachCategory")
      //logger.info("saving data to other table as text format")
      //createTableAsSelect("youtubedb","youtubedb","top10inEachCategory_text","top10inEachCategory","/user/hive/warehouse/youtubedb.db")


      logger.info("Loading data to Mysql.......")
      saveToMysql(top10rated,configMap("Database")+".top10rated")
      saveToMysql(top5UploadsCategory,configMap("Database")+".top5UploadsCategory")
      saveToMysql(top10inEachCategory,configMap("Database")+".top10inEachCategory")
      logger.info("Data Exported to mysql.....")

      logger.info("Validating data..........")
      loadMysqlTable(top10rated,configMap("Database")+".top10rated")
      loadMysqlTable(top5UploadsCategory,configMap("Database")+".top5UploadsCategory")
      loadMysqlTable(top10inEachCategory,configMap("Database")+".top10inEachCategory")
      logger.info("validation Completed...")

      logger.info("Updating Audit Table for Success......")
      executeQuery("insert into "+configMap("Database")+".Audit select "+execution_date.toInt+",'Sucess','NA',"+execution_year.toInt)
      logger.info("stopping sparksession....")

      spark.stop()
    }
    catch{
      case e: Exception=>
        val error_msg=e.getMessage
        println("[FAILED]:"+error_msg)

        val technicalReason=error_msg.replace(''',' ').replaceAll("\n"," ")

        logger.info("Updating Audit table for Failure........")
        executeQuery("insert into "+configMap("Database")+".+Audit select "+execution_date.toInt+",'Failed','"+technicalReason+"',"+execution_year.toInt)
        logger.info("Data Saved to Audit table.......")
    }

  def saveToMysql(DF:DataFrame,table:String): Unit ={
    val prop = new java.util.Properties()
    prop.put("user", configMap("userName"))
    prop.put("password", configMap("password"))
    DF.write.mode("Overwrite").jdbc(configMap("mysqlurl"), s"""${table}""", prop)
  }

  def createTableAsSelect(destDbName:String,likeDbName:String,desTableName:String,likeTableName:String,dbLocation:String): Unit ={
    logger.info("create table :"+likeDbName+":"+likeTableName+":"+destDbName+":"+desTableName)
    //sparkConfig.spark.sql("create table if not exists "+destDbName+"."+desTableName+" stored as textfile as select * from "+likeDbName+"."+likeTableName+" location '"+dbLocation+"'")
    executeQuery("create table if not exists "+destDbName+"."+desTableName+" stored as textfile as select * from "+likeDbName+"."+likeTableName)
    logger.info("createTableAsSelect() ends....")
    ()
  }

  def executeQuery(query:String): Unit ={
    logger.info("executing query...."+query)
    sparkConfig.spark.sql(query)
  }

  def loadMysqlTable(DF:DataFrame,tablename:String): Unit = {
    logger.info("Validating count from mysql table..............")
    val tblDf=sparkConfig.spark.read.format("jdbc").option("url", configMap("mysqlurl"))
      .option("dbtable", s"""${tablename}""")
      .option("user", configMap("userName"))
      .option("password", configMap("password"))
      .option("dbtable", "(select count(*) from "+s"""${tablename}"""+") as T").load()
    var countTblDF=tblDf.collect()(0).getLong(0)
    var DFcount=DF.count() //.toInt)
    if(countTblDF==DFcount){
      println("count is matching form Dataframe "+ DF +"="+DFcount+" count from mysql table "+tablename+" = "+countTblDF)
    }
    else {
      println("error..........."+"count mismatch Dataframe "+ DF +"="+DFcount+" count from mysql table "+tablename+" = "+countTblDF)
    }
  }
  }
}
