// Databricks notebook source
import sys.process._
import java.net.URL
import java.io.File
import org.apache.log4j._

// COMMAND ----------

object scalaLogger extends Serializable {
   @transient lazy val log = Logger.getLogger(getClass.getName) //Getting log instance for logging the activities, used during debugging
}

// COMMAND ----------

// REUSABLE METHODS
//Method to get the name of the method where an exception is occured
def getMethodName:String = new Exception().getStackTrace().apply(1).getMethodName()

//Downloads the file with the given URL
def downloadFromGivenURL(urlToBeDownloaded:String):Unit = {
  try{
    scalaLogger.log.info("Going to download the data from the given URL")
    new URL(urlToBeDownloaded) #> new File(urlToBeDownloaded.split("/").last) !!
  }
  catch{
    case e:Exception => println("Exception occured in the method... " +  getMethodName)
                       scalaLogger.log.error("failed to download from the given URL")
  }
}

// DoublePrecision method to have two digits after the decimal point
def doublePrecision(in:Double):String = f"$in%2.2f"

// COMMAND ----------

// Defining the case classes based on the input formats, which will be used in DataFrame creation
case class Format1756To1858(year:Integer, month:Integer, day:Integer,morning:String, noon:String,evening:String)
case class Format1859To1960(year:Integer, month:Integer, day:Integer, morning:String, noon:String, evening:String, tmin:String,tmax:String)
case class Format1961ToTill(year:Integer, month:Integer, day:Integer, morning:String, noon:String, evening:String, tmin:String,tmax:String,estimated_diurnal_mean:String)

// COMMAND ----------

downloadFromGivenURL("https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/temperature/daily/raw/stockholm_daily_temp_obs_1756_1858_t1t2t3.txt")
downloadFromGivenURL("https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/temperature/daily/raw/stockholm_daily_temp_obs_1859_1960_t1t2t3txtn.txt")
downloadFromGivenURL("https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/temperature/daily/raw/stockholm_daily_temp_obs_1961_2012_t1t2t3txtntm.txt")
downloadFromGivenURL("https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/temperature/daily/raw/stockholm_daily_temp_obs_2013_2017_t1t2t3txtntm.txt")
downloadFromGivenURL("https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/temperature/daily/raw/stockholmA_daily_temp_obs_2013_2017_t1t2t3txtntm.txt")

// COMMAND ----------

def generateAVROFileFor1756To1858(inputPath:String) = {
  try{
    val inputRDD = sc.textFile(inputPath) //Creating RDD with the given input path
    if(inputRDD.first.length == 34){
        val data1756To1858 = inputRDD.map(line => Format1756To1858(line.substring(1,5).trim.toInt, line.substring(7,9).trim.toInt,line.substring(11,13).trim.toInt,line.substring(15,21).trim,line.substring(22,27).trim,line.substring(29,34).trim)).toDF
        data1756To1858.coalesce(1).write.format("avro").mode(SaveMode.Append).save("file:/databricks/driver/TemperatureData/1756_1858/")
      }
    else
    {
      scalaLogger.log.info("Going to download the data from the given URL")
    }
  }catch {
    case e:Exception => scalaLogger.log.error("error occured while processing 1756 to 1858 file")
  }
}

// COMMAND ----------

def generateAVROFileFor1859To1960(inputPath:String) = {
  try{
    val inputRDD = sc.textFile(inputPath) //Creating RDD with the given input path
    if(inputRDD.first.length == 40){
       val data1859To1960 = inputRDD.map(reading =>Format1859To1960(reading.substring(0,4).trim.toInt, reading.substring(5,8).trim.toInt, reading.substring(8,11).trim.toInt,doublePrecision(reading.substring(11,17).trim.toDouble),doublePrecision(reading.substring(17,22).trim.toDouble),doublePrecision(reading.substring(22,29).trim.toDouble),doublePrecision(reading.substring(29,34).trim.toDouble),doublePrecision(reading.substring(34,40).trim.toDouble))).toDF
      println("count is: "+ data1859To1960.count)
      data1859To1960.coalesce(1).write.format("avro").mode(SaveMode.Append).save("file:/databricks/driver/TemperatureData/1859_1960/")
      }
    else
    {
      scalaLogger.log.info("Going to download the data from the given URL")
      //println("incorrect format")
    }
  }catch {
    case e:Exception => scalaLogger.log.error("error occured while processing 1859 to 1960 file")
     // println("error")
  }
}

// COMMAND ----------

def generateAVROFileFor1961ToTill(inputPath:String) = {
  try{
    val inputRDD1 = sc.textFile(inputPath) //Creating RDD with the given input path
    if(inputRDD1.first.length >= 46){ // Have hard code to consider 3 different sources with 46 and 47 length, needs to be improved
       val data1961ToTill =  inputRDD1.map(t =>Format1961ToTill(t.substring(0,4).trim.toInt, t.substring(5,8).trim.toInt, t.substring(8,11).trim.toInt,doublePrecision(t.substring(11,17).trim.toDouble),doublePrecision(t.substring(17,22).trim.toDouble),doublePrecision(t.substring(22,29).trim.toDouble),doublePrecision(t.substring(29,34).trim.toDouble),doublePrecision(t.substring(34,40).trim.toDouble),doublePrecision(t.substring(40,t.length).trim.toDouble))).toDF
      data1961ToTill.coalesce(1).write.format("avro").mode(SaveMode.Append).save("file:/databricks/driver/TemperatureData/1961_TillDate/")
      println("count is: "+ data1961ToTill.count)
      }
    else
    {
     scalaLogger.log.info("Going to download the data from the given URL")
      //println("incorrect format")
    }
  }catch {
    case e:Exception => scalaLogger.log.error("error occured while processing 1756 to 1858 file")
  }
}

// COMMAND ----------

generateAVROFileFor1756To1858("file:/databricks/driver/stockholm_daily_temp_obs_1756_1858_t1t2t3.txt")
generateAVROFileFor1859To1960("file:/databricks/driver/stockholm_daily_temp_obs_1859_1960_t1t2t3txtn.txt")
generateAVROFileFor1961ToTill("file:/databricks/driver/stockholm_daily_temp_obs_1961_2012_t1t2t3txtntm.txt")
generateAVROFileFor1961ToTill("file:/databricks/driver/stockholm_daily_temp_obs_2013_2017_t1t2t3txtntm.txt")
generateAVROFileFor1961ToTill("file:/databricks/driver/stockholmA_daily_temp_obs_2013_2017_t1t2t3txtntm.txt")

// COMMAND ----------

val temperatureData1756To1858 = spark.read.format("avro").load("file:/databricks/driver/TemperatureData/1756_1858")
val temperatureData1859To1960 = spark.read.format("avro").load("file:/databricks/driver/TemperatureData/1859_1960")
val temperatureData1961_TillDate  = spark.read.format("avro").load("file:/databricks/driver/TemperatureData/1961_TillDate")

val cols1756To1858 = temperatureData1756To1858.columns.toList
val cols1859To1960 = temperatureData1859To1960.columns.toList
val cols1961_TillDate = temperatureData1961_TillDate.columns.toList
val requiredColumns = cols1756To1858 ++ cols1859To1960 ++ cols1961_TillDate distinct

def exprs(cols:List[String],totalcols:List[String]) = {
  totalcols.map(x => x match{
    case x if cols.contains(x) => col(x)
    case _ => lit(null).as(x)
  })
}

val resultantDF = temperatureData1756To1858.select(exprs(cols1756To1858, requiredColumns):_*).union(temperatureData1859To1960.select(exprs(cols1859To1960, requiredColumns):_*).union(temperatureData1961_TillDate.select(exprs(cols1961_TillDate, requiredColumns):_*)))//.drop(col1:_*)
//resultantDF.select(col("year")).distinct.sort($"year".desc).show
resultantDF.coalesce(1).write.format("avro").mode(SaveMode.Append).save("file:/databricks/driver/ConsolidatedDataInUnifiedForm/")

// COMMAND ----------

import sys.process._
"ls -l /databricks/driver/ConsolidatedDataInUnifiedForm/".!// 1961_TillDate".!
