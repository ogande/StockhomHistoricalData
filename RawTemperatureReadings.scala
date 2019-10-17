// Databricks notebook source
// DBTITLE 1,Imports
//Imports required for this Notebook
import sys.process._
import java.net.URL
import java.io.File
import org.apache.log4j._
import org.apache.spark.sql.functions.{col, lit}

// COMMAND ----------

// DBTITLE 1,Logger for logging
// Description: Defining an object i.e. Singleton for logging the messages, implements Serializable to work in Distributed environment
object scalaLogger extends Serializable { 
   @transient lazy val log = Logger.getLogger(getClass.getName) //Getting log instance for logging the activities, used during debugging
}

// COMMAND ----------

// DBTITLE 1,Reusable Methods
// Description: Method to get the name of the method where an exception is occured
// Parameters: None
// Return type: String
def getMethodName:String = new Exception().getStackTrace().apply(1).getMethodName()

// Description: Downloads the file with the given URL
// Parameters: path to be downloaded of String type
// Return type: Any - As it is having an exception hanlding, the super type of all the types is Any
def downloadFromGivenURL(urlToBeDownloaded:String) = {
  try{
    scalaLogger.log.info("Going to download the data from the given URL")
    new URL(urlToBeDownloaded) #> new File(urlToBeDownloaded.split("/").last) !!
  }
  catch{
    case e:Exception => println("Exception occured in the method... " +  getMethodName)
                       scalaLogger.log.error("failed to download from the given URL")
  }
}

// Description: DoublePrecision method to have two digits after the decimal point
// Parameters: in Double
// Return type: String
def doublePrecision(in:Double):String = f"$in%2.2f"

//Description : Unifies the columns that are required for the given table
//Parameters: column names of the given table in String and also List of total columns that we are expecting as String
//Return type: List[org.apache.spark.sql.Column], which may have value if the column already exists or null
def unifiedColumns(availableColumns:List[String],totalColumns:List[String]) = {
  totalColumns.map(clm => clm match{
    case c if availableColumns.contains(clm) => col(c)
    case _ => lit(null).as(clm)
  })
}

// COMMAND ----------

// DBTITLE 1,Downloading the required files
// Downloads the historical raw data for the given source
downloadFromGivenURL("https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/air_pressure/raw/stockholm_barometer_1756_1858.txt")
downloadFromGivenURL("https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/air_pressure/raw/stockholm_barometer_1859_1861.txt")
downloadFromGivenURL("https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/air_pressure/raw/stockholm_barometer_1862_1937.txt")
downloadFromGivenURL("https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/air_pressure/raw/stockholm_barometer_1938_1960.txt")
downloadFromGivenURL("https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/air_pressure/raw/stockholm_barometer_1961_2012.txt")
downloadFromGivenURL("https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/air_pressure/raw/stockholm_barometer_2013_2017.txt")
downloadFromGivenURL("https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/air_pressure/raw/stockholmA_barometer_2013_2017.txt")

// COMMAND ----------

// Defining the case classes based on the input formats, which will be used in DataFrame creation
case class Format1756To1858(year:Integer, month:Integer, day:Integer,morning:String, noon:String,evening:String)
case class Format1859To1960(year:Integer, month:Integer, day:Integer, morning:String, noon:String, evening:String, tmin:String,tmax:String)
case class Format1961To2017(year:Integer, month:Integer, day:Integer, morning:String, noon:String, evening:String, tmin:String,tmax:String,estimated_diurnal_mean:String)

// COMMAND ----------

// Description: Generation of AVRO file for 1756 to 1858 temperature raw data
// Parameters: path of the file of String type
// Return type: Unit - nothing but void, in scala it is Unit
def generateAVROFileFor1756To1858(inputPath:String) = {
  try{
    val inputRDD = sc.textFile(inputPath) //OP Creating RDD with the given input path, used sc.textFile instead of spark.read.text, which creates DF
    if(inputRDD.first.length == 34){
        val data1756To1858 = inputRDD.map(line => Format1756To1858(line.substring(1,5).trim.toInt, line.substring(7,9).trim.toInt,line.substring(11,13).trim.toInt,line.substring(15,21).trim,line.substring(22,27).trim,line.substring(29,34).trim)).toDF
        data1756To1858.coalesce(1).write.format("avro").mode(SaveMode.Append).save("file:/databricks/driver/TemperatureData/1756_1858/")
      display(data1756To1858)
      }
    else
    {
      scalaLogger.log.info("Going to download the data from the given URL")
       println("hello1")
    }
  }catch {
    case e:Exception => scalaLogger.log.error("error occured while processing 1756 to 1858 file")
  }
}
//generateAVROFileFor1756To1858("file:/databricks/driver/stockholm_daily_temp_obs_1756_1858_t1t21t3.txt")

// COMMAND ----------

// Description: Generation of AVRO file for 1859 to 1960 temperature raw data
// Parameters: path of the file of String type
// Return type: Unit - nothing but void, in scala it is Unit
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

// Description: Generation of AVRO file for 1961 to 2017 temperature raw data
// Parameters: path of the file of String type
// Return type: Unit - nothing but void, in scala it is Unit
def generateAVROFileFor1961To2017(inputPath:String) = {
  try{
    val inputRDD1 = sc.textFile(inputPath) //Creating RDD with the given input path
    if(inputRDD1.first.length >= 46){ // Have hard code to consider 3 different sources with 46 and 47 length, needs to be improved
       val data1961To2017 =  inputRDD1.map(t =>Format1961To2017(t.substring(0,4).trim.toInt, t.substring(5,8).trim.toInt, t.substring(8,11).trim.toInt,doublePrecision(t.substring(11,17).trim.toDouble),doublePrecision(t.substring(17,22).trim.toDouble),doublePrecision(t.substring(22,29).trim.toDouble),doublePrecision(t.substring(29,34).trim.toDouble),doublePrecision(t.substring(34,40).trim.toDouble),doublePrecision(t.substring(40,t.length).trim.toDouble))).toDF
      data1961To2017.coalesce(1).write.format("avro").mode(SaveMode.Append).save("file:/databricks/driver/TemperatureData/1961_TillDate/")
      //println("count is: "+ data1961To2017.count)
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

// Generating the AVRO files required for the temperature data
generateAVROFileFor1756To1858("file:/databricks/driver/stockholm_daily_temp_obs_1756_1858_t1t2t3.txt")
generateAVROFileFor1859To1960("file:/databricks/driver/stockholm_daily_temp_obs_1859_1960_t1t2t3txtn.txt")
generateAVROFileFor1961To2017("file:/databricks/driver/stockholm_daily_temp_obs_1961_2012_t1t2t3txtntm.txt")
generateAVROFileFor1961To2017("file:/databricks/driver/stockholm_daily_temp_obs_2013_2017_t1t2t3txtntm.txt")
generateAVROFileFor1961To2017("file:/databricks/driver/stockholmA_daily_temp_obs_2013_2017_t1t2t3txtntm.txt")

// COMMAND ----------

// Unifiying the temperature data and storing in AVRO format for consistency across the data for consumption by Data Scientists or other users
val temperatureData1756To1858 = spark.read.format("avro").load("file:/databricks/driver/TemperatureData/1756_1858")
val temperatureData1859To1960 = spark.read.format("avro").load("file:/databricks/driver/TemperatureData/1859_1960")
val temperatureData1961_TillDate  = spark.read.format("avro").load("file:/databricks/driver/TemperatureData/1961_TillDate")

val cols1756To1858 = temperatureData1756To1858.columns.toList
val cols1859To1960 = temperatureData1859To1960.columns.toList
val cols1961_TillDate = temperatureData1961_TillDate.columns.toList
val requiredColumns = cols1756To1858 ++ cols1859To1960 ++ cols1961_TillDate distinct //Getting required columns by removing the duplicates using distinct

val resultantDF = temperatureData1756To1858.select(unifiedColumns(cols1756To1858, requiredColumns):_*).union(temperatureData1859To1960.select(unifiedColumns(cols1859To1960, requiredColumns):_*).union(temperatureData1961_TillDate.select(unifiedColumns(cols1961_TillDate, requiredColumns):_*)))
resultantDF.coalesce(1).write.format("avro").mode(SaveMode.Append).save("file:/databricks/driver/ConsolidatedDataInUnifiedForm/")
//resultantDF.select(col("year")).distinct.sort($"year".desc).show

// COMMAND ----------

downloadFromGivenURL("https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/temperature/daily/raw/stockholm_daily_temp_obs_1756_1858_t1t2t3.txt")
downloadFromGivenURL("https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/temperature/daily/raw/stockholm_daily_temp_obs_1859_1960_t1t2t3txtn.txt")
downloadFromGivenURL("https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/temperature/daily/raw/stockholm_daily_temp_obs_1961_2012_t1t2t3txtntm.txt")
downloadFromGivenURL("https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/temperature/daily/raw/stockholm_daily_temp_obs_2013_2017_t1t2t3txtntm.txt")
downloadFromGivenURL("https://bolin.su.se/data/stockholm/files/stockholm-historical-weather-observations-2017/temperature/daily/raw/stockholmA_daily_temp_obs_2013_2017_t1t2t3txtntm.txt")

// COMMAND ----------

import sys.process._
"ls -l /databricks/driver/".! //ConsolidatedDataInUnifiedForm/".!// 1961_TillDate".!
