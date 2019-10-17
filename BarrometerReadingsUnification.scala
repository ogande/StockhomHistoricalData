// Databricks notebook source
// 1756_1858
// Case class for the Barometer Data Schema
case class BarometerDataSchema(year:String, month:String, day:String, mBaro:String, mBaroTemp:String, nBaro:String, nBaroTemp:String, eBaro:String, eBaroTemp:String)

val inRDD = sc.textFile("file:/databricks/driver/stockholm_barometer_1756_1858.txt")
val dfFor1756To1858 = inRDD.map{
  line => BarometerDataSchema(line.substring(0,4).trim, line.substring(4,8).trim,line.substring(8,11).trim,line.substring(11,18).trim, line.substring(18,25).trim, line.substring(25,32).trim, line.substring(32,39).trim, line.substring(39,46).trim, line.substring(46,line.length).trim)
}.toDF
dfFor1756To1858.repartition(1).write.format("parquet").option("header", "true").option("delimiter","\t").mode(SaveMode.Append).save("file:/databricks/driver/BarometerData/1756_1858")


// COMMAND ----------

// Defining case class for Schema from 1859 to 1861
case class BarometerDataSchema1859To1861(year:String, month:String, day:String, mBaro:String, mBaroTemp:String, mAirPressure:String, nBaro:String, nBaroTemp:String,nAirPressure:String, eBaro:String, eBaroTemp:String,eAirPressure:String)

def doublePrecision(in:Double):String = f"$in%2.2f"

//Defining a class for the inches conversion to hpa 
class toHpa(data:String) {
  def convert = doublePrecision((data.toDouble/10.toDouble)*33.86389.toDouble)
}

// Description: implicit method for converstion of the inch from 0.1* mm into inches
// Parameters: String
// Return Type: String
implicit def swedishInchtoHpaConversion(si:String) = new toHpa(si)

val input18591861 = sc.textFile("file:/databricks/driver/stockholm_barometer_1859_1861.txt")
val dfFor18591861 = input18591861.map{
  line => BarometerDataSchema1859To1861(line.substring(0,4).trim, line.substring(4,8).trim,line.substring(8,11).trim,line.substring(11,19).trim.convert, line.substring(19,25).trim, line.substring(25,32).trim.convert, line.substring(32,40).trim.convert, line.substring(40,46).trim, line.substring(46,53).trim.convert,line.substring(53,61).trim.convert, line.substring(61,67).trim, line.substring(67,73).trim.convert)
}.toDF

dfFor18591861.repartition(1).write.format("parquet").option("header", "true").option("delimiter","\t").mode(SaveMode.Append).save("file:/databricks/driver/BarometerData/1859_1861")

//display(dfFor18591861.filter($"year" === 1859))

// COMMAND ----------

//1862_1937
// Implicit class for implicit conversion of mmHg to Inches
// Parameters: mmhg of Double type
implicit class mmHgTohpa(mmhg:Double){
  
  //Defining the two decimal format for the consistency of the numbers
  val twoDecimalFormat = new java.text.DecimalFormat("0.00")
  twoDecimalFormat.setRoundingMode(java.math.RoundingMode.UP) //Setting to Rouning up so that we will get much accurate result
  
  //Defining mmHgToHpa converter using implicits
  def mmHgToHpa = twoDecimalFormat.format(mmhg*1.33322).toString
}

//Schema for the 1862 to 1937 input data
case class BarometerDataSchemaForAirPressure(year:String, month:String, day:String, mAirPressure:String,nAirPressure:String, eAirPressure:String)

val input18621937 = sc.textFile("file:/databricks/driver/stockholm_barometer_1862_1937.txt")
val dfFor18621937 = input18621937.map(record => BarometerDataSchemaForAirPressure(record.substring(0,6).trim, record.substring(6,10).trim, record.substring(10,14).trim, record.substring(21,28).trim.toDouble.mmHgToHpa, record.substring(21,28).trim.toDouble.mmHgToHpa, record.substring(28,34).trim.toDouble.mmHgToHpa)).toDF
dfFor18621937.repartition(1).write.format("parquet").option("header", "true").option("delimiter","\t").mode(SaveMode.Append).save("file:/databricks/driver/BarometerData/1862_1937/")
//display(dfFor18621937)

// COMMAND ----------

// 1938 to 1960 Barometer data
val inputRDD = sc.textFile("file:/databricks/driver/stockholm_barometer_1938_1960.txt")

val reqDF = inputRDD.map(entry => BarometerDataSchemaForAirPressure(entry.substring(0,6).trim,entry.substring(6,10).trim, entry.substring(10,14).trim, entry.substring(14,22).trim, entry.substring(22,30).trim, entry.substring(30,37).trim)).toDF
reqDF.repartition(1).write.format("parquet").option("header", "true").option("delimiter","\t").mode(SaveMode.Append).save("file:/databricks/driver/BarometerData/1938_1960/")
//display(reqDF)

// COMMAND ----------

// This method can be used for 1961 to 2017 only air pressure readings..
// Parameters: path as String
// Return type: Unit
def barometerDataOnlyAirPressure(path:String) = {
  val inputPath = sc.textFile(path)
  val requiredDF = inputPath.map(entry =>BarometerDataSchemaForAirPressure(entry.substring(0,5).trim, entry.substring(5,8).trim, entry.substring(8,11).trim, doublePrecision(entry.substring(11,18).trim.toDouble), doublePrecision(entry.substring(18,25).trim.toDouble), doublePrecision(entry.substring(25,31).trim.toDouble))).toDF
  //display(requiredDF)
  val destinationPath = "file:/databricks/driver/BarometerData/"+ path.split("/").last.dropRight(4).takeRight(9)

  requiredDF.repartition(1).write.format("parquet").option("header", "true").option("delimiter","\t").mode(SaveMode.Append).save(destinationPath)
}

barometerDataOnlyAirPressure("file:/databricks/driver/stockholm_barometer_1961_2012.txt")
barometerDataOnlyAirPressure("file:/databricks/driver/stockholm_barometer_2013_2017.txt")
barometerDataOnlyAirPressure("file:/databricks/driver/stockholmA_barometer_2013_2017.txt")
