# StockhomHistoricalWeatherObservationsData

Assumptions:
1.  ENVIRONMENT used for developing the code is Azure DataBricks Community Edition
2.  Length of the input is Fixed for different years formats.
3.  For good schema evolution and efficient storage we will be using AVRO, a row major file format (we don't have mulitple/nested values in any of the columns).
4.  Double precision to have data integrity/consistency across all years data because different years reading are having different precisions. 
5.  No partition by is used because the number of rows are 365 in a year
6.  All the temperature readings are in degC, so kept as it is
7.  Used local File System in Databricks community Edition(Spark doesn't differentiate the functionality based on the File System, as we know spark is a processing Engine - doesn't have storage, so supports other File Systems). To place the files in HDFS, we can use hadoop fs -copyFromLocal <<source path>> <<Destination path>>.

Improvements further can be made:
1. Depending on the use case/Data scintist need, we can do PartitionBy for yearly consumption
2. Refinement of folder structure can be done based on the consumption.
3. Whatever functions are reusable can be made UDF and register, we can use in other scala notebooks
4. Unification can be done for all the years of data in one method

Generated data:
1. Each individual temperature readings have been stored in the AVRO format
2. All the available historical data is stored in one AVRO file, the columns which are not available are having null 
