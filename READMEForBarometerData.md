Assumptions:
1.  ENVIRONMENT used for developing the code is Azure DataBricks Community Edition
2.  Length of the input is Fixed for different years formats.
3.  For good an efficient storage decided to use Parquet format
4.  Double precision to have data integrity/consistency across all years data because different years reading are having different precisions. Handled for few columns data.
5.  No partition by is used because the number of rows are 365 in a year
6.  All the temperature readings are in degC, so kept as it is
7.  Air pressure is unified and stored in hpa (other units found are mmhg and swedish inches)
8. Used local File System in Databricks community Edition(Spark doesn't differentiate the functionality based on the File System, as we know spark is a processing Engine - doesn't have storage, so supports other File Systems). To place the files in HDFS, we can use hadoop fs -copyFromLocal <<source path>> <<Destination path>>.
9. Not included correction period of 1996 to 2000 period

Improvements further can be made:
1. Depending on the use case/Data scintist need, we can do PartitionBy for yearly consumption
2. Refinement of folder structure can be done based on the consumption.
3. Whatever functions are reusable can be made UDF and register, we can use in other scala notebooks
4. Unification can be done for all the years of data in one method
5. Dynamic Schema construction
6. Folders hard coded, which can be handled in programmatic way
7. Data coming in both manual and automated, we can add column to differentiate
8. Can be refined to work in on-prem
9. Unification of formatting can be done if needed for other columns
