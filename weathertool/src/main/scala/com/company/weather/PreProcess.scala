package com.company.weather

import org.apache.hadoop.fs._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import scala.collection.mutable._
import scala.util.Try
import org.apache.spark.rdd.RDD

class PreProcess extends Serializable {

  /*
   *getting the configuration variables from the BO objects
   */
  BO.logger.info("setting the configuration variables")
  val inputPath = BO.json.config.input_path
  val outputPath = BO.json.config.output_path
  val stationList = BO.json.config.stations.toList.map(_.toLowerCase())
  var schema_list = new ArrayBuffer[String]()
  BO.json.config.schema.toList.foreach(s => {
    schema_list += s.cname + ":" + s.ctype
  })
  val column_schema = schema_list.toList
  BO.logger.info("inputPath : " + inputPath)
  BO.logger.info("outputPath : " + outputPath)
  BO.logger.info("stationList : " + stationList)
  BO.logger.info("column_schema : " + column_schema)

  /*
   *deriving same data type columns into single groups
   */
  def getColTypedGroups(): Try[(List[String], List[String], List[String])] = {
    var double_col = new ListBuffer[String]()
    var int_col = new ListBuffer[String]()
    var string_col = new ListBuffer[String]()
    column_schema.map(cols => {
      cols.split(":")(1) match {
        case "double" => double_col += cols.split(":")(0)
        case "int" => int_col += cols.split(":")(0)
        case "string" => string_col += cols.split(":")(0)
      }
    })
    BO.logger.info("double_col : " + double_col)
    BO.logger.info("int_col : " + int_col)
    BO.logger.info("string_col : " + string_col)
    Try((string_col.toList, int_col.toList, double_col.toList))
  }

  /*
   *casting all the columns for each of the same group data type columns to respective target type
   * e.g.: all integer columns are cast from string to integer, all double columns are cast from string to double
   */
  def castColumns(df: DataFrame, sourceType: DataType, targetType: DataType, ignoreCol: List[String]): Try[DataFrame] = {
    Try(df.schema.filter(_.dataType == sourceType).foldLeft(df) {
      case (acc, col) if ignoreCol contains col.name => acc.withColumn(col.name, df(col.name))
      case (acc, col) => acc.withColumn(col.name, df(col.name).cast(targetType))
    })
  }

  /*
   *deriving the schema of StructTypefor all the input columns
   *this is target schema of data to which we pre-process the raw data and store in parquet, as per the config.json schema mapping
   */
  def getStructSchema(): Try[StructType] = {
    Try(StructType(column_schema.map(cols => {
      cols.split(":")(1) match {
        case "string" => StructField(cols.split(":")(0), StringType, true)
        case "int" => StructField(cols.split(":")(0), IntegerType, true)
        case "long" => StructField(cols.split(":")(0), LongType, true)
        case "double" => StructField(cols.split(":")(0), DoubleType, true)
        case "timestamp" => StructField(cols.split(":")(0), TimestampType, true)
      }
    })))
  }

  /*
   *deriving the default schema of StructType for all the input columns
   *this is source schema of data, of which the dataframe is formed before writing pre-processed data
   */
  def getDefaultInStringSchema(): Try[StructType] = {
    Try(StructType(column_schema.map(cols => {
      StructField(cols.split(":")(0), StringType, true)
    })))
  }

  /*
   *prepend the station name at the beginning of all records in the input of each station
   *this is needed as we need a column with station name in each of the station input data
   */
  def prependStationName(line: String, station_name: String): Try[String] = {
    Try(station_name + " " + line)
  }

  /*
   *pre-processing the data for each station:-
   *  prepend station name
   *  remove first 6-7 metadata and comments like data lines
   *  remove special characters from all the records
   *  split the rows into columns by space, to get structured column data to form dataframe
   */
  def dataPreprocessing(file: String): Try[(RDD[Row], String)] = {
    var rdd = BO.spark.sparkContext.textFile(file)
    val station_name_from_file = rdd.first()
    val station_name_parts = file.split("\\/")
    val station_name = station_name_parts(station_name_parts.length - 1).split("\\.")(0)
    BO.logger.info("pre-processing file for station : " + station_name)
    Try(
      (
        rdd.filter(x => {
          x != station_name_from_file
        })
          .map(x => prependStationName(x, station_name))
          .filter(x => (!x.get.startsWith(station_name + " Location")))
          .filter(x => (!x.get.startsWith(station_name + " Estimated")))
          .filter(x => (!x.get.startsWith(station_name + " Missing")))
          .filter(x => (!x.get.startsWith(station_name + " Sunshine")))
          .filter(x => (!x.get.startsWith(station_name + "    yyyy  ")))
          .filter(x => (!x.get.startsWith(station_name + "               degC    ")))
          .filter(x => (!x.get.startsWith(station_name + " Site closed")))
          .map(x => x.get.replaceAll("-", "").replaceAll("provisional", "").replaceAll("\\#", "").replaceAll("\\*", ""))
          .map(line => line.split("\\s+"))
          .map(x => x.map(_.replace("---", "")))
          .filter(x => x.length == 8)
          .map(columns => Row(columns(0).trim(), columns(1).trim(), columns(2).trim(), columns(3).trim(), columns(4).trim(), columns(5).trim(), columns(6).trim(), columns(7).trim())),
        station_name
      )
    )
  }

  /*
   *pre-process the input data for each file
   *  read the input data as string data types
   *  cast each column to target type as per the config.json schema mapping
   *  writing the pre-processed data in parquet format to intermediate directory PreProcessedData
   */
  def preProcessing() = {
    val files = FileSystem.get(BO.spark.sparkContext.hadoopConfiguration).listStatus(new Path(inputPath))
    files.foreach { file => {
      val filename = file.getPath.toString()
      BO.logger.info("preprocessing file : " + filename)
      var (rowRdd, station_name) = dataPreprocessing(filename).get
      BO.logger.info("creating dataframe of preprocessed data for station : " + station_name)
      var df = BO.spark.createDataFrame(rowRdd, getDefaultInStringSchema.get)
      BO.logger.info("casting double type columns from string to double type")
      var cast_df = castColumns(df, StringType, DoubleType, (getColTypedGroups.get._1 ++ getColTypedGroups.get._2)).get
      BO.logger.info("casting integer type columns from string to integer type")
      cast_df = castColumns(df, StringType, IntegerType, (getColTypedGroups.get._1 ++ getColTypedGroups.get._3)).get
      BO.output_write_path = (outputPath + "/PreProcessedData/" + station_name).replaceAll("-", "_").replaceAll("\\(", "").replaceAll("\\)", "").split("\\s+").mkString("_")
      BO.logger.info("writing the preprocessed data to " + BO.output_write_path + " for station : " + station_name)
      cast_df.write.mode(SaveMode.Overwrite).parquet(BO.output_write_path)
    }
    }
  }
}
