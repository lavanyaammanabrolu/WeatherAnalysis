package com.company.weather

import org.apache.spark.sql._
import scala.util.Try
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._
import scala.collection.mutable._

class PatternAnalysis {

  /*
   *iteratively rename all the aggregaed measure columns to respective tmax,tmin,af,rain,sun
   */
  def renameColumns(df: Try[DataFrame], str: String) = {
    Try(BO.json.config.measuresCols.toList.foldLeft(df.get)((df, c) =>
      df.drop(c).withColumnRenamed(c + str, c)
    ))
  }

  /*
   *method to group by groupByCol and calculate the avg of aggCol column
   */
  def aggAvgDF(groupByCol: String, aggCol: String, df: DataFrame) = {
    Try(df.groupBy(groupByCol).agg(avg(col(aggCol)).alias(aggCol + "_avg")))
  }

  /*
   *method to group by groupByCol1,groupByCol2 and calculate the avg of aggCol column
   */
  def aggAvgDF(groupByCol1: String, groupByCol2: String, aggCol: String, df: DataFrame) = {
    Try(df.groupBy(groupByCol1, groupByCol2).agg(avg(col(aggCol)).alias(aggCol + "_avg")))
  }

  /*
   *iteratively join all of the dataframes from dfList, each with each of the aggregated columns
   */
  def joinAllDataframes(joinCol: List[String], dfList: List[DataFrame]) = {
    Try(dfList.toList.tail.foldLeft(dfList.head)((accDF, newDF) => accDF.join(newDF, joinCol)))
  }

  /*
   *deriving the averages for each of the 5 measures : tmax,tmin,af,rain,sunshine
   */
  def deriveAveragedMeasures(groupByCol: List[String], df: DataFrame) = {
    val dfList = new ListBuffer[DataFrame]()
    groupByCol.length match {
      case 1 => BO.json.config.measuresCols.toList.foreach(aggCol => {
        dfList += aggAvgDF(groupByCol(0), aggCol, df).get
      })
      case 2 => BO.json.config.measuresCols.toList.foreach(aggCol => {
        dfList += aggAvgDF(groupByCol(0), groupByCol(1), aggCol, df).get
      })
    }
    joinAllDataframes(groupByCol, dfList.toList)
  }

  /*
   *forming the pattern descriptions across all stations on yearly basis as follows:-
   *  if the total average deviations add up to a negative number then it indicates that, that measure has reduced by that number over past years till now
   *  if the total average deviations add up to a positive number then it indicates that, that measure has increased by that number over past years till now
   */
  def derivePatterns(df: DataFrame) = {
    val tmax_bias = df.withColumn("tmax", col("tmax").cast("decimal")).agg(sum("tmax").alias("tmax_sum")).drop("tmax")
      .withColumn("patterns", when(col("tmax_sum") < 0, concat(lit("The avg maximum temperature reduced by "), abs(col("tmax_sum")), lit("degC over the past " + BO.yearNum + " years"))).otherwise(concat(lit("The avg maximum temperature increased by "), abs(col("tmax_sum")), lit("degC over the past " + BO.yearNum + " years"))))
      .drop("tmax_sum", "season")
    val tmin_bias = df.withColumn("tmin", col("tmin").cast("decimal")).agg(sum("tmin").alias("tmin_sum")).drop("tmin")
      .withColumn("patterns", when(col("tmin_sum") < 0, concat(lit("The avg minimum temperature reduced by "), abs(col("tmin_sum")), lit("degC over the past " + BO.yearNum + " years"))).otherwise(concat(lit("The avg minimum temperature increased by "), abs(col("tmin_sum")), lit("degC over the past " + BO.yearNum + " years"))))
      .drop("tmin_sum", "season")
    val af_bias = df.withColumn("af", col("af").cast("decimal")).agg(sum("af").alias("af_sum")).drop("af")
      .withColumn("patterns", when(col("af_sum") < 0, concat(lit("The avg frosty days reduced by "), abs(col("af_sum")), lit("days over the past " + BO.yearNum + " years"))).otherwise(concat(lit("The avg frosty days increased by "), abs(col("af_sum")), lit("days over the past " + BO.yearNum + " years"))))
      .drop("af_sum", "season")
    val rain_bias = df.withColumn("rain", col("rain").cast("decimal")).agg(sum("rain").alias("rain_sum")).drop("rain")
      .withColumn("patterns", when(col("rain_sum") < 0, concat(lit("The avg rainfall reduced by "), abs(col("rain_sum")), lit("mm over the past " + BO.yearNum + " years"))).otherwise(concat(lit("The avg rainfall increased by "), abs(col("rain_sum")), lit("mm over the past " + BO.yearNum + " years"))))
      .drop("rain_sum", "season")
    val sun_bias = df.withColumn("sun", col("sun").cast("decimal")).agg(sum("sun").alias("sun_sum")).drop("sun")
      .withColumn("patterns", when(col("sun_sum") < 0, concat(lit("The avg sunshine reduced by "), abs(col("sun_sum")), lit("hours over the past " + BO.yearNum + " years"))).otherwise(concat(lit("The avg sunshine increased by "), abs(col("sun_sum")), lit("hours over the past " + BO.yearNum + " years"))))
      .drop("sun_sum", "season")
    val reportDF = tmax_bias.union(tmin_bias.union(af_bias.union(rain_bias.union(sun_bias)))).withColumnRenamed("tmax_sum", "measure_deviations")
    reportDF.write.mode(SaveMode.Overwrite).csv(BO.json.config.output_path + "Answers/Q4c/YearlyTrend")
  }

  /*
   *forming the pattern descriptions across all stations for each season every year as follows:-
   *  if the total average deviations add up to a negative number then it indicates that, that measure has reduced by that number, for that season, over past years till now
   *  if the total average deviations add up to a positive number then it indicates that, that measure has increased by that number, for that season, over past years till now
   */
  def derivePatterns(grpCol: String, df: DataFrame) = {
    val tmax_bias = df.withColumn("tmax", col("tmax").cast("decimal")).groupBy(grpCol).agg(sum("tmax").alias("tmax_sum")).drop("tmax")
      .withColumn("patterns", when(col("tmax_sum") < 0, concat(lit("The "), col("season"), lit(" avg maximum temperature reduced by "), abs(col("tmax_sum")), lit("degC over the past "), lit(BO.yearNum), lit(" years"))).otherwise(concat(lit("The "), col("season"), lit(" avg maximum temperature increased by "), abs(col("tmax_sum")), lit("degC over the past "), lit(BO.yearNum), lit(" years"))))
      .drop("tmax_sum", "season")
    val tmin_bias = df.withColumn("tmin", col("tmin").cast("decimal")).groupBy(grpCol).agg(sum("tmin").alias("tmin_sum")).drop("tmin")
      .withColumn("patterns", when(col("tmin_sum") < 0, concat(lit("The "), col("season"), lit(" avg minimum temperature reduced by "), abs(col("tmin_sum")), lit("degC over the past "), lit(BO.yearNum), lit(" years"))).otherwise(concat(lit("The "), col("season"), lit(" avg minimum temperature increased by "), abs(col("tmin_sum")), lit("degC over the past "), lit(BO.yearNum), lit(" years"))))
      .drop("tmin_sum", "season")
    val af_bias = df.withColumn("af", col("af").cast("decimal")).groupBy(grpCol).agg(sum("af").alias("af_sum")).drop("af")
      .withColumn("patterns", when(col("af_sum") < 0, concat(lit("The "), col("season"), lit(" avg frosty days reduced by "), abs(col("af_sum")), lit("days over the past "), lit(BO.yearNum), lit(" years"))).otherwise(concat(lit("The "), col("season"), lit(" avg frosty days increased by "), abs(col("af_sum")), lit("days over the past "), lit(BO.yearNum), lit(" years"))))
      .drop("af_sum", "season")
    val rain_bias = df.withColumn("rain", col("rain").cast("decimal")).groupBy(grpCol).agg(sum("rain").alias("rain_sum")).drop("rain")
      .withColumn("patterns", when(col("rain_sum") < 0, concat(lit("The "), col("season"), lit(" avg rainfall reduced by "), abs(col("rain_sum")), lit("mm over the past "), lit(BO.yearNum), lit(" years"))).otherwise(concat(lit("The "), col("season"), lit(" avg rainfall increased by "), abs(col("rain_sum")), lit("mm over the past "), lit(BO.yearNum), lit(" years"))))
      .drop("rain_sum", "season")
    val sun_bias = df.withColumn("sun", col("sun").cast("decimal")).groupBy(grpCol).agg(sum("sun").alias("sun_sum")).drop("sun")
      .withColumn("patterns", when(col("sun_sum") < 0, concat(lit("The "), col("season"), lit(" avg sunshine reduced by "), abs(col("sun_sum")), lit("hours over the past "), lit(BO.yearNum), lit(" years"))).otherwise(concat(lit("The "), col("season"), lit(" avg sunshine increased by "), abs(col("sun_sum")), lit("hours over the past "), lit(BO.yearNum), lit(" years"))))
      .drop("sun_sum", "season")
    val reportDF = tmax_bias.union(tmin_bias.union(af_bias.union(rain_bias.union(sun_bias)))).withColumnRenamed("tmax_sum", "measure_deviations")
    reportDF.write.mode(SaveMode.Overwrite).csv(BO.json.config.output_path + "Answers/Q4c/SeasonalTrend")
  }

  /*
   *Deriving trend/direction for each of the 5 measures over all the years across all stations
   *  deriving the averages for each of the measures across all stations
   *  deriving the deviations to know the trend direction of each of the measures(tmax,tmin,af,rainfall,sunshine)
   *  form the pattern descriptions across all stations on yearly basis
   */
  def deriveYearlyMeasureTrends(pp_df: DataFrame) = {
    val groupByCol = List("year")
    val orderbyCol = groupByCol(0)
    BO.logger.info("Deriving yearly averages for each of the 5 measures across all stations together")
    val yearlyAverageMeasuresDF = renameColumns(deriveAveragedMeasures(groupByCol, pp_df), "_avg").get
    BO.logger.info("Deriving the change directions of each average measures between every consecutive 2 years, for each year")
    val yearlyAverageMeasuresChangesDF = measureChanges(createLeadColumn(None, orderbyCol, yearlyAverageMeasuresDF)).get
    BO.logger.info("Deriving the change pattern of each average measures over the years, for each year")
    derivePatterns(yearlyAverageMeasuresChangesDF)
  }

  /*
   *deriving the season column based on the month number
   */
  def seasoned_data(df: DataFrame) = {
    val seasonedDF = df.withColumn("season", when((col("month") === 12) || (col("month") === 1) || (col("month") === 2), "winter")
      .otherwise(when((col("month") === 3) || (col("month") === 4) || (col("month") === 5), "spring")
        .otherwise(when((col("month") === 6) || (col("month") === 7) || (col("month") === 8), "summer")
          .otherwise((when((col("month") === 9) || (col("month") === 10) || (col("month") === 11), "autumn"))
          )
        )
      )
    )
    seasonedDF
  }

  /*
   *Deriving trend/direction for each of the 5 measures on seasonal basis, all the years across all stations
   *  deriving the averages for each of the measures across all stations, for each season
   *  deriving the deviations to know the trend direction of each of the measures(tmax,tmin,af,rainfall,sunshine)
   *  form the pattern descriptions across all stations on seasonal basis
   */
  def deriveSeasonalMeasureTrends(pp_df: DataFrame) = {
    val groupByCol = List("year", "season")
    val orderbyCol = groupByCol(0)
    BO.logger.info("Deriving yearly seasonal averages for each of the 5 measures across all stations together.")
    val seasonalAverageMeasuresDF = renameColumns(deriveAveragedMeasures(groupByCol, seasoned_data(pp_df)), "_avg").get
    BO.logger.info("Deriving the change direction of each average measures between every consecutive 2 years, for each year,season")
    val seasonalAverageMeasuresChangesDF = measureChanges(createLeadColumn(Some(groupByCol(1)), orderbyCol, seasonalAverageMeasuresDF)).get
    BO.logger.info("Deriving the change pattern of each average measures over the years, for each season")
    val patternedDF = derivePatterns(groupByCol(1), seasonalAverageMeasuresChangesDF)
  }

  /*
   *adding the lead column
   */
  def createLeadColumn(partitionByCol: Option[String], orderbyCol: String, df: DataFrame) = {
    partitionByCol match {
      case None => Try(BO.json.config.measuresCols.toList.foldLeft(df)((df, c) =>
        df.withColumn("lead_" + c, lead(c, 1).over(Window.orderBy(orderbyCol)))
      ))
      case _ => Try(BO.json.config.measuresCols.toList.foldLeft(df)((df, c) =>
        df.withColumn("lead_" + c, lead(c, 1).over(Window.partitionBy(partitionByCol.get).orderBy(orderbyCol)))
      ))
    }
  }

  def createLeadColumn(partitionByCol1: String, partitionByCol2: String, orderbyCol: String, df: DataFrame) = {
    Try(BO.json.config.measuresCols.toList.foldLeft(df)((df, c) =>
      df.withColumn("lead_" + c, lead(c, 1).over(Window.partitionBy(partitionByCol1, partitionByCol2).orderBy(orderbyCol)))
        .filter(col("lead_" + c).isNotNull)
    ))
  }

  /*
   *deriving the deviations of each of the measure averages, between each of the 2 consecutive years
   */
  def measureChanges(df: Try[DataFrame]) = {
    Try(BO.json.config.measuresCols.toList.foldLeft(df.get)((df, c) =>
      df.withColumn(c + "_change", col("lead_" + c) - col(c)).drop("lead_" + c, c).withColumnRenamed(c + "_change", c)
    ))
  }

}
