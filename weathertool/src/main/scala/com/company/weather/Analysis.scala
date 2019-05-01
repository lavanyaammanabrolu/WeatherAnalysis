package com.company.weather

import org.apache.spark.sql._
import scala.util.Try
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions._

class Analysis {

  val pa = new PatternAnalysis()
  var answers: DataFrame = null

  /*
   *reads the pre-prcoessed data and
   *calls each method to get answer to each of the 5 questions
   */
  def analyzePreProcessedData() = {
    BO.logger.info("reading the preprocessed data for all stations together")
    var preprocessed_path = BO.json.config.output_path + "/PreProcessedData/*/*"
    var pp_df = BO.spark.read.parquet(preprocessed_path)
    BO.logger.info("Caching pp_df dataframe")
    pp_df.cache()
    BO.logger.info("finished reading the preprocessed data for all stations together")

    BO.logger.info("Deriving the total number of years for which all the data is present")
    BO.yearNum = numOfYears(pp_df)
    BO.logger.info("BO.yearNum : " + BO.yearNum)

    BO.logger.info("Calling rankStationsByOnlineTimePeriod() method to rank stations by howlong they have been online for sub question 4.a")
    rankStationsByOnlineTimePeriod(pp_df)

    BO.logger.info("Calling rankStationsByRainOrSun() method to rank stations by rain and/or sunshine for sub question 4.b")
    rankStationsByRainOrSun(pp_df, "rain")
    rankStationsByRainOrSun(pp_df, "sun")

    BO.logger.info("Calling deriveYearlyMeasureTrends(),deriveYearlyMeasureTrendsPerStation(),deriveSeasonalMeasureTrends(),deriveSeasonalMeasureTrendsPerStation() method to find different patterns for sub question 4.c")
    pa.deriveYearlyMeasureTrends(pp_df)
    pa.deriveSeasonalMeasureTrends(pp_df)

    BO.logger.info("Calling maxRainOrSunYear() method to calculate sub question 4.d")
    maxRainOrSunYear(pp_df, "rain")
    maxRainOrSunYear(pp_df, "sun")

    BO.logger.info("Calling deriveBestWorstYearForMay() method to calculate sub question 4.e")
    deriveBestWorstYearForMay(pp_df)
  }

  /*
   *getting the total number of years for which the weather data was present across all stations
   */
  def numOfYears(df: DataFrame): Long = {
    df.select("year").distinct().count()
  }

  /*
   *calculating the may averages for tmin,tmax,af,rain and sunshine across all stations
   *calculating the best and worst year based on these averages using the following order:-
   *  the year with maximum sunshine,tmax,tmin & minimum rainfall,frosty days is taken to be best year
   */
  def deriveBestWorstYearForMay(pp_df: DataFrame) = {
    val pp_df_filtered = Try(pp_df.filter(col("month") === 5)).get
    val groupByCol = List("year")
    BO.logger.info("Deriving averages for each of the 5 measures across all stations for each year's May month")
    val yearlyMayAverageMeasuresDF = pa.deriveAveragedMeasures(groupByCol, pp_df_filtered).get
    yearlyMayAverageMeasuresDF.write.mode(SaveMode.Overwrite).csv(BO.json.config.output_path + "Answers/Q4e/MayAverages")
    BO.logger.info("Ranking the 5 averages in the order -> sunshine.desc,tmax.desc,tmin.desc,rain.asc,frosty_days.asc -> inorder to determine best and worst year for May month")
    val yearlyRankedMayAverageMeasuresDF = Try(yearlyMayAverageMeasuresDF.withColumn("rank", row_number().over(Window.orderBy(col("sun_avg").desc, col("tmax_avg").desc, col("tmin_avg").desc, col("rain_avg").asc, col("af_avg").asc)))).get
    BO.logger.info("Caching the yearlyRankedAverageMeasuresDF dataframe")
    yearlyRankedMayAverageMeasuresDF.cache()
    val best_year = Try(yearlyRankedMayAverageMeasuresDF.filter(col("rank") === 1).select(groupByCol(0)).withColumn("year_type", lit("best_year")).withColumn("value", col("year")).drop("year")).get
    val min_rank = Try(yearlyRankedMayAverageMeasuresDF.agg(max(col("rank"))).rdd.map(r => r(0)).collect()(0)).get
    val worst_year = Try(yearlyRankedMayAverageMeasuresDF.filter(col("rank") === min_rank).select(groupByCol(0)).withColumn("year_type", lit("worst_year")).withColumn("value", col("year")).drop("year")).get
    val bestWorstYearDF = best_year.union(worst_year)
    bestWorstYearDF.write.mode(SaveMode.Overwrite).csv(BO.json.config.output_path + "Answers/Q4e/BestWorstYear")
  }

  /*
   *rank stations by how long they have been online
   *  the station with least/minimum year will be ranked 1 and so on
   */
  def rankStationsByOnlineTimePeriod(df: DataFrame) = {
    BO.logger.info("Ranking stations by how long they have been online, by deriving the minimum/oldest year for every station and then ranking each station on this min_year")
    val pp_df_min_year = df.groupBy("station").agg(min(col("year")).alias("min_year"))
      .withColumn("rank", row_number().over(Window.orderBy(col("min_year"))))
    pp_df_min_year.write.mode(SaveMode.Overwrite).csv(BO.json.config.output_path + "Answers/Q4a/OldestStation")
  }

  /*
   *ranking stations by max rain and maximum sunshine
   */
  def rankStationsByRainOrSun(df: DataFrame, aggCol: String) = {
    BO.logger.info("Ranking stations by " + aggCol)
    val pp_df_ranked_year = df.groupBy("station").agg(max(col(aggCol)).alias("max_" + aggCol))
      .withColumn("rank", row_number().over(Window.orderBy(col("max_" + aggCol).desc)))
    pp_df_ranked_year.select("rank", "station", "max_" + aggCol)
    pp_df_ranked_year.write.mode(SaveMode.Overwrite).csv(BO.json.config.output_path + "Answers/Q4b/Max" + aggCol.split("_")(0).capitalize + "Station")
  }

  /*
   *getting the year with maximum sunshine and rainfall for every station
   */
  def maxRainOrSunYear(df: DataFrame, aggCol: String) = {
    BO.logger.info("Deriving the year with maximum " + aggCol + " for each station")
    val pp_df_min_year = df.groupBy("station").agg(max(col(aggCol)).alias("max_" + aggCol)).withColumnRenamed("station", "station_1")
    val joined = df.join(broadcast(pp_df_min_year), col("station") === col("station_1")).drop("station_1")
    val year_df = joined.where(col(aggCol) === col("max_" + aggCol)).select("station", "year", "max_" + aggCol)
    year_df.write.mode(SaveMode.Overwrite).csv(BO.json.config.output_path + "Answers/Q4d/Max" + aggCol.split("_")(0).capitalize + "Year")
  }

}
