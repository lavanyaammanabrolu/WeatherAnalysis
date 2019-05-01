package com.company.weather

import org.apache.log4j._
import org.apache.spark.rdd._
import org.apache.spark.sql.SparkSession
import scala.io.Source

class Configuration {

  /*
   *parsing the json string from the config.json file
   */
  def parseJson(configJson: String) = {
    BO.logger.info("parsing the config json file : " + configJson)
    BO.jsonStr = Source.fromFile(configJson).getLines.mkString.stripMargin
    BO.logger.info("json string : " + BO.jsonStr)
    val jp = new JsonParser(BO.jsonStr)
    BO.logger.info("finished parsing the config json file")
    BO.json = jp.json
    BO.logger.info("BO.json : " + BO.json)
  }

  /*
   *setting up the spark session and spark context objects
   */
  def sparkSetup() = {
    BO.logger.info("creating spark session")
    BO.spark = if (BO.json.config.mode.get.equalsIgnoreCase("local")) SparkSession.builder().appName("Weather Analyser").master("local[*]").getOrCreate() else SparkSession.builder().appName("Weather Analyser").getOrCreate()
    BO.sc = BO.spark.sparkContext
  }
}
