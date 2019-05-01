package com.company.weather

import org.apache.log4j.Logger

object WeatherAnalysis extends Configuration {
  def main(args: Array[String]): Unit = {
    println("Started")
    //    setting the logger
    BO.logger = Logger.getLogger(getClass.getName)
    //    reading the config.json file and parsing the json file
    BO.logger.info("Started Configuration Read")
    try {
      parseJson(args(0))
    } catch {
      case e: Throwable => BO.logger.error("Error while reading configuration : \n" + e.printStackTrace())
    }
    BO.logger.info("Completed Configuration Read")
    //    setting up spark
    BO.logger.info("Started Spark Setup")
    try {
      sparkSetup()
    } catch {
      case e: Throwable => BO.logger.error("Error while setting up spark session : \n" + e.printStackTrace())
    }
    BO.logger.info("Completed Spark Setup")
    //    pre-processing the raw weather data in order to bring it into a consistent structure for later analysis
    BO.logger.info("Started Preprocessing Weather Data For All Stations")
    try {
      val pp = new PreProcess()
      pp.preProcessing()
    } catch {
      case e: Throwable => BO.logger.error("Error while preprocessing weather data : \n" + e.printStackTrace())
    }
    BO.logger.info("Completed Preprocessing Weather Data For All Stations")
    //    analysis of the preprocessed data starts from here, where in all the processing for the 5 sub questions is done here
    BO.logger.info("Started Reading Preprocessed Data For All Stations")
    try {
      val asr = new Analysis()
      asr.analyzePreProcessedData()
    } catch {
      case e: Throwable => BO.logger.error("Error while preprocessing weather data : \n" + e.printStackTrace())
    }
    BO.logger.info("Completed Reading Preprocessed Data For All Stations")

  }
}
