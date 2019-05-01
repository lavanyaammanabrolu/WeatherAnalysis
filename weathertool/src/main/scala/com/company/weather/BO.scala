package com.company.weather

import org.apache.spark.sql._
import org.apache.spark._
import org.apache.log4j._

object BO {
  private var _logger:Logger = null
  private var _spark:SparkSession = null
  private var _sc:SparkContext = null
  private var _jsonStr:String = null
  private var _json:Config = null
  private var _output_write_path:String = null
  private var _yearNum:Long = 0

  def logger = _logger
  def spark = _spark
  def sc =_sc
  def jsonStr = _jsonStr
  def json = _json
  def output_write_path = _output_write_path
  def yearNum = _yearNum

  def logger_=(value:Logger)=_logger=value
  def spark_=(value:SparkSession)=_spark=value
  def sc_=(value:SparkContext)=_sc=value
  def jsonStr_=(value:String)=_jsonStr=value
  def json_=(value:Config)=_json=value
  def output_write_path_=(value:String)=_output_write_path=value
  def yearNum_=(value:Long) = _yearNum = value

}
