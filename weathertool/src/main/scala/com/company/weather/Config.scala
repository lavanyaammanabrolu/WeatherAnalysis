package com.company.weather

case class Config(config:ConfigJSON) extends Serializable
case class ConfigJSON(mode:Option[String]=Some("local"),input_path:String,output_path:String,schema:Array[Fields],stations:Array[String],measuresCols:Array[String],winterMonths:Array[Int],springMonths:Array[Int],summerMonths:Array[Int],autumnMonths:Array[Int]) extends Serializable
case class Fields(cname:String,ctype:String) extends Serializable
