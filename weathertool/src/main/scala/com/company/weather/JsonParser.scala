package com.company.weather

import net.liftweb.json._

class JsonParser(jsonString:String) extends Serializable {
  implicit val formats = DefaultFormats
  val json:Config = parse(jsonString).extract[Config]
  println(json)
}
