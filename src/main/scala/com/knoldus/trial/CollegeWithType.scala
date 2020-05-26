package com.knoldus.trial

import play.api.libs.json.Json

case class CollegeWithType(dataType:String,college: College)

object CollegeWithType{
  implicit val format = Json.format[CollegeWithType]
}
