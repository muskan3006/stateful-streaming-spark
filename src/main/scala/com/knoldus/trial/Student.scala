package com.knoldus.trial

import play.api.libs.json.Json

case class Student(id:Int,name:String,mobile:Int)
object Student{
  implicit val format = Json.format[Student]
}