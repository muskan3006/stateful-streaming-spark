package com.knoldus

import play.api.libs.json.{Json, OFormat}

case class Book(bookId: Int, bookName: String, qty: Int)

object Book {
  implicit val format: OFormat[Book] = Json.format[Book]
}
