package com.knoldus.trial

import play.api.libs.json.{Json, OFormat}

case class College(id:Int, student:Option[Student],department: Option[Department])
 object College{
   implicit val format: OFormat[College] = Json.format[College]

   def getCollege(studentSample:Option[College],departmentSample:Option[College]):Option[College]={
     (studentSample, departmentSample) match {
       case (Some(student), Some(department)) =>
         Some(student.copy(id=student.id,student=student.student,department=department.department))
       case (Some(studentSample), None) =>
         Some(studentSample)
       case (None, Some(departmentSample)) => Some(departmentSample)
       case _ => None
     }
   }

 }
