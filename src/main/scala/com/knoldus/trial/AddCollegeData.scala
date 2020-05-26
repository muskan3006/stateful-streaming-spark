package com.knoldus.trial

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

object AddCollegeData {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Trial")
    val ssc = new StreamingContext(conf, Seconds(10))
    val interval = 20
    ssc.checkpoint("_checkpoint")
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark_mapWithState",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean))
    val topics = Seq("try")
    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))
      .flatMap(record => JsonUtils.parseAsOpt[CollegeWithType](record.value))
      .map { collegeWithType =>
        (collegeWithType.college.id, collegeWithType)
      }

        def getCollegeFieldFromState(state: State[College], withType: CollegeWithType):Option[CollegeWithType] ={
          state.getOption match {
            case Some(college) if college.id == withType.college.id =>
              val newSOSystem = College.getCollege(Some(withType.college), state.getOption())

              newSOSystem.map(soSys => withType.copy(college = soSys))
            case _ =>
              College.getCollege(Some(withType.college), None).map(storeOnceSystem => withType.copy(college = storeOnceSystem))

          }
        }


    val mappingFunction = (key: Int, value: Option[CollegeWithType], state: State[College]) => {
      value.flatMap { collegeWithType =>
        collegeWithType.dataType.toLowerCase match {
          case "student" =>
           getCollegeFieldFromState(state,collegeWithType)
          case "department" =>
            if (collegeWithType.college.department.isDefined) {
              state.update(collegeWithType.college)
            }
            Some(collegeWithType.college)
          case msg =>
            println(s"Invalid message type $msg received from Kafka")
            None
        }
      }
    }
    val addData = kafkaStream.mapWithState(StateSpec.function(mappingFunction)).checkpoint(Seconds(interval.toLong))
    addData.print()
    ssc.start()
    ssc.awaitTermination()
  }

}
