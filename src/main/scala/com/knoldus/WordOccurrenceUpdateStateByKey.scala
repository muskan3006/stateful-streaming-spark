package com.knoldus

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordOccurrenceUpdateStateByKey {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[*]").setAppName("KafkaReceiver")
    val ssc = new StreamingContext(conf, Seconds(10))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark_updateStateByKey",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean))
    val topics = Seq("demo")
    val kafkaStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))
    val splits = kafkaStream.map(record => (record.key(), record.value)).flatMap(x => x._2.split(" "))
        val updateFunc = (values: Seq[Int], state: Option[Int]) => {
          val currentCount = values.sum
          val previousCount = state.getOrElse(0)
          Some(currentCount + previousCount)
        }

    //Defining a check point directory for performing stateful operations
    ssc.checkpoint("/home/knoldus/Downloads/hadoop")
    val wordCounts = splits.map(x => (x, 1)).reduceByKey(_ + _).updateStateByKey(updateFunc)

    wordCounts.print() //prints the wordcount result of the stream
    ssc.start()
    ssc.awaitTermination()
  }
}