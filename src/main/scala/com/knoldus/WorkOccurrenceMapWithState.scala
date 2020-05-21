package com.knoldus

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}

object WorkOccurrenceMapWithState {

    def main(args: Array[String]) {
      val conf = new SparkConf().setMaster("local[*]").setAppName("KafkaReceiver")
      val ssc = new StreamingContext(conf, Seconds(10))

      val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> "localhost:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "spark_mapWithState",
        "auto.offset.reset" -> "latest",
        "enable.auto.commit" -> (false: java.lang.Boolean))
      val topics = Seq("demo")
      val kafkaStream = KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Subscribe[String, String](topics, kafkaParams))
      val splits = kafkaStream.map(record => (record.key(), record.value)).flatMap(x => x._2.split(" "))

      val mappingFunc = (word: String, one: Option[Int], state: State[Int]) => {
        val sum = one.getOrElse(0) + state.getOption.getOrElse(0)

        state.update(sum)
        (word, sum)
      }

      ssc.checkpoint("/home/knoldus/Downloads/hadoop")
      val wordCounts = splits.map(x => (x, 1)).reduceByKey(_ + _).mapWithState(StateSpec.function(mappingFunc))

      wordCounts.print() //prints the wordcount result of the stream
      ssc.start()
      ssc.awaitTermination()
    }

}
