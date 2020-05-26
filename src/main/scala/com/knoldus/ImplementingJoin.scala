package com.knoldus

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, State, StateSpec, StreamingContext}
import play.api.libs.json.Json

object ImplementingJoin {
  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Trial")
    val ssc = new StreamingContext(conf, Seconds(10))
    val interval = 15
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

    val bookData = kafkaStream.map(record => Json.parse(record.value()).as[Book])
    val bookRecord = bookData.map(book => (book.bookId, book))

    val mappingFunction = (key: Int, value: Option[Book], state: State[Library]) => {
      def updateLibrary(newBook: Book): (Int, Library) = {
        val existingBooks: Seq[Book] =
          state
            .getOption()
            .map(_.books)
            .getOrElse(Seq[Book]())
        val updatedLibrary = Library(newBook +: existingBooks)
        state.update(updatedLibrary)
        (key, updatedLibrary)

      }

      value.map(book => updateLibrary(book))
    }

    val library = bookRecord
      .mapWithState(StateSpec.function(mappingFunction))
      .checkpoint(Seconds(interval.toLong))

    library.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
