import kafka.serializer.StringDecoder
import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.log4j.{Level, Logger}
import java.util.HashMap
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.json4s._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write
import org.apache.spark.streaming.Interval

object KafkaWordCount {
  implicit val formats = DefaultFormats
  def main(args: Array[String]): Unit={
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }
    Logger.getLogger("org").setLevel(Level.WARN)
    val Array(zkQuorum, group, topics, numThreads) = args
    val sparkConf = new SparkConf().setAppName("KafkaWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    ssc.checkpoint("/tmp/checkpoint")
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L))
      .reduceByKeyAndWindow(_+_,_-_, Seconds(1), Seconds(1), 1).foreachRDD(rdd => {
          if(rdd.count !=0 ){
               val props = new HashMap[String, Object]()
               props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "ip-172-31-30-16:9092")
               props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
               "org.apache.kafka.common.serialization.StringSerializer")
               props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
               "org.apache.kafka.common.serialization.StringSerializer")
               val producer = new KafkaProducer[String, String](props)
               val str = write(rdd.collect)
               val message = new ProducerRecord[String, String]("result", null, str)
               producer.send(message)
          }
      })
    ssc.start()
    ssc.awaitTermination()
  }
}

/*
object KafkaWordCount{
def main(args:Array[String]){
  Logger.getLogger("org").setLevel(Level.WARN)
  val sc = new SparkConf().setAppName("KafkaWordCount").setMaster("yarn")
  val ssc = new StreamingContext(sc,Seconds(5))
  ssc.checkpoint("/tmp/checkpoint")
  val zkQuorum = "ip-172-31-30-16:2181"
  val group = "1"
  val topics = "wordtest"
  val numThreads = 1
  val topicMap =topics.split(" ").map((_,numThreads.toInt)).toMap
  val lineMap = KafkaUtils.createStream(ssc,zkQuorum,group,topicMap)
  val lines = lineMap.map(_._2)
  val words = lines.flatMap(_.split(" "))
  val pair = words.map(x => (x,1))
  val wordCounts = pair.reduceByKeyAndWindow(_ + _,_ - _,Minutes(2),Seconds(10),2)
  wordCounts.print
  ssc.start
  ssc.awaitTermination
 }
} 

*/

/*
object KafkaWordCount{
def main(args:Array[String]){
  if (args.length < 2) {
      System.err.println(s"""
        |Usage: ClickstreamSparkstreaming <brokers> <topics>
        |  <brokers> is a list of one or more Kafka brokers
        |  <topics> is a list of one or more kafka topics to consume from
        |
        """.stripMargin)
      System.exit(1)
    }

  val Array(brokers, topics) = args
  val sc = new SparkConf().setAppName("KafkaWordCount")
  val ssc = new StreamingContext(sc,Seconds(5))
  val topicsSet = topics.split(",").toSet
  val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
  val lineMap = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
  val lines = lineMap.map(_._2)
  val words = lines.flatMap(_.split(" "))
  val pair = words.map(x => (x,1))
  val wordCounts = pair.reduceByKeyAndWindow(_ + _,_ - _,Minutes(2),Seconds(10),2)
  wordCounts.print
  ssc.start
  ssc.awaitTermination
 }
} 

*/
