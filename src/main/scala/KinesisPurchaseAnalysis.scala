import java.nio.ByteBuffer

import scala.util.Random

import com.amazonaws.auth.{BasicAWSCredentials, DefaultAWSCredentialsProviderChain}
import com.amazonaws.regions.RegionUtils
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.amazonaws.services.kinesis.model.PutRecordRequest
import org.apache.log4j.{Level, Logger}

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions
import org.apache.spark.streaming.kinesis.KinesisUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kinesis.KinesisInputDStream


object KinesisPurchaseAnalysis {
  def main(args: Array[String]) {
    if (args.length != 3) {
      System.err.println(
        """
          |Usage: KinesisWordCountASL <app-name> <stream-name> <endpoint-url> <region-name>
          |
          |    <app-name> is the name of the consumer app, used to track the read data in DynamoDB
          |    <stream-name> is the name of the Kinesis stream
          |    <endpoint-url> is the endpoint of the Kinesis service
          |                   (e.g. https://kinesis.us-east-1.amazonaws.com)
          |
          |Generate input data for Kinesis stream using the example KinesisWordProducerASL.
          |See http://spark.apache.org/docs/latest/streaming-kinesis-integration.html for more
          |details.
        """.stripMargin)
      System.exit(1)
    }

    Logger.getLogger("org").setLevel(Level.ERROR)
    val Array(appName, streamName, endpointUrl) = args
    val credentials = new DefaultAWSCredentialsProviderChain().getCredentials()
    require(credentials != null,
      "No AWS credentials found. Please specify credentials using one of the methods specified " +
        "in http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/credentials.html")
    val kinesisClient = new AmazonKinesisClient(credentials)
    kinesisClient.setEndpoint(endpointUrl)
    val numShards = kinesisClient.describeStream(streamName).getStreamDescription().getShards().size
    val numStreams = numShards
    val batchInterval = Seconds(5)

    val kinesisCheckpointInterval = batchInterval
    //val regionName = RegionUtils.getRegionByEndpoint(endpointUrl).getName()
    val regionName="cn-north-1"
    
    val sparkConfig = new SparkConf().setAppName("KinesisPurchaseAnalysis").setMaster("yarn")
    //sparkConfig.set("spark.driver.allowMultipleContexts", "true") 
    val ssc = new StreamingContext(sparkConfig, batchInterval)
    ssc.checkpoint("/tmp/checkpoint")

    val kinesisStreams = KinesisUtils.createStream(ssc, "KinesisPurchaseAnalysis", "sparkinput", "https://kinesis.cn-north-1.amazonaws.com.cn", "cn-north-1",
          InitialPositionInStream.LATEST, kinesisCheckpointInterval, StorageLevel.MEMORY_AND_DISK_2)

    /*
    val kinesisStreams = (0 until numStreams).map { i =>
      KinesisUtils.createStream(ssc, appName, streamName, endpointUrl, regionName,
        InitialPositionInStream.TRIM_HORIZON, kinesisCheckpointInterval, StorageLevel.MEMORY_AND_DISK_2)
    }

    
    val kinesisStreams = (0 until numStreams).map { i =>
      KinesisInputDStream.builder
        .streamingContext(ssc)
        .streamName(streamName1)
        .endpointUrl(endpointUrl)
        .regionName(regionName)
        .initialPosition(InitialPositionInStream.LATEST)
        .checkpointAppName(appName1)
        .checkpointInterval(kinesisCheckpointInterval)
        .storageLevel(StorageLevel.MEMORY_AND_DISK_2)
        .build()
    }
    */

    //val unionStreams = ssc.union(kinesisStreams)
    //val words = unionStreams.flatMap(byteArray => new String(byteArray).split(" "))
    //val pair = words.map(x => (x,1))
    //val wordCounts = words.map(word => (word, 1)).reduceByKey(_ + _)
    //val wordCounts = pair.reduceByKeyAndWindow(_ + _,_ - _,Minutes(2),Seconds(10),2)
    //wordCounts.print()
    kinesisStreams.print()

    ssc.start()
    ssc.awaitTermination()
  }
}


/*
import kafka.serializer.StringDecoder
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.kafka.KafkaUtils

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.regions.RegionUtils
import com.amazonaws.services.kinesis.{AmazonKinesis, AmazonKinesisClient}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kinesis.KinesisInputDStream
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream.LATEST

import java.nio.ByteBuffer
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.model.PutRecordRequest
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.dstream.DStream.toPairDStreamFunctions

object KinesisPurchaseAnalysis {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val appName = "KinesisPurchaseAnalysis"
    val streamName = "sparkinput"
    val endpointUrl = "https://kinesis.cn-north-1.amazonaws.com.cn"

    val credentials = new DefaultAWSCredentialsProviderChain().getCredentials()
    require(credentials !=null, "No AWS credentials found. Please specify credentials using one of the methods specified " +
      "in http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/credentials.html")
    val kinesisClient = new AmazonKinesisClient(credentials)
    kinesisClient.setEndpoint(endpointUrl)
    val numShards = kinesisClient.describeStream(streamName).getStreamDescription.getShards().size()

    val numStreams = numShards

    // Spark Streaming batch interval
    val batchInterval = Milliseconds(2000)
    val kinesisCheckpointInterval = batchInterval
    val regionName = getRegionNameByEndpoint(endpointUrl)

    val sparkConf = new SparkConf().setAppName("KinesisPurchaseAnalysis").setMaster("yarn")
    val ssc = new StreamingContext(sparkConf, batchInterval)
    val kinesisStreams = KinesisInputDStream.builder
        .streamingContext(ssc)
        .streamName(streamName)
        .endpointUrl(endpointUrl)
        .regionName(regionName)
        .initialPosition(InitialPositionInStream.LATEST)
        .checkpointAppName(appName)
        .checkpointInterval(kinesisCheckpointInterval)
        .storageLevel(StorageLevel.MEMORY_AND_DISK_2)
        .build()
    
    /*
    val kinesisStreams = (0 until numStreams).map { i =>
      KinesisInputDStream.builder
        .streamingContext(ssc)
        .streamName(streamName)
        .endpointUrl(endpointUrl)
        .regionName(regionName)
        .initialPosition(InitialPositionInStream.LATEST)
        .checkpointAppName(appName)
        .checkpointInterval(kinesisCheckpointInterval)
        .storageLevel(StorageLevel.MEMORY_AND_DISK_2)
        .build()
    }
    */
    //Union all the streams
    val unionStreams = ssc.union(kinesisStreams)

    val words = unionStreams.flatMap(byteArray => new String(byteArray).split(" "))

    val wordCounts =words.map(word => (word, 1)).reduceByKey(_ + _)
    /*
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
    */

    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }


  def getRegionNameByEndpoint(endpoint: String): String = {
    import scala.collection.JavaConverters._
    val uri = new java.net.URI(endpoint)
    RegionUtils.getRegionsForService(AmazonKinesis.ENDPOINT_PREFIX)
      .asScala
      .find(_.getAvailableEndpoints.asScala.toSeq.contains(uri.getHost))
      .map(_.getName)
      .getOrElse(
        throw new IllegalArgumentException(s"Could not resolve region for endpoint: $endpoint"))
  }
}

*/
