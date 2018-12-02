/*
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{Level, Logger}

object NetworkWordCountStateful {
  def main(args: Array[String]) {
    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.foldLeft(0)(_ + _)
      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
    }
    Logger.getLogger("org").setLevel(Level.WARN)
    val conf = new SparkConf().setMaster("yarn").setAppName("NetworkWordCountStateful")
    val sc = new StreamingContext(conf, Seconds(5))
    sc.checkpoint("/tmp/checkpoint/")
    val lines = sc.socketTextStream("ip-172-31-16-118", 9999)
    val words = lines.flatMap(_.split(" "))
    val wordDstream = words.map(x => (x, 1))
    val stateDstream = wordDstream.updateStateByKey[Int](updateFunc)
    stateDstream.print()
    stateDstream.saveAsTextFiles("/tmp/output.txt")
    sc.start()
    sc.awaitTermination()
  }
}
*/

import java.sql.{PreparedStatement, Connection, DriverManager}
import java.util.concurrent.atomic.AtomicInteger
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.storage.StorageLevel
import org.apache.log4j.{Level, Logger}

object NetworkWordCountStateful {
  def main(args: Array[String]) {
    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      val currentCount = values.foldLeft(0)(_ + _)
      val previousCount = state.getOrElse(0)
      Some(currentCount + previousCount)
    }
    Logger.getLogger("org").setLevel(Level.WARN)
    
    val conf = new SparkConf().setMaster("yarn").setAppName("NetworkWordCountStateful")
    val sc = new StreamingContext(conf, Seconds(5))
    sc.checkpoint("/tmp/checkpoint/")
    val lines = sc.socketTextStream("ip-172-31-16-118", 9999)
    val words = lines.flatMap(_.split(" "))
    val wordDstream = words.map(x => (x, 1))
    val stateDstream = wordDstream.updateStateByKey[Int](updateFunc)
    stateDstream.print()

    stateDstream.foreachRDD(rdd => {
      def func(records: Iterator[(String,Int)]) {
        var conn: Connection = null
        var stmt: PreparedStatement = null
        try {
          val url = "jdbc:mysql://mysql57test.c9sigk4hihzd.rds.cn-north-1.amazonaws.com.cn:3306/spark"
          val user = "admin"
          val password = "Chris7766!"
          conn = DriverManager.getConnection(url, user, password)
          records.foreach(p => {
            val sql = "insert into wordcount(word,count) values (?,?)"
            stmt = conn.prepareStatement(sql);
            stmt.setString(1, p._1.trim)
                        stmt.setInt(2,p._2.toInt)
            stmt.executeUpdate()
          })
        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          if (stmt != null) {
            stmt.close()
          }
          if (conn != null) {
            conn.close()
          }
        }
      }
      val repartitionedRDD = rdd.repartition(3)
      repartitionedRDD.foreachPartition(func)
    })
    sc.start()
    sc.awaitTermination()
  }
}
