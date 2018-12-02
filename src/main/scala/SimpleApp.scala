import org.apache.spark._
import SparkContext._
object SparkJoin{
  def main(args:Array[String]){
    if (args.length!=3){
      println("usage is WordCount <rating> <movie> <output>")
      return
    }
    val conf = new SparkConf().setAppName("SparkJoin").setMaster("yarn")
    val sc = new SparkContext(conf)

    val textFile=sc.textFile(args(0))

    val rating = textFile.map(line => { 
      val fileds = line.split("::") 
      (fileds(1).toInt, fileds(2).toDouble) 
   }) 

   val movieScores = rating 
       .groupByKey()
       .map(data => { 
          val avg = data._2.sum / data._2.size 
         (data._1, avg) 
       })

   val movies = sc.textFile(args(1))
   val movieskey=movies.map(line=>{
   val fileds=line.split("::")
   (fileds(0).toInt,fileds(1))
   }).keyBy(tup=>tup._1)

   val result = movieScores
     .keyBy(tup=>tup._1)
     .join(movieskey)
     .filter(f=>f._2._1._2>4.0)
     .map(f=>"\""+f._1+"\",\""+f._2._1._2+"\",\""+f._2._2._2+"\"")

    result.saveAsTextFile(args(2))
  }
}
