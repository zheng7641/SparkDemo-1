import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by zhaoxuan on 2017/4/17.
  */
object NetworkWordCount {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("NetworkWordCount").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))

    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split("\\s"))
    val pairs = words.map((_, 1))
    val wordCount = pairs.reduceByKey(_+_)

    wordCount.print()

    ssc.start()
    ssc.awaitTermination()
  }
}