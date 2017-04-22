import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhaoxuan on 2017/4/11.
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Word Count").setMaster("local")
    val sc = new SparkContext(conf)
    val file = sc.textFile("/Users/zhaoxuan/Desktop/test.txt")
    val wordCount = file.flatMap(_.split("\\s")).map((_, 1)).reduceByKey(_+_)

    wordCount.sortByKey().foreach(println)

    sc.stop()
  }
}
