import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhaoxuan on 2017/4/11.
  */
object CountA {
  def main(args: Array[String]): Unit = {
    val file = "hdfs://10.109.247.120:9000/input/"
    val conf = new SparkConf().setAppName("test application").setMaster("spark://10.109.247.120:7077")
      .setJars(List("/Users/zhaoxuan/Documents/Programs/IdeaProjects/SparkDemo/out/artifacts/SparkDemo_jar/SparkDemo.jar"))
    val sc = new SparkContext(conf)
    val count = sc.textFile(file, 2).filter(line => line.contains("a")).count()

    println(s"there are $count lines that contains 'a'")
    sc.stop()
  }
}
