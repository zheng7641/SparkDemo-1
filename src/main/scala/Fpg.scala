import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
/**
  * Created by Zhaoxuan on 2017/5/11.
  */
object Fpg {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("FPG Demo").setMaster("local[4]")
    val sc = new SparkContext(conf)

    val data = sc.textFile("hdfs://10.109.247.120:9000/input/pumsb.csv")

    val transactions: RDD[Array[String]] = data.map(s => s.trim.split(','))

    val fpg = new FPGrowth().setMinSupport(0.68).setNumPartitions(10)

    val model = fpg.run(transactions)


    val c = model.freqItemsets.collect().length

    model.freqItemsets.collect().foreach { itemset =>
      println(itemset.items.mkString("[", ",", "]") + "," + itemset.freq)
    }

    sc.stop()
  }
}
