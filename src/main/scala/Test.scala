import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Zhaoxuan on 2017/4/28.
  */
object Test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Test App").setMaster("local")
    val sc = new SparkContext(conf)

    val a = new Array[Node](5)

    for (i <- a.indices) {
      a(i) = new Node()
    }

    val aRDD = sc.parallelize(a)

    val rdd = sc.parallelize(Array(1,2,3,4,5,6))

    val v = sc.broadcast(rdd)

    aRDD.map(_.increase(v)).foreach(_.printResult())
  }
}

class Node() extends Serializable {
  private val x = new Array[Int](5)

  def increase(broadcast: Broadcast[RDD[Int]]): Node = {
    for (i <- x.indices) {
      x(i) += broadcast.value.filter(x => this.lessThan(x)).count().toInt
    }
    this
  }

  def lessThan(x: Int): Boolean = {
    if (x < 3) {
      return true
    }
    false
  }

  def printResult(): Unit = {
    for (i <- x.indices) {
      print(x(i))
    }
    println()
  }
}
