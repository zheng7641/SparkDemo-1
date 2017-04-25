import org.apache.spark.util.AccumulatorV2

/**
  * Created by zhaoxuan on 2017/4/17.
  */
class MyAccumulator extends AccumulatorV2[Long, Long] {
  private var count = 0L

  override def add(v: Long): Unit = {
    count += v
  }

  override def reset(): Unit = {
    count = 0
  }

  override def merge(other: AccumulatorV2[Long, Long]): Unit = {
    count = other.value
  }

  override def copy(): AccumulatorV2[Long, Long] = {
    val myAccumulator = new MyAccumulator()
    myAccumulator.count = this.count
    myAccumulator
  }

  override def isZero: Boolean = {
    count == 0
  }

  override def value: Long = {
    count
  }
}
