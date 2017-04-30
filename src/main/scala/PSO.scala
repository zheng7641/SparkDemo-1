import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * Created by zhaoxuan on 2017/4/23.
  */
object PSO {
  private val dimP = 12
  private val dimF = 1

  def compareTo(p1: Array[Double], p2: Array[Double]): Boolean = {
    for (i <- p1.indices) {
      if (p1(i) > p2(i)) {
        return true
      }
    }
    false
  }

  def calFitness(pos: Array[Double], data: Broadcast[DataFrame]): Array[Double] = {
    val fitness = new Array[Double](dimF)
    val n = data.value.collect().count(t => isValid(pos, t.toSeq.toArray.map(_.toString.toDouble)))

    fitness(0) = n / data.value.collect().length

    fitness
  }

  def isValid(pos: Array[Double], transaction: Array[Double]): Boolean = {
    var j = 0
    for (i <- transaction.indices) {
      if (pos(j) < 0.66 ) {
        if (transaction(i) < pos(j + 1) && transaction(i) > pos(j + 2)) {
          return false
        }
      }
      j += 3
    }
    true
  }

  def main(args: Array[String]) {
    val ss = SparkSession.builder().appName("PSO").master("local[4]").config("spark.sql.warehouse.dir", "C:/Users/Zhaoxuan/IdeaProjects/SparkDemo/spark-warehouse").getOrCreate()

    //create DataFrame from source file
    val schemaString = "depth latitude longitude richter"

    val fields = schemaString.split("\\s").map(fieldName => StructField(fieldName, DoubleType, nullable = true))
    val schema = StructType(fields)

    val rowRDD = ss.sparkContext.textFile("C:/Users/Zhaoxuan/Desktop/Data/QU.dat").map(_.split(","))
      .map(line => Row(line(0).toDouble, line(1).toDouble, line(2).toDouble, line(3).toDouble))

    val df = ss.createDataFrame(rowRDD, schema)

    //init particle swarm
    val fieldNames = df.schema.fieldNames
    val lB = df.agg(fieldNames(0) -> "min", fieldNames(1) -> "min", fieldNames(2) -> "min", fieldNames(3) -> "min")
      .first().toSeq.toArray.map(_.toString.toDouble)
    val uB = df.agg(fieldNames(0) -> "max", fieldNames(1) -> "max", fieldNames(2) -> "max", fieldNames(3) -> "max")
      .first().toSeq.toArray.map(_.toString.toDouble)
    val gB = new GlobalBest(dimP, dimF)

    ss.sparkContext.register(gB, "Global Best")

    val broadCastDF = ss.sparkContext.broadcast(df)
    val broadCastLB = ss.sparkContext.broadcast(lB)
    val broadCastUB = ss.sparkContext.broadcast(uB)

    val num = 10
    val particles = new Array[Particle](num)
    for (i <- 0 until num) {
      particles(i) = new Particle(dimP, dimF, broadCastLB.value, broadCastUB.value, gB, broadCastDF)
    }

    val particlesRDD = ss.sparkContext.parallelize(particles)

    particlesRDD.foreach(_.randInit())

    val iteration = 100

    for (i <- 0 until iteration) {
      particlesRDD.map(_.run())
      printf(s"Iteration: $i\n")
    }

    println(gB.value.size)

    for (i <- gB.value.indices) {
      println("Position: " + gB.value(i).pos + "  Fitness: " + gB.value(i).fitness)
    }
  }
}

class Particle(dimP: Int, dimF: Int, lB: Array[Double], uB: Array[Double], gB: GlobalBest, data: Broadcast[DataFrame]) extends Serializable{
  private var lBest: Record = new Record(dimP, dimF)
  private val cur: Record = new Record(dimP, dimF)
  private val velocity: Array[Double] = new Array[Double](dimP)
  private val w = 0.8
  private val c1 = 0.2
  private val c2 = 0.2

  def randInit(): Particle = {
    val rand = new Random()
    for (i <- 0 until (dimP / 3)) {
      val j = 3 * i
      cur.pos(j) = rand.nextDouble()
      cur.pos(j + 1) = lB(i) + rand.nextDouble() * (uB(i) - lB(i))
      cur.pos(j + 2) = cur.pos(j + 1) + rand.nextDouble() * (uB(i) - cur.pos(j + 1))
    }

    cur.fitness = PSO.calFitness(cur.pos, data)
//    println(cur.pos)
//    println(cur.fitness)

    this
  }

  def updateBest(): Unit = {
    updateLocal()
    updateGlobal()
  }

  def updateLocal(): Unit = {
    if (PSO.compareTo(cur.fitness, lBest.fitness)) {
      lBest = cur
    }
  }

  def updateGlobal(): Unit = {
    gB.update(cur)
  }

  def updateCur(): Unit = {
    updateCurPos()
    updateFitness()
  }

  def updateCurPos(): Unit = {
    for (i <- 0 until dimP) {
      cur.pos(i) += velocity(i)
    }
  }

  def updateV(): Unit = {
    val rand = new Random()
    val r1 = rand.nextDouble()
    val r2 = rand.nextDouble()

    for (i <- 0 until dimP) {
      val gBest = gB.select()
      velocity(i) = w * velocity(i) + c1 * r1 * (gBest.pos(i) - cur.pos(i)) + c2 * r2 * (lBest.pos(i) - cur.pos(i))
    }
  }

  def updateFitness(): Unit = {
    cur.fitness = PSO.calFitness(cur.pos, data)
  }

  def run(): Particle = {
    updateBest()
    updateCur()
    updateV()

    this
  }
}


class Record(dimP: Int, dimF: Int) extends Serializable{
  var pos: Array[Double] = new Array[Double](dimP)
  var fitness: Array[Double] = new Array[Double](dimF)
}


class GlobalBest(dimP: Int, dimF: Int) extends AccumulatorV2[Record, ArrayBuffer[Record]] {
  private var gBest: ArrayBuffer[Record] = ArrayBuffer[Record]()
  private var notModified = true

  def update(v: Record): Unit = {
    for (i <- gBest.indices) {
      if (PSO.compareTo(v.fitness, gBest(i).fitness)) {
        val index = Math.floor(gBest.size * (new Random).nextDouble()).toInt
        gBest(index) = v
        notModified = false
        return
      }
    }

    add(v)
  }

  def select(): Record = {
    val index = Math.floor(gBest.size * (new Random).nextDouble()).toInt
    gBest(index)
  }

  override def add(v: Record): Unit = {
    gBest.append(v)
    notModified = false
  }

  override def reset(): Unit = {
    gBest = ArrayBuffer[Record]()
    notModified = true
  }

  override def merge(other: AccumulatorV2[Record, ArrayBuffer[Record]]): Unit = {
    gBest.appendAll(other.value)
    notModified = false
  }

  override def copy(): AccumulatorV2[Record, ArrayBuffer[Record]] = {
    val newGlobalBest = new GlobalBest(this.dimP, this.dimF)
    newGlobalBest.gBest = this.gBest
    notModified = false
    newGlobalBest
  }

  override def isZero: Boolean = {
    notModified
  }

  override def value: ArrayBuffer[Record] = {
    gBest
  }
}