import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * Created by zhaoxuan on 2017/4/23.
  */
object PSO {
  private val dimP = 6
  private val dimF = 4

  def compareTo(p1: Array[Double], p2: Array[Double]): Boolean = {
    true
  }

  def calFitness(pos: Array[Double]): Array[Double] = {
    val fitness = new Array[Double](dimF)
    fitness
  }

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("PSO")
    val sc = new SparkContext(conf)

    val data = sc.textFile("")
    val dim = dimP / 3
    val lB = new Array[Double](dim)
    val uB = new Array[Double](dim)
    val gB = new GlobalBest(dimP, dimF)

    val num = 10
    val particles = new Array[Particle](num)
    for (i <- 0 until num) {
      particles(i) = new Particle(dimP, dimF, lB, uB, gB)
    }

    val particlesRDD = sc.parallelize(particles)

    val iteration = 100

    for (i <- 0 until iteration) {
      particlesRDD.foreach(_.run())
    }
  }
}

class Particle(dimP: Int, dimF: Int, lB: Array[Double], uB: Array[Double], gB: GlobalBest) {
  private var lBest: Record = new Record(dimP, dimF)
  private val cur: Record = new Record(dimP, dimF)
  private val velocity: Array[Double] = new Array[Double](dimP)
  private val w = 0.8
  private val c1 = 0.2
  private val c2 = 0.2

//  private val this.dimP = dimP
//  private val this.dimF = dimF

  def randInit(): Unit = {
    val rand = new Random()
    for (i <- 0 until (dimP / 3)) {
      val j = 3 * i
      cur.pos(j) = rand.nextDouble()
      cur.pos(j + 1) = lB(i) + rand.nextDouble() * (uB(i) - lB(i))
      cur.pos(j + 2) = cur.pos(j + 1) + rand.nextDouble() * (uB(i) - cur.pos(j + 1))
    }

    cur.fitness = PSO.calFitness(cur.pos)
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
    cur.fitness = PSO.calFitness(cur.pos)
  }

  def run(): Unit = {
    randInit()
    updateBest()
    updateCur()
  }
}


class Record(dimP: Int, dimF: Int) {
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