import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql._
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

import scala.util.Random

/**
  * Created by zhaoxuan on 2017/4/30.
  */
object NewPSO {
  def main(args: Array[String]): Unit = {
//    val warehouseDir = "C:/Users/Zhaoxuan/IdeaProjects/SparkDemo/spark-warehouse"
    val warehouseDir = "/Users/zhaoxuan/Documents/Programs/IdeaProjects/SparkDemo/spark-warehouse"
    val ss = SparkSession.builder().appName("PSO").master("local[4]").config("spark.sql.warehouse.dir", warehouseDir).getOrCreate()

    //create DataFrame from source file
    val schemaString = "depth latitude longitude richter"

    val fields = schemaString.split("\\s").map(fieldName => StructField(fieldName, DoubleType, nullable = true))
    val schema = StructType(fields)

    val filePath = "/Users/zhaoxuan/Documents/Data/PSO Data/QU.dat"
    val rowRDD = ss.sparkContext.textFile(filePath).map(_.split(","))
      .map(line => Row(line(0).toDouble, line(1).toDouble, line(2).toDouble, line(3).toDouble))

    val df = ss.createDataFrame(rowRDD, schema)

    //init particle swarm
    val dimP = 12
    val dimF = 4
    val population = 10
    val fieldNames = df.schema.fieldNames

    val lB = df.agg(fieldNames(0) -> "min", fieldNames(1) -> "min", fieldNames(2) -> "min", fieldNames(3) -> "min")
      .first().toSeq.toArray.map(_.toString.toDouble)
    val uB = df.agg(fieldNames(0) -> "max", fieldNames(1) -> "max", fieldNames(2) -> "max", fieldNames(3) -> "max")
      .first().toSeq.toArray.map(_.toString.toDouble)

    val particles = new Array[Particle](population)

    for (i <- particles.indices) {
      particles(i) = new Particle(dimP, dimF, lB, uB)
    }

    val particleRDD = ss.sparkContext.parallelize(particles)

    val res = particleRDD.map(_.randInit()).collect()

    for (i <- 0 until population) {
      res(i).printInfo()
    }

    ss.stop()
  }
}

class Record(dimP: Int, dimF: Int) extends Serializable{
  var pos: Array[Double] = new Array[Double](dimP)
  var fitness: Array[Double] = new Array[Double](dimF)
}

class Particle(dimP: Int, dimF: Int, lB: Array[Double], uB: Array[Double]) extends Serializable{
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

//    cur.fitness = PSO.calFitness(cur.pos)
    //    println(cur.pos)
    //    println(cur.fitness)
    this
  }

  def printInfo(): Unit = {
    print("Pos: ")
    for (i <- 0 until dimP) {
      print(cur.pos(i) + " ")
    }

    print("Fitness: ")
    for (i <- 0 until dimF) {
      print(cur.fitness(i) + " ")
    }
    println()
  }
//
//  def updateBest(): Unit = {
//    updateLocal()
//    updateGlobal()
//  }
//
//  def updateLocal(): Unit = {
//    if (PSO.compareTo(cur.fitness, lBest.fitness)) {
//      lBest = cur
//    }
//  }
//
//  def updateGlobal(): Unit = {
//    gB.update(cur)
//  }

//  def updateCur(): Unit = {
  //    updateCurPos()
  //    updateFitness()
  //  }

//  def updateCurPos(): Unit = {
//    for (i <- 0 until dimP) {
//      cur.pos(i) += velocity(i)
//    }
//  }

//  def updateV(): Unit = {
//    val rand = new Random()
//    val r1 = rand.nextDouble()
//    val r2 = rand.nextDouble()
//
//    for (i <- 0 until dimP) {
//      val gBest = gB.select()
//      velocity(i) = w * velocity(i) + c1 * r1 * (gBest.pos(i) - cur.pos(i)) + c2 * r2 * (lBest.pos(i) - cur.pos(i))
//    }
//  }

//  def updateFitness(): Unit = {
//    cur.fitness = PSO.calFitness(cur.pos, data)
//  }
//
//  def run(): Particle = {
//    updateBest()
//    updateCur()
//    updateV()
//
//    this
//  }
}