import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

import scala.util.Random

/**
  * Created by zhaoxuan on 2017/4/30.
  */
object NewPSO {
  def main(args: Array[String]): Unit = {
//    val warehouseDir = "C:/Users/Zhaoxuan/IdeaProjects/SparkDemo/spark-warehouse"
//    val warehouseDir = "/Users/zhaoxuan/Documents/Programs/IdeaProjects/SparkDemo/spark-warehouse"
//    val ss = SparkSession.builder().appName("PSO").master("local[4]").config("spark.sql.warehouse.dir", warehouseDir).getOrCreate()
    val ss = SparkSession.builder().appName("PSO").getOrCreate()

    //create DataFrame from source file
    val schemaString = "depth latitude longitude richter"
//    val schemaString = "1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17"

    val fields = schemaString.split("\\s").map(fieldName => StructField(fieldName, DoubleType, nullable = true))
    val schema = StructType(fields)

//    val filePath = "/Users/zhaoxuan/Documents/Data/PSO Data/QU.dat"
    val filePath = "hdfs://10.109.247.120:9000/input/pso_test_data_800.txt"
    val rowRDD = ss.sparkContext.textFile(filePath).map(_.split(","))
      .map(line => Row(line(0).toDouble, line(1).toDouble, line(2).toDouble, line(3).toDouble))

    val lineRDD = ss.sparkContext.textFile(filePath).map(_.split(",")).map(_.map(_.toDouble))

    val df = ss.createDataFrame(rowRDD, schema)

    //init particle swarm
    val dimP = 12
    val dimF = 1
    val population = 5
    val iteration = 1
    val length = rowRDD.count()

    val fieldNames = df.schema.fieldNames
    val lB = df.agg(fieldNames(0) -> "min", fieldNames(1) -> "min", fieldNames(2) -> "min", fieldNames(3) -> "min")
      .first().toSeq.toArray.map(_.toString.toDouble)
    val uB = df.agg(fieldNames(0) -> "max", fieldNames(1) -> "max", fieldNames(2) -> "max", fieldNames(3) -> "max")
      .first().toSeq.toArray.map(_.toString.toDouble)

    var particles = new Array[Particle](population)

    for (i <- particles.indices) {
      particles(i) = new Particle(dimP, dimF, lB, uB)
    }

    var particleRDD = ss.sparkContext.parallelize(particles)

    particleRDD = particleRDD.map(_.randInit())

    particles = particleRDD.collect()

    //acc
//    lineRDD.cache()
//    val acc = ss.sparkContext.longAccumulator("count")
//    for (iter <- 0 until iteration) {
//      println(iter)
//      for (i <- particles.indices) {
//        lineRDD.filter(dataFilterArray(_, particles(i).cur.pos)).foreach(line => acc.add(1L))
//
//        val num = acc.value
//
//        acc.reset()
//
//        particles(i).cur.fitness(0) = num.toDouble / 100000
//      }
//    }

    //cache + count
    lineRDD.cache()
    for (iter <- 0 until iteration) {
      println(iter)
      for (i <- particles.indices) {
        val num = lineRDD.filter(dataFilterArray(_, particles(i).cur.pos)).count()

        particles(i).cur.fitness(0) = num.toDouble / 100000
      }
    }

    //collect + length
//    lineRDD.cache()
//    for (iter <- 0 until iteration) {
//      println(iter)
//      for (i <- particles.indices) {
//        val num = lineRDD.filter(dataFilterArray(_, particles(i).cur.pos)).collect().length
//
//        particles(i).cur.fitness(0) = num.toDouble / 100000
//      }
//    }

    //particle
//    val lines = lineRDD.collect()
//    val broadCast = ss.sparkContext.broadcast(lines)
//
//    for (iter <- 0 until iteration) {
//      println(iter)
//      val tmp = particleRDD.map(_.updateFitness(broadCast.value)).collect()
//      particleRDD = ss.sparkContext.parallelize(tmp)
//    }
//
//    val res = particleRDD.collect()
    val res = particles

    for (i <- res.indices) {
      println(res(i).cur.fitness(0))
    }

    ss.stop()
  }

  def dataFilter(row: Row, pos: Array[Double]): Boolean = {
    val array = row.toSeq.toArray.map(_.toString.toDouble)

    var j = 0
    for (i <- array.indices) {
      if (pos(j) < 0.66) {
        if (array(i) < pos(j + 1) || array(i) > pos(j + 2)) {
          return false
        }
      }
      j += 3
    }
    true
  }

  def dataFilterArray(array: Array[Double], pos: Array[Double]): Boolean = {
    var j = 0
    for (i <- array.indices) {
      if (pos(j) < 0.66) {
        if (array(i) < pos(j + 1) || array(i) > pos(j + 2)) {
          return false
        }
      }
      j += 3
    }
    true
  }
}

class Record(dimP: Int, dimF: Int) extends Serializable{
  var pos: Array[Double] = new Array[Double](dimP)
  var fitness: Array[Double] = new Array[Double](dimF)
}

class Particle(dimP: Int, dimF: Int, lB: Array[Double], uB: Array[Double]) extends Serializable{
  private var lBest: Record = new Record(dimP, dimF)
  val cur: Record = new Record(dimP, dimF)
  private val velocity: Array[Double] = new Array[Double](dimP)
  private val w = 0.8
  private val c1 = 0.2
  private val c2 = 0.2

  def randInit(): Particle = {
    val rand = new Random()
    var j = 0
    for (i <- 0 until (dimP / 3)) {
      cur.pos(j) = rand.nextDouble()
      cur.pos(j + 1) = lB(i) + rand.nextDouble() * (uB(i) - lB(i))
      cur.pos(j + 2) = cur.pos(j + 1) + rand.nextDouble() * (uB(i) - cur.pos(j + 1))
      j += 3
    }
    this
  }

  def updateFitness(data: Array[Array[Double]]): Particle = {
    var num = 0.0
    val len = data.length

    for (i <- 0 until len) {
      var k = 0
      for (j <- data(i).indices) {
        if (cur.pos(k) < 0.66) {
          if (data(i)(j) >= cur.pos(k + 1) && data(i)(j) <= cur.pos(k + 2)) {
            num += 1.0
          }
        }
        k += 3
      }
    }

    cur.fitness(0) = num / len

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

//  def run(): Particle = {
//    updateBest()
//    updateCur()
//    updateV()
//
//    this
//  }
}