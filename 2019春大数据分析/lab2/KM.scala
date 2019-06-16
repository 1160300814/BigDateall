package lab2Bigdata

import breeze.linalg.DenseVector
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ArrayBuffer

object KM {
  // data => Vector
  def parseVector(line:String):DenseVector[Double]={
    DenseVector(line.split(',').map(_.toDouble))
  }

  //Educlidean distance
  def squaredDis(a:DenseVector[Double],b:DenseVector[Double]):Double={
    var dis = 0.0
    for(i <- 1 until a.size){
      dis +=  math.sqrt((a(i)-b(i))*(a(i)-b(i)))
    }
    return dis
  }

  //find closest center point
  def closestPoint(p:DenseVector[Double],centers:Array[DenseVector[Double]]):Int={
    var bestIndex = 0
    var closest = Double.PositiveInfinity
    for(i <- 0 until centers.length){
      val tempDis = squaredDis(p, centers(i))
      if(tempDis < closest){
        closest = tempDis
        bestIndex = i
      }
    }
    bestIndex
  }

  def main(args:Array[String]): Unit ={
    val conf = new SparkConf().setAppName("homework").setMaster("local")
    val sc = new SparkContext(conf)

    val File = "./src/main/scala/lab2Bigdata/USCensus1990.data.txt"
    val input = sc.textFile(File)

    val K = 5 //fen ji lei
    val data = input.map(parseVector _).cache() // jiang du ru de shu ju bian wei xiang liang
    var kPoints = data.takeSample(withReplacement = false,K) // sui ji chou qu
    val convergeDis = 0.000001 // biao shi zhong xin dian bu bian
    var tempDis = 1.0
    var tempIteration = 0 //die dai ci shu
    var closest = data.map(x => ( closestPoint(x, kPoints),(x,1) ) )


    println("random centers:")
    kPoints.foreach(println)
    println("Start KMeans: ")
    while(convergeDis < tempDis && tempIteration < 20){
      val pointStats = closest.groupByKey().map(x => {//caculate new hcenter point
        var update = Vector[Double]()
        val len = x._2.toArray.take(1)(0)._1.length //Vector
        var arrayBuffer = ArrayBuffer[Double]()
        var cnt =0
        for(ix <- 0 until len){
          arrayBuffer ++= ArrayBuffer(0.0)
        }
        for(arr <- x._2.toArray){ //die dai qi
          val tr = arr._1.toArray //qi zhong xiang liang
          cnt += arr._2
          for(i <- tr.indices){
            arrayBuffer(i) = arrayBuffer(i) +(tr(i)*arr._2.toDouble)
          }
        }
        for(i <- arrayBuffer.indices){
          arrayBuffer(i) = arrayBuffer(i)/cnt.toDouble
          update ++= Vector(arrayBuffer(i))
        }
        (x._1,update)
      })

      //rdd => Array
      var arrayPointS = Array[Vector[Double]]()
      arrayPointS = pointStats.map(x => x._2).cache().take(K)
      //pointStats.take(n)(n-1)
      //println("new center: ")
      //arrayPointS.foreach(println)
      tempDis = 0.0 //ji suan xin jiu zhong xin dian ping fang cha
      for(iii <- 0 until K){//caculate new pints and o;d points distance
          tempDis += squaredDis(kPoints(iii),DenseVector(arrayPointS(iii).toArray)) //DenseVector(pointStats.take(iii)(iii-1)._2.toArray))
      }
      for(iii <- 0 until K){//uodate center points
          kPoints(iii) = DenseVector(arrayPointS(iii).toArray)
      }
      closest = data.map(x => ( closestPoint(x, kPoints),(x,1) ) )
      println("Finished iteration ( "+tempIteration+" , "+tempDis+"), center: ")
      kPoints.foreach(println)
      tempIteration = tempIteration + 1
    }
    println("Final centers:")
    kPoints.foreach(println)

    //result
    sc.makeRDD(closest.map(x => {
      val v1 = x._2._1.toArray
      ((v1(0)-10000).toInt,x._1)
    }).sortByKey().take(10000)).saveAsTextFile("./src/main/scala/lab2Bigdata/order_result.utf8")
    sc.makeRDD(kPoints).saveAsTextFile("./src/main/scala/lab2Bigdata/center_kmeans.txt")
    sc.stop()
  }
}
