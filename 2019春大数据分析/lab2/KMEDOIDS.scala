package lab2Bigdata

import breeze.linalg.DenseVector
import org.apache.spark.{SparkConf, SparkContext}

object KMEDOIDS {
  // data => Vector
  def parseVector(line:String):DenseVector[Double]={
    DenseVector(line.split(',').map(_.toDouble))
  }

  //find closest center point
  def closestPoint(p:DenseVector[Double],centers:Array[DenseVector[Double]]): (Int,Double) ={
    var bestIndex = 0
    var closest = Double.PositiveInfinity
    for(i <- 0 until centers.length){
      val tempDis = squaredDis(p, centers(i))
      if(tempDis < closest){
        closest = tempDis
        bestIndex = i
      }
    }
    (bestIndex,closest)
  }
  //Educlidean distance
  def squaredDis(a:DenseVector[Double],b:DenseVector[Double]):Double={
    var dis = 0.0
    for(i <- 1 until a.size){
      dis +=  math.sqrt((a(i)-b(i))*(a(i)-b(i)))
    }
    return dis
  }
  def main(args:Array[String]): Unit = {
    val conf = new SparkConf().setAppName("homework").setMaster("local")
    val sc = new SparkContext(conf)
    val File = "./src/main/scala/lab2Bigdata/USCensus1990.data.txt"
    val input = sc.textFile(File)

    val K = 5 //fen ji lei
    val data = input.map(parseVector _).cache() // jiang du ru de shu ju bian wei xiang liang
    var kPoints = data.takeSample(withReplacement = false,K) // sui ji chou qu

 //   val File1 = "./src/main/scala/lab2Bigdata/km_centers.txt"
 //   val input1 = sc.textFile(File1)
 //   var kPoints =  input1.map(line => {
 //     val items = line.split(",")
 //     DenseVector(items.slice(0, items.length).map(_.toDouble))
 //   }).take(K)
    //println(kPoints)

    val convergeDis = 0.0000001 // biao shi zhong xin dian bu bian
    var tempDis = 1.0
    var tempIteration = 0 //die dai ci shu
    var closest = data.map(x =>{
      val mediumkeys = closestPoint(x, kPoints)
      (mediumkeys._1,(x,1,mediumkeys._2))
    })


    println("random centers:")
    kPoints.foreach(println)
    println("Start KMEDOIDS: ")

    while(convergeDis < tempDis || tempIteration < 500){
      var comparepoint = DenseVector[Double]()
      var sumi = 0.0
      val pointStats = closest.groupByKey().map(x => {
        val num = (math.random*(x._2.size-1-1) +1).toInt
        println("random num: " + num)
        var count = 0
        val pointsnum = x._2.size
        var sumtest = 0.0

        for(arr <- x._2.toArray){ //die dai qi, cacalate sumi and random point
          sumi += arr._3
          if(count == num) {
            comparepoint = arr._1
          }
          count += 1
        }

        println("comparepoint: "+comparepoint)
        for(iii <- x._2.toArray){//die dai qi, cacaulate sumtest
          sumtest += squaredDis(iii._1, comparepoint)
        }

        println("umtest and sumi: "+sumtest +" and " +sumi)
        if(sumtest > sumi){//no update center
          comparepoint = kPoints(x._1)
        }

        (x._1, comparepoint)
      }).sortByKey().take(K)

      println("new center: ")
      pointStats.foreach(println)

      tempDis = 0.0 //ji suan xin jiu zhong xin dian ping fang cha
      for(iii <- 0 until K){//caculate new pints and o;d points distance
        tempDis += squaredDis(kPoints(iii),pointStats(iii)._2)
      }
      for(iii <- 0 until K){//uodate center points
        kPoints(iii) = pointStats(iii)._2
      }
      closest = data.map(x =>{
        val mediumkeys = closestPoint(x, kPoints)
        (mediumkeys._1,(x,1,mediumkeys._2))
      })
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
    }).sortByKey().take(10000)).saveAsTextFile("./src/main/scala/lab2Bigdata/KMEDOIDS_result.utf8")
    sc.makeRDD(kPoints).saveAsTextFile("./src/main/scala/lab2Bigdata/KMEDOIDS_center.txt")
    sc.stop()

  }

}
