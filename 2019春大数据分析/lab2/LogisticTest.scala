package lab2Bigdata

import breeze.linalg.DenseVector
import breeze.numerics.{exp, log2}
import org.apache.spark.{SparkConf, SparkContext}

object LogisticTest {
  //sigmoid转换函数
  def sigmoid(x:Double):Double = {
    return 1.0/(1+exp(-x))
  }

  def VMult(a:DenseVector[Double],b:DenseVector[Double]):Double ={
    var amultb = 0.0
    for(i <- 0 until a.length){
      amultb += a(i)*b(i)
    }
    return amultb
  }

  def main(args:Array[String]): Unit = {
    val conf = new SparkConf().setAppName("homework").setMaster("local")
    val sc = new SparkContext(conf)
    val File1 = "./src/main/scala/lab2Bigdata/LogisticVector.txt"
    val input1 = sc.textFile(File1)
    var good =  input1.map(line => {
      val items = line.split(",")
      DenseVector(items.slice(0, items.length).map(_.toDouble))
    }).take(1)(0)
    println(good)

    val File2 = "./src/main/scala/lab2Bigdata/SUSY.csv"
    val input2 = sc.textFile(File2)
    val alltestData = input2.map(line => {
      val items = line.split(",")
      val label1 = items(0).toDouble
      items(0) = "1.0"
      (label1, DenseVector(items.slice(0, items.length).map(_.toDouble)))
    })
    val takeSample = alltestData.randomSplit(Array(0.001,0.996))
    val resultdata = takeSample(0).map(x => {//takeSample(0)
      val prelabel = sigmoid(VMult(x._2, good))
      var labelresu = 0.000000000000000000e+00
      if(prelabel > 0.5){
        labelresu = 1.000000000000000000e+00
      }
      var rightcout = 0
      if(x._1==labelresu){
        rightcout = 1
      }
      //println("old: "+x._1+" new: "+labelresu +"  "+x)
      rightcout
    })
    val alltestcount = resultdata.count()
    val rightcount = resultdata.sum()
    val rightRate = rightcount/alltestcount
    val writeText = "RightNum: " + rightcount+"\r\n"+"allNum: " + alltestcount+"\r\n"+"rate: " + rightRate
    println(writeText)
    sc.makeRDD(writeText).saveAsTextFile("./src/main/scala/lab2Bigdata/LogisticTest_result.txt")
  }
}
