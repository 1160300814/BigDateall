package lab2Bigdata

import breeze.linalg.DenseVector
import breeze.numerics._
import org.apache.spark.{SparkConf, SparkContext}

object Logistic {

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

  def VAdd(a:DenseVector[Double],b:DenseVector[Double]):DenseVector[Double] ={
    var aaddb = Vector[Double]()
    val len = a.length
    for(i <- 0 until len){
      aaddb ++= Vector(a(i)+b(i))
    }
    return DenseVector(aaddb.toArray)
  }

  def VStart(a:Int): DenseVector[Double] ={
    var aaddb = Vector[Double]()
    val len = a
    for(i <- 0 until len){
      aaddb ++= Vector(1.0)
    }
    return DenseVector(aaddb.toArray)
  }

  def main(args:Array[String]): Unit ={
    val conf = new SparkConf().setAppName("homework").setMaster("local")
    val sc = new SparkContext(conf)
    val File = "./src/main/scala/lab2Bigdata/SUSY.csv"
    val input = sc.textFile(File)

    // 样本数据划分,训练样本占0.8,测试样本占0.2
    val alltrainData = input.map(line => {
      val items = line.split(",")//\\s+
      val label1 = items(0).toDouble
      items(0) = "1.0"
      (label1, DenseVector(items.slice(0, items.length).map(_.toDouble)))
    })
    val takeSample = alltrainData.randomSplit(Array(0.002,0.998))

    val dataParts = takeSample(0).randomSplit(Array(0.8, 0.2))
    val trainData = dataParts(0).cache()
    val testData = dataParts(1).cache()
    println("train count: "+trainData.count())
    println("test count: "+testData.count())

    //train
    val alpha = 0.001 //设置梯度的阀值，该值越大梯度上升幅度越大
    val maxCycles = 500 //设置迭代的次数
    var cyclenum = 0
    val oldpre = 0.001
    val nowpre = 1

    val len = trainData.take(1)(0)._2.length //Vector length
    var weights = VStart(len)
    println("Start Weight: "+weights)
    //梯度上升求最优参数
    while(cyclenum < maxCycles){
      println(" Cycle: "+cyclenum)
      val weights1 = trainData.map(x => {
        var ab = Vector[Double]()
        val h = sigmoid(VMult(x._2,weights))
        val error = x._1 - h
        for(ii <- 0 until x._2.length){
          ab ++= Vector(alpha*error*x._2(ii))
        }
        //val partWei = x._2.map(y => y*alpha*error)
        //println(" ( "+h+"  "+error +"  "+ab +" ) ")
        DenseVector(ab.toArray)
      }).reduce((v1 ,v2) => VAdd(v1, v2))
      weights = VAdd(weights1,weights)
      //println(" Cycle: "+cyclenum+" new Weights: "+weights)
      println(weights.toString())
      cyclenum += 1
    }
    sc.makeRDD(weights.toArray).saveAsTextFile("./src/main/scala/lab2Bigdata/Logistic_Weights.txt")
    println("Finish train !")
  }
}
