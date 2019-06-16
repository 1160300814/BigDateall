package lab2Bigdata
import breeze.linalg.DenseVector
import org.apache.spark.{SparkConf, SparkContext}
import breeze.numerics._
import scala.math.Pi

object GNB {

  def distributiveFunc(mean: Double, variance: Double)(x: Double) : Double = { //柯里化分布函数
    if (variance == 0.0) {
      if (x == mean) 1.0
      else 0.0
    } else {
      1.0 / sqrt(2 * Pi * variance) * exp(- pow(x - mean, 2.0) / (2 * variance))
    }
  }

  def main(args:Array[String]): Unit = {
    val conf = new SparkConf().setAppName("homework").setMaster("local")
    val sc = new SparkContext(conf)
    val File = "./src/main/scala/lab2Bigdata/SUSY.csv"
    val input = sc.textFile(File)

    // 样本数据划分,训练样本占0.8,测试样本占0.2
    val alltrainData = input.map(line => {
      val items = line.split(",")//\\s+
      (items(0).toDouble, DenseVector(items.slice(1, items.length).map(_.toDouble)))
    })
    val dataParts = alltrainData.randomSplit(Array(0.8, 0.2))
    val trainData = dataParts(0).groupByKey()
    val testData = dataParts(1).cache()

    val sampleN = alltrainData.count()//input
    val classN = 2.0 //0 or 1
    val lambda = 1.0
    //计算各类的出现概率(注意平滑因子lambda)
    val allpi = trainData.cache().map{case (c, a) => {
      val p = (a.toList.length * 1.0 + lambda) / (sampleN + lambda * classN)
      (c, log2(p)) //取对数,防止后期出现连乘(小数连乘容易精度丢失)
    }}

    val pji = trainData.cache().mapValues(a => {//b.mapValues("x" + _ + "x").collect
      val aSum = a.reduce((v1 ,v2) => v1 + v2) //求总数
      val aSampleN = a.toArray.length //求总数
      val mean = aSum / (aSampleN * 1.0) //求均值
      val variance = a.map(i => { //求方差(去中心化->求和->求均值)
        ((i - mean)*(i - mean))
      }).reduce((v1 ,v2) => v1 + v2) / (aSampleN * 1.0)
      val paras = mean.toArray.zip(variance.toArray)
      paras.map(p => distributiveFunc(p._1, p._2)_) //返回(类别,[特征1的分布函数, ..., 特征n的分布函数])
      })
    println("Finish Train ! Start Test : ")

    val model = new GuassianNBModel(allpi.collectAsMap(), pji.collectAsMap())
    val reasult = testData.map(x => {
      val res = model.predict(x._2)
      var count = 0
      if(res._2 == x._1){//right
        count = 1
      }
//      println("true is " + x._1 + ", predict is " + res._2 + ", score = " + pow(2, res._1))
      count
    })
    val alltestcount = reasult.count()
    val rightcount = reasult.sum()
    val rightRate = rightcount/alltestcount
    val writeText = "RightNum: " + rightcount+"\r\n"+"allNum: " + alltestcount+"\r\n"+"rate: " + rightRate
    println(writeText)
    sc.makeRDD(writeText).saveAsTextFile("./src/main/scala/lab2Bigdata/GNB_result.txt")
  }
}
class GuassianNBModel(val pi:collection.Map[Double, Double], val pji:collection.Map[Double, Array[Double => Double]]) extends Serializable {

  def predict(features: DenseVector[Double]) = {
    pji.map{case (label, models) => {
      val score = models.zip(features.toArray).map{case (m, v) => {
        log2(m(v)) //取对数,防止后期出现连乘(小数连乘容易精度丢失)
      }}.sum + pi(label)
      (score, label) //返回(log(P(F1...Fn|Label)*P(Label)), Label)
    }}.max //选概率最大的,其对应的Label就是模型的预测
  }
}
