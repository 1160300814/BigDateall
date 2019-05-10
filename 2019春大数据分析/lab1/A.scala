package lab1bigdata

import org.apache.spark.{SparkConf, SparkContext}

object A {
  //属性user_birthday和review_date中日期字段随机使用2018-03-21、2018/03/21、March 21, 2019这三种格式
  def Userbir(t: String): String ={
    var retu = ""
    if(t.contains("/")|| t.contains("-")){
      val sp = t.split("-|\\/")
      retu = sp(0) + "-" + sp(1) + "-" + sp(2)
    }else{
      val sp = t.split(",|\\s")
      if(sp(0).equals("January")){
        sp(0) = "01"
      }else if(sp(0).equals("February")){
        sp(0) = "02"
      }else if(sp(0).equals("March")){
        sp(0) = "03"
      }else if(sp(0).equals("April")) {
        sp(0) = "04"
      }else if(sp(0).equals("May")) {
        sp(0) = "05"
      }else if(sp(0).equals("June")){
        sp(0) = "06"
      }else if(sp(0).equals("July")){
        sp(0) = "07"
      }else if(sp(0).equals("August")){
        sp(0) = "08"
      }else if(sp(0).equals("September")){
        sp(0) = "09"
      }else if(sp(0).equals("October")){
        sp(0) = "10"
      }else if(sp(0).equals("November")){
        sp(0) = "11"
      }else if(sp(0).equals("December")){
        sp(0) = "12"
      }else{
        println("ERROR"+sp(0))
        return "ERROR"
      }
      if(sp(1).toInt < 10){
        sp(1) = "0".concat(sp(1))
      }
      retu = sp(2) + "-" + sp(0) + "-" + sp(1)
    }
    //print(retu)
    return retu
  }
  //temperature有华氏和摄氏两种，摄氏
  def Temp(t:String):Double={
    var retu = 0.0
    if(t.endsWith("℃")){
      retu = t.substring(0,t.length()-1).toDouble
    }else if(t.endsWith("℉")) {
      retu = (t.substring(0, t.length()-1).toDouble - 32.0)/1.8
    }
    else{
      retu = -273.0
    }
    return retu
  }
  //属性longitude和latitude的有效范围分别为[8.1461259, 11.1993265]和[56.5824856, 57.750511]，要求过滤掉无效的数据项
  def Tuple(l:(String, Double, Double, Double, String, Double, Double, String, String, String, String, Double, Boolean)): Boolean = {
    return l._2 >= 8.1461259 && l._2 <= 11.1993265 && l._3 >= 56.5824856 && l._3 <= 57.750511
  }
  //属性rating需要进行归一化，转换为0~1。
  def Rat(t:String):Double={
    if(t.contains("?")){
      return -1
    }else{
      return t.toDouble/100.0
    }
  }
  //假设rating在前1%和最后1%为作弊评分和恶意评分，要求过滤掉作弊评分和恶意评分数据。
  def medium(t:(Int, Double, Any), alln : Long):Boolean={
    if(t._1 < 0){//rating = null
      val randomnum = scala.util.Random.nextInt(100)
      randomnum >= 2 && randomnum <= 98
    }else{//select 4she5ru
       //t._1 < (alln*0.99-0.5).toInt && t._1 > (alln*0.01+0.5).toInt
      return t._1 >= (alln.toDouble*0.01 + 0.5).toInt && t._1 <= (alln.toDouble*0.99 - 0.5).toInt
    }
  }

  //give String if String equal null:return -1;else return Double
  def IsNUll(t:String):Double={
    if(t.contains("?")){
            return -1.0
    }else{
            return t.toDouble
    }
  }

  def main(args: Array[String]): Unit ={
        val conf = new SparkConf().setAppName("homework").setMaster("local")
        val sc = new SparkContext(conf)

        val File = "./src/main/scala/lab1bigdata/large_data.utf8"
        val input = sc.textFile(File)

        //bu fen guo lv,biao zhun hua, gui yi hua,
        //fen ceng chou yang, ding yi ge shi
        val fractions: Map[String, Double]=List(("teacher", 0.01), ("writer", 0.01), ("programmer", 0.01),
          ("farmer", 0.01), ("accountant",0.01), ("artist", 0.01),("Manager", 0.01),("doctor", 0.01)).toMap
        val input2 = input.map(line => {
          val l = line.split("\\|")//fen ge
          var flag = true
          for(i<-0 until l.length){
            if(l(i).contains("?")){//pan duan shi fou you null
              flag = false
            }
          }
          val tuple = (l(0), l(1).toDouble, l(2).toDouble, l(3).toDouble, Userbir(l(4)), Temp(l(5)),
            Rat(l(6)), l(7), Userbir(l(8)), l(9), l(10),
            if(l(11).contains("?")) -1.0 else l(11).toDouble, flag)
          (tuple.copy(), Tuple(tuple))
          //bu fen guo lv
        }).filter(_._2).map(x => (x._1._11, x._1)).sampleByKey(false,fractions,0)
          .map(x => (x._2._7,x._2)).sortByKey(false)
        //input2.foreach(println)
        //qi yu guo lv
        //print("ecc")
        //println(input2.filter(_._2._13).count())
        val numall = input2.count()
        println("one : "+numall)
        var ordernum = 0
        val input3 = input2.map(x =>{
          (x._1,x._2)
        }).sortByKey(false).map(x=>{

          if(x._1 < 0){
            (-1, x._1, x._2)
          }else{
            ordernum = ordernum + 1
            (ordernum, x._1, x._2)
          }
        }).filter(medium(_, numall)).map(x => x._3)
        input3.saveAsTextFile("./src/main/scala/lab1bigdata/medium_data.utf8")
        //clear
        //null
        val trueall = input3.filter(_._13)
        println("true : "+trueall.count())
        //user_income library
        val incomelib = trueall.map(x => ((x._10,x._11),x)).groupByKey().map(x => (x._1,x._2.map(x => x._12).sum/x._2.map(x=>1).sum))//.count()
        var containnull = input3.filter(_._13 == false)
        println("false : "+containnull.count())
        //user_income:user_nationality和user_career(同一国家同一职业的人收入接近)
        val income = containnull.filter(x => x._12 < 0).map(x => ((x._10,x._11),x)).join(incomelib).map(y => {
          val x = y._2._1
          (x._1, x._2, x._3, x._4, x._5, x._6, x._7, x._8, x._9, x._10, x._11, y._2._2, x._13)
        })
        containnull = containnull.filter(x => x._12 >= 0).union(income)

        //rating:user_income、longitude、latitude和altitude(收入相近的人对同一地点的评价打分接近)
        val ratinttype = trueall.map(x => ((x._2, x._3,x._4),x._12,x))
        val ratinglib = ratinttype.map(x => ((x._1._1).toInt + (x._1._2).toInt, (x._1._3, x._2)))

        val rating = containnull.filter(x => x._7 < 0).map(x => ((x._2, x._3, x._4), x._12, x))
          .map(x => ((x._1._1).toInt + (x._1._2).toInt, (x._1._3, x._2, x._3)))
          .join(ratinglib).map(x => {
          ((x._2._1._1 - x._2._2._1, x._2._1._2 - x._2._2._2), (x._2._1._3, x._2._2._2)) // sort, tuple, rating sample
        }).sortByKey(true).take(5).map(x =>
          (x._2._1, x._2._2)
          ).groupBy(_._1).map(x => (x._1, x._2.map(x => x._2).sum/x._2.length))
          .map(x => (x._1._1, x._1._2, x._1._3, x._1._4, x._1._5, x._1._6, x._2, x._1._8, x._1._9, x._1._10, x._1._11, x._1._12, x._1._13))
        val rating_rdd = sc.makeRDD(rating.toList)
    //proj_bucket.map(x => (((x._1._1-8.1461259)*70*400).toInt + ((x._1._2-56.5824856)*70).toInt, 1)).reduceByKey(_+_).foreach(println)

    /*
        val dict_fame = ratinttype.map(x => (((x._1._1-8.1461259)*70*400).toInt + ((x._1._2-56.5824856)*70).toInt, (x._1._3, x._2)))

        val rating = containnull.filter(x => x._7 < 0).map(x => ((x._2, x._3, x._4), x._12, x))
          .map(x => (((x._1._1-8.1461259)*70*400).toInt + ((x._1._2-56.5824856)*70).toInt, (x._1._3, x._2, x._3)))
          .join(dict_fame).map(x => {
          ((x._2._1._1 - x._2._2._1, x._2._1._2 - x._2._2._2), (x._2._1._3, x._2._2._2)) // sort, tuple, rating sample
        }).sortByKey(true).take(5).map(x =>
          (x._2._1, x._2._2)
        ).groupBy(_._1).map(x => (x._1, x._2.map(x => x._2).sum / x._2.length))
          .map(x => (x._1._1, x._1._2, x._1._3, x._1._4, x._1._5, x._1._6, x._2, x._1._8, x._1._9, x._1._10, x._1._11, x._1._12, x._1._13))
    */



    containnull = containnull.filter(_._7 > 0).union(rating_rdd)
    //containnull.foreach(println)

    containnull.saveAsTextFile("./src/main/scala/lab1bigdata/cleaning_result.utf8")

    trueall.saveAsTextFile("./src/main/scala/lab1bigdata/true_items.utf8")

      }



  }
