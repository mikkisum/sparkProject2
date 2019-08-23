package adProjectDay02

import adProjectDay02.sparkUtils.RqtUtils.RqtUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

object ispDistribute {
  def main(args: Array[String]): Unit = {
    //建立连接
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      // 设置序列化方式 采用Kyro序列化方式，比默认序列化方式性能高
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    //获取数据
    val df: DataFrame = sQLContext.read.parquet("E://AAA")
    // 取出需要的字段
    val data: RDD[(String, List[Double])] = df.map(row => {
      val requestmode: Int = row.getAs[Int]("requestmode")
      val processnode = row.getAs[Int]("processnode")
      val iseffective = row.getAs[Int]("iseffective")
      val isbilling = row.getAs[Int]("isbilling")
      val isbid = row.getAs[Int]("isbid")
      val iswin = row.getAs[Int]("iswin")
      val adorderid = row.getAs[Int]("adorderid")
      val WinPrice = row.getAs[Double]("winprice")
      val adpayment = row.getAs[Double]("adpayment")
      //key 值
      var ispname = row.getAs[String]("ispname")
      if (ispname=="未知"){ispname="其他"}
      val key = ispname
      //创建3个值对应处理9个指标
      val request: List[Double] = RqtUtils.request(requestmode, processnode)
      val click: List[Double] = RqtUtils.click(requestmode, iseffective)
      val ad: List[Double] = RqtUtils.Ad(iseffective, isbilling, isbid, iswin, adorderid, WinPrice, adpayment)
      val bigList = request ::: click ::: ad
      (key, bigList)

    })

    data
    //写指标
    val res: RDD[(String, List[Double])] = data.reduceByKey((x,y)=>(x zip y).map(t=>t._1+t._2))
    //输出
    res.sortBy(t=>t._2(0),false).collect().toBuffer.foreach(println)
    //写入到mysql
    val resFinal= res.map(t => {
      result(t._1, t._2(0), t._2(1), t._2(2), t._2(3), t._2(4), t._2(5), t._2(6), t._2(7), t._2(8))
    })
    val dff: DataFrame = sQLContext.createDataFrame(resFinal)
    dff.show()

  }

}
case class result(ispname:String,
                 originalRequests:Double,
                 effectiveRequests:Double,
                 AdRequests:Double,
                 displayTimes:Double,
                 clickTimes:Double,
                 bidingNums:Double,
                 winningNums:Double,
                 DSPadCosume:Double,
                 DSPasCost:Double)

