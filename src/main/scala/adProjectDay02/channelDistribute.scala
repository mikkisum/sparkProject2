package adProjectDay02

import adProjectDay02.sparkUtils.RqtUtils.RqtUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

object channelDistribute {
  def main(args: Array[String]): Unit = {

    //建立连接
    val conf = new SparkConf().setAppName(this.getClass.getName).setMaster("local[*]")
      // 设置序列化方式 采用Kyro序列化方式，比默认序列化方式性能高
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    //获取数据
    val df: DataFrame = sQLContext.read.parquet("E://AAA")
    // 取出需要的字段
    val data: RDD[(Int, List[Double])] = df.map(row => {
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

      val k = row.getAs[Int]("adplatformproviderid")

      //创建3个值对应处理9个指标
      val request: List[Double] = RqtUtils.request(requestmode, processnode)
      val click: List[Double] = RqtUtils.click(requestmode, iseffective)
      val ad: List[Double] = RqtUtils.Ad(iseffective, isbilling, isbid, iswin, adorderid, WinPrice, adpayment)
      val bigList = request ::: click ::: ad
      (k, bigList)

    })

    data
    //写指标
    val res: RDD[(Int, List[Double])] = data.reduceByKey((x, y) => (x zip y).map(t => t._1 + t._2))
    //输出
    res.sortBy(t => t._2(0), false).collect().toBuffer.foreach(println)

  }
}
