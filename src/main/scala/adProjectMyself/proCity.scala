package adProjectMyself

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

object proCity {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName(this.getClass.getName)
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    val df: DataFrame = sQLContext.read.parquet("E:/AAA")
    df.registerTempTable("basedata")
    val res: DataFrame = sQLContext.sql("select count(*),provincename,cityname from basedata group by provincename,cityname")
    //res.coalesce(1).write.mode(SaveMode.Overwrite).partitionBy("provincename","cityname").json("e://output")
    //写入mysql数据库
    val load: Config = ConfigFactory.load()
    val prop = new Properties()
    prop.setProperty("user",load.getString("jdbc.user"))
    prop.setProperty("password",load.getString("jdbc.password"))
    res.write.mode("append").jdbc(load.getString("jdbc.url"),load.getString("jdbc.TableName"),prop)
    sc.stop()

  }

}
