package adProjectDay02.sparkUtils.RqtUtils

import java.sql.{Connection, DriverManager}
import java.util

object JDBCConnectPoolsDemo {
private val max=10 //设置最大连接数
private val ConnectionNum=10 //设置 每次可以获取几个Connection
private var conNum=0
  //连接数
  private val pool=new util.LinkedList[Connection]()//连接池
  //加载Driver
  def getDriver():Unit={
    if(conNum<max&&pool.isEmpty){
      Class.forName("com.mysql.jdbc.Driver")//根据类的全称拿到类？
    }else if(conNum>=max&&pool.isEmpty){
      print("当前暂无可用Connection")
      Thread.sleep(2000)
      getDriver()//???
    }
  }
  def getConn():Connection={
    if(pool.isEmpty){
      getDriver()
      for(i<- 1 to ConnectionNum){
        val conn=DriverManager.getConnection("jdbc:mysql://hadoop01:3306/sparkTest","root","root")
        pool.push(conn)
        conNum += 1
      }
    }
    val conn:Connection=pool.pop()
    conn
  }
def returnConn(conn:Connection):Unit={
  pool.push(conn)
}
}
