package adProjectDay02.sparkUtils.RqtUtils



object RqtUtils {
  //处理请求数
  def request(requestmode: Int,processnode:Int):List[Double]={
    if(requestmode==1){
      if(processnode==1)List(1,0,0)
      else if(processnode==2)List(1,1,0)
      else List(1,1,1)

    }else List(0,0,0)

  }

  //处理点击数
  def click(requestmode:Int,iseffective:Int):List[Double]={
    if(iseffective==1){
      requestmode match {
        case 2 =>return List(1,0)
        case 3 =>return  List(1,1)
        case _ =>return  List(0,0)

      }

    }
    List(0,0)


  }

  //处理竞价操作
  def Ad(iseffective:Int,isbilling:Int,isbid:Int,iswin:Int,
         adorderid:Int,WinPrice:Double,adpayment:Double):List[Double]={
    if(iseffective==1&&isbilling==1&&isbid==1){
      iswin match {
        case 1 =>adorderid match {
          case 1=>return List(1,1,WinPrice/1000,adpayment/1000)
        }
        case 0 =>return List(1,0,0,0)
      }
    }
    List(0,0,0,0)

  }






































}
