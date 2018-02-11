package bd_fiber_metrics.entity
/**
  *
  * @param userNum 截至目前骑行的用户数(去重 )
  * @param suBikeNum　截至目前产生成功订单的车辆数（去重）
  * @param bikeNum　　截至目前被扫码的车辆数（去重 ）
  */
case class TotalInfo(userNum:Int,suBikeNum:Int,bikeNum:Int)

/**
  * 订单字段的信息
  *
  * @param orderId
  * @param userId
  * @param bikeNo
  * @param lockType
  * @param lockImei
  * @param orderStatus
  * @param rideTime
  * @param rideDistance
  * @param paidPrice
  * @param currency
  * @param createTime
  * @param updateTime
  * @param startTime
  * @param endTime
  * @param bikeTag
  * @param country
  * @param city
  * @param startGeo
  * @param endGeo
  * @param basePrice
  * @param taxRates
  */
case class OrderInfo(orderId :Int,userId:Int,bikeNo:String,lockType:Int,lockImei:String,orderStatus:Int,
                     rideTime:Int,rideDistance:Double,paidPrice:Double,currency:String,createTime:Long,updateTime:Long,
                     startTime:Long,endTime:Long,bikeTag:String,country:String,city:Int,startGeo:String,endGeo:String,
                     basePrice:Double, taxRates:Double)