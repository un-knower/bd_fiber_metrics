package bd_fiber_metrics.utils

import java.sql.{ResultSet, PreparedStatement, DriverManager, Connection}


import bd_fiber_metrics.entity.{OrderInfo, TotalInfo, Constants}

import org.apache.commons.lang3.StringUtils
import org.apache.spark.internal.Logging

import org.json.JSONObject
import redis.clients.jedis.Jedis

import scala.collection.mutable


class JdbcUtils  extends Logging{
  // 默认mysql的驱动
  val driver = ConfUtils.get(Constants.MYSQL_JDBC_DRIVER, "com.mysql.cj.jdbc.Driver")

  /**
    *
    * @return 获取 数据库的连接
    */
  def getCon(url: String, user: String, pwd: String): Connection = {
    Class.forName(driver)
    DriverManager.getConnection(url, user, pwd)
  }

  /**
    *
    * 获取展现表的连接
    *
    * @return
    */
  def getBiCon(): Connection = {
    getCon(ConfUtils.get(Constants.BI_JDBC_URL), ConfUtils.get(Constants.BI_JDBC_USER), ConfUtils.get(Constants.BI_JDBC_PASSWORD))
  }

  /**
    * 获取初始化订单的连接
    *
    * @return
    */
  def getDataCon(): Connection = {
    getCon(ConfUtils.get(Constants.DATA_JDBC_URL), ConfUtils.get(Constants.DATA_JDBC_USER), ConfUtils.get(Constants.DATA_JDBC_PASSWORD))
  }

  def initRedis(): mutable.HashSet[String] ={
    val set = new  mutable.HashSet[String]()
    log.info(" init user num and bike num to redis")
    val start = System.currentTimeMillis()
    val jd = new Jedis(ConfUtils.get(Constants.REDIS_HOSTS,"localhost"),Integer.parseInt( ConfUtils.get(Constants.REDIS_PORTS,"6379")),1000000)
    jd.auth(ConfUtils.get(Constants.REDIS_PASSWORD))
    val pl = jd.pipelined()
    val conn = getDataCon()
    var pst: PreparedStatement = null
    var rs: ResultSet = null
    try {
      pst = conn.prepareStatement("select country, user_id, (case when order_status in (20,40,42,49,50) then bike_no end) su_bike_no, bike_no  from inter_order")
      rs = pst.executeQuery()
      while (rs.next()) {
        val country = rs.getString("country")
        set.+=(country)
        val userId = rs.getInt("user_id").toString
        if(!StringUtils.isEmpty(userId)){
          pl.pfadd(country+"_streaming_all_user",userId)
        }
        val suBikeNo= rs.getString("su_bike_no")
        if(!StringUtils.isEmpty(suBikeNo)){
          pl.pfadd(country+"_streaming_su_bikeNo",suBikeNo)
        }
        val bikeNo= rs.getString("bike_no")
        if(!StringUtils.isEmpty(bikeNo)){
          pl.pfadd(country+"_streaming_bikeNo",bikeNo)
        }
      }
      val end = System.currentTimeMillis()
      log.info(s"cost ${(end -start) / 1000 } s")
      pl.close()
    }
    catch {
      case e: Exception => {
        rs.close()
        pst.close()
        throw new RuntimeException(e)
      }
    }
    finally {
      jd.disconnect
      conn.close()
    }
    log.info(s" the set is ${set}")
    set
  }
  /**
    * 获取订单表里的总用户 成功车辆数 总订单车辆数
    *
    * @return
    */
  def getOrderInfo(): TotalInfo = {
    val conn = getDataCon()
    var pst: PreparedStatement = null
    var rs: ResultSet = null
    var t_info = TotalInfo(0, 0, 0)
    try {
      pst = conn.prepareStatement("select count(distinct user_id) user_num,count(distinct case when order_status in (20,40,42,49,50) then bike_no end) su_bike_num,count(distinct bike_no ) bike_num from inter_order")
      rs = pst.executeQuery()
      while (rs.next()) {
        t_info = TotalInfo(rs.getInt("user_num"), rs.getInt("su_bike_num"), rs.getInt("bike_num"))
      }
    }
    catch {
      case e: Exception => {
        rs.close()
        pst.close()
        throw new RuntimeException(e)
      }
    }
    finally {
      conn.close()
    }
    t_info
  }


}

object JdbcUtils {
  def apply() = new JdbcUtils()

  def main(args: Array[String]) {
    val ll = Array((1, "one"), (2, "two"), (3, "three"))
    val t = ll.map(l => s"${l._1}:${l._2}").mkString(",")
    println(t)
    for ( i <- 0 to 100) {
      val s =
        s"""
        |{
        |    "orderId": 1457,
        |    "userId": 1457,
        |    "bikeNo": "6070611000${i}",
        |    "lockType": 5,
        |    "lockImei": "0102606405C6289D",
        |    "orderStatus": ${i},
        |    "rideTime": 0,
        |    "rideDistance": 0,
        |    "paidPrice": null,
        |    "currency": "CAD",
        |    "createTime": 1512972403000,
        |    "updateTime": 1512972403301,
        |    "startTime": null,
        |    "endTime": null,
        |    "bikeTag": "",
        |    "country": "CAN",
        |    "city": 0,
        |    "startGeo": "geo:31.2222040,121.3780756,8.11931991577148;mt=1;u=256.376",
        |    "endGeo": null,
        |    "basePrice": null,
        |    "taxRates": null,
        |    "timestamp": 1512972403306
        |}
      """.stripMargin

    println(
      ConfUtils.parseString(s).getOrElse("parse error ======================"))
      }
    //println(ConfUtils.parseString(s))
     //val js = new JSONObject(s)
    // println(js.getString("basePrice"))

    //JdbcUtils().initRedis()
/*    println( s" ${js.getInt("orderId")} -- ${js.getInt("userId")} -- ${js.getString("bikeNo")} --${js.getInt("lockType")}" +
      s" -- ${js.getString("lockImei")} -- ${js.getInt("orderStatus")} -- ${js.getInt("rideTime")} -- ${js.getDouble("rideDistance")} " +
      s"-- ${js.getDouble("paidPrice")} -- ${js.getString("currency")} -- ${js.getLong("createTime")} -- ${js.getLong("updateTime")} " +
      s"-- ${js.getLong("startTime")} -- ${js.getLong("endTime")} -- ${js.getString("bikeTag")} -- ${js.getString("country")} -- ${
        js.getInt("city")
      } -- ${js.getString("startGeo")} -- ${js.getString("endGeo")} -- ${js.getDouble("basePrice")} -- ${js.getDouble("taxRates")}")*/

  }

}
