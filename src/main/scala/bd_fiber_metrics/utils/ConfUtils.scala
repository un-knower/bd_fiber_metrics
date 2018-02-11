package bd_fiber_metrics.utils
import java.io.InputStream
import java.util.Properties

import bd_fiber_metrics.entity.OrderInfo

import org.apache.commons.lang3.StringUtils
import org.apache.spark.internal.Logging
import org.json.JSONObject


object ConfUtils extends  Logging{
  private val path = "/conf.properties"
  private val prop: Properties = new Properties()
 private var jsonObject:JSONObject = null


  /** *
    * 加载默认的文件配置
    */
  private val loadProperties = {
    if (prop.isEmpty) {
      this.synchronized {
        var is: InputStream = null
        try {
          is = this.getClass.getResourceAsStream(path)
          prop.load(is)
        }
        catch {
          case e: Exception => throw new RuntimeException(e)
        }
        finally {
          is.close()
        }
      }
    }
    prop

  }

  /** *
    * 获取配置文件的值
    *
    * @param key 传入获取的值　
    */
  def get(key: String): String = {
    log.debug(s"the key is ${key} the value is ${prop.getProperty(key)}")
    prop.getProperty(key)

  }

  /** *
    *
    * @param key   　传入获取的值　
    * @param value 　如果获取的值得不到，返回默认值　
    * @return
    */
  def get(key: String, value: String): String = {
    log.debug(s"the key is ${key} the value is ${prop.getProperty(key, value)}")
    prop.getProperty(key, value)

  }

  /**
    * 把一些需要z转化成long 或者 double 等类型的值 都转化为0 防止转化失败异常
    * @param v
    * @return
    */
  def forNullFefault(v: Object):String  = {
    if ( v.equals("") || v.toString.equalsIgnoreCase("null")) {
      "0"
    }
    else{
      v.toString
    }
  }
  /**
    * 得到解析后的类
    * @param value
    * @return
    */
  //TODO 解析出错的日志记录到Mongo里面
   def parseString(value:String): Option[OrderInfo] = {
     try {
         jsonObject  =  new JSONObject(value)
         if( jsonObject.getString("bikeNo").equals("null") || StringUtils.isEmpty(jsonObject.getString("bikeNo")) ) {
           log.info(s" bikeNo is  illegal ,the record is ${jsonObject}")
           return None
         }
         else  if( jsonObject.getString("userId").equals("null") || StringUtils.isEmpty(jsonObject.getString("userId")) ) {
           log.info(s" userId is  illegal ,the record is ${jsonObject}")
           return None
         }
         Some(OrderInfo( Integer.parseInt(forNullFefault(jsonObject.getString("orderId"))),Integer.parseInt(forNullFefault(jsonObject.getString("userId"))),jsonObject.getString("bikeNo"),Integer.parseInt(forNullFefault(jsonObject.getString("lockType"))),
         jsonObject.getString("lockImei"),Integer.parseInt(forNullFefault(jsonObject.getString("orderStatus"))),Integer.parseInt(forNullFefault(jsonObject.getString("rideTime"))),forNullFefault(jsonObject.getString("rideDistance")).toDouble,
         forNullFefault(jsonObject.getString("paidPrice")).toDouble,jsonObject.getString("currency"),forNullFefault(jsonObject.getString("createTime")).toLong, forNullFefault(jsonObject.getString("updateTime")).toLong,
         forNullFefault(jsonObject.getString("startTime")).toLong,forNullFefault(jsonObject.getString("endTime")).toLong, jsonObject.getString("bikeTag"),jsonObject.getString("country"),
         Integer.parseInt(forNullFefault(jsonObject.getString("city"))),jsonObject.getString("startGeo"),jsonObject.getString("endGeo"),forNullFefault(jsonObject.getString("basePrice")).toDouble, forNullFefault(jsonObject.getString("taxRates")).toDouble))
     }
     catch {
       case e:Exception => log.warn(s" parse the ${value} is error ",e)
             None
     }
   }

}
