package bd_fiber_metrics.utils

import bd_fiber_metrics.entity.Constants
import kafka.common.TopicAndPartition
import org.apache.spark.internal.Logging
import org.apache.spark.streaming.kafka.OffsetRange

import scala.collection.{mutable, immutable}

/** *
  * 偏移量的管理类
  */
class OffsetManager extends Logging{
  /** *
    * 写偏移量 到 mysql
    */
  def writePartition(offsetRanges: Array[OffsetRange], start: Boolean) = {
    val start_time = System.currentTimeMillis()
    val sql: StringBuffer = new StringBuffer(s"replace  into ${Constants.OFFSET_TABLE} values ")
    if (start) {
      for (of <- offsetRanges) {
        sql.append(s" ( '${of.topic}' , ${of.partition} ,${of.fromOffset}) ,")
      }
    }
    else {
      for (of <- offsetRanges) {
        sql.append(s" ( '${of.topic}' , ${of.partition} ,${of.untilOffset}) ,")
      }
    }
    sql.replace(sql.length() - 1, sql.length(), "")
    writeMysql(sql.toString)
    val end_time = System.currentTimeMillis()
    log.info(s" cost ${(end_time - start_time) / 1000} s save offset")

  }

  def writeMysql(sql: String) = {
    log.debug(s"save offset sql ${sql}")
    val conn = JdbcUtils().getBiCon()
    try {
      val pst = conn.prepareStatement(sql)
      pst.executeUpdate()
    } catch {
      case e: Exception => throw new RuntimeException(e)
    }
    finally {
      conn.close()
    }
  }

  /**
    * 启动的时候读取偏移量
    */
  def readOffset(topics:Seq[String]): Option[Map[TopicAndPartition,Long]] ={
    val sql:StringBuffer = new StringBuffer(s"select * from ${Constants.OFFSET_TABLE} where ")
    val sqlappend = new StringBuffer(" ")
    for ( i <- 0 until topics.size){
      sqlappend.append(s" topic = '${topics(i)}' or ")
    }
    sqlappend.replace(sqlappend.length()-3,sqlappend.length(),"")
    sql.append(sqlappend)
    val map =new mutable.HashMap[TopicAndPartition,Long]
    val conn = JdbcUtils().getBiCon
      val ps =   conn.prepareStatement(sql.toString)
      val rs =  ps.executeQuery()
      while(rs.next()){
      log.info (s"get the topic is ${rs.getString("topic")} partition is ${rs.getInt("partition")} offset is ${rs.getInt("offset")} ")
    map.+=((TopicAndPartition(rs.getString("topic"),rs.getInt("partition")),rs.getInt("offset")))
    }
    rs.close()
    ps.close()
    conn.close()
    if(map.size > 0) {
      Some(map.toMap)
    }
    else {
      None
    }
  }

}

object OffsetManager {
  def apply() = new OffsetManager()


}