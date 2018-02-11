package bd_fiber_metrics.utils


import bd_fiber_metrics.entity.Constants
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.aggregate.{Complete, AggregateExpression}
import org.apache.spark.sql.execution.aggregate.ScalaUDAF
import org.apache.spark.sql.{Column, Row}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import redis.clients.jedis.Jedis



class HyperLogLogUDAF(rName:String) extends UserDefinedAggregateFunction{
  private  var jd:Jedis = null
  private val pName =rName

  /**
    * 输入类型
    *
    * @return
    */
  override def inputSchema: StructType ={
     StructType(StructField("value", StringType,true) :: Nil)
  }
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit ={
    buffer(0) = (buffer.getSeq[String](0).toSet + input.getString(0)).toSeq
  }
  /**
    * 聚合的中间过程中产生的数据的数据类型定义
    * @return
    */
  override def bufferSchema: StructType = {
    StructType(StructField("items", ArrayType(StringType, true)) :: Nil)
  }
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit ={
    buffer1(0) = (buffer1.getSeq[String](0).toSet ++ buffer2.getSeq[String](0).toSet).toSeq
  }

  override def initialize(buffer: MutableAggregationBuffer): Unit ={
    buffer(0) = Seq[String]()
  }

  override def deterministic: Boolean = {
    true
  }
  //TODO 需要优化
  override def evaluate(buffer: Row): Any = {
    jd = {
     if (null == jd || !jd.isConnected) {
      val jds = new Jedis(ConfUtils.get(Constants.REDIS_HOSTS,"localhost"),Integer.parseInt( ConfUtils.get(Constants.REDIS_PORTS,"6379")),1000000)
       jds.auth(ConfUtils.get(Constants.REDIS_PASSWORD))
       jds
     }
     else {
       jd
      }
    }
   val pl =  jd.pipelined()
    buffer.getSeq[String](0).foreach( v =>  if (!StringUtils.isEmpty(v)){
    pl.pfadd(pName,v)
   })
    pl.close()
     val dt = jd.pfcount(Array(pName): _*)
    jd.close()
    dt
  }
  override def dataType: DataType = {LongType}



}