package bd_fiber_metrics

import java.util

import bd_fiber_metrics.entity.{TotalInfo, Constants}
import bd_fiber_metrics.utils._
import kafka.message.MessageAndMetadata
import kafka.serializer.{Decoder, StringDecoder}
import org.apache.hadoop.fs.viewfs.ConfigUtil
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.{Row, SaveMode, SQLContext, SparkSession}
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.streaming.kafka.{DirectKafkaInputDStream, OffsetRange, HasOffsetRanges, KafkaUtils}
import org.apache.spark.{TaskContext, SparkContext, SparkConf}

import scala.collection.mutable

/**
 * 由于本代码上线比较着急,没有按照java的设计模式，等２期开始的时候
  * 请按照设计规范，高内聚低耦合 以及 方便扩展的模式调整代码
 */
object App  extends  Logging{

  val fm =  OffsetManager()
  val kafkaParams = Map[String, String](
    "bootstrap.servers" -> ConfUtils.get(Constants.BOOTSRAP_SERVERS,"localhost:9092"),
    "group.id" -> ConfUtils.get(Constants.GROUP_ID,"bigdata")
  )
  val topics = ConfUtils.get(Constants.TOPICS).split(",").toSet
  val setRedis =  JdbcUtils().initRedis()
  /**
    * 获取DStream
    */
  def getDirectStream(ssc:StreamingContext) ={
    fm.readOffset(topics.toSeq) match{
      case Some(offsetInfo) =>
        log.info("use mysql offset")
        val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.message)
        KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder,(String,String)](ssc, kafkaParams, offsetInfo, messageHandler)
      case  None =>
        log.info ("use new offset")
        KafkaUtils.createDirectStream[String, String,StringDecoder,StringDecoder](ssc, kafkaParams.+(("auto.offset.reset" -> "largest")), topics)
    }
  }
  def main(args: Array[String]) {
    val conf = new SparkConf()/*.setMaster("local[2]").setAppName("fiber_test")
    conf.set("spark.default.parallelism","3")
    conf.set("spark.sql.shuffle.partitions","18")*/
    val sc = SparkContext.getOrCreate(conf)
    val sparkSession = SparkSession.builder.config(conf).getOrCreate()

    import sparkSession.implicits._
    setRedis.foreach( country => {
      sparkSession.udf.register(country+"countAllUser",new HyperLogLogUDAF(country+"_streaming_all_user"))
      sparkSession.udf.register(country+"countSuBike",new HyperLogLogUDAF(country+"_streaming_su_bikeNo"))
      sparkSession.udf.register(country+"countBike",new HyperLogLogUDAF(country+"_streaming_bikeNo"))
    } )
    //sparkSession.udf.register("mysum",new BloomFilterUDAF)

    val ssc =   new StreamingContext(sc,Duration(60000))
    // 100.114.158.6 order-init order-update   //kafka.sandbox.ubike.bk inter-order-event
     //  inter-order-event
  //  val ds = KafkaUtils.createDirectStream[String, String,StringDecoder,StringDecoder](ssc, kafkaParams, topics)
    val ds = getDirectStream(ssc)
    ds.foreachRDD{ (rdd,time) =>
      //获取0时区时间
      val time2 = time.toString().substring(0,13).toLong - (8 * 60 * 60 *1000)
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      fm.writePartition(offsetRanges,true)
      try {
      val df = rdd.mapPartitions( iter => {
          iter.map( k_m => ConfUtils.parseString(k_m._2) )
        } ).filter(f => f!=None).map( f=> f.get).toDF()
      df.createOrReplaceTempView("order_info")
/*      val data = sparkSession.sql("select country,0 city_name,(case when country='Canada' then CanadacountAllUser(userId) when country='TWN' then TWNcountAllUser(userId) when country='USA' then USAcountAllUser(userId) end ) user_count,count(distinct userId) user_num,count(1) order_num,sum(case when orderStatus in (20,40,42,49,50) then 1 else 0 end) su_order_num" +
        ",  sum( case when paidPrice > 0 then 1 else 0 end) pay_order_num,sum(paidPrice) order_amount,count(distinct case when  orderStatus in (20,40,42,49,50) then bikeNo end)  su_bike_num,(case when  country='Canada' then  CanadacountSuBike(distinct case when  orderStatus in (20,40,42,49,50) then bikeNo end) when  country='TWN' then  TWNcountSuBike(distinct case when  orderStatus in (20,40,42,49,50) then bikeNo end) when  country='USA' then  USAcountSuBike(distinct case when  orderStatus in (20,40,42,49,50) then bikeNo end) end ) bike_num," +
        "count( distinct bikeNo) use_bike_num, (case when country='Canada' then CanadacountBike(bikeNo) when country='TWN' then TWNcountBike(bikeNo) when country='USA' then USAcountBike(bikeNo) end ) use_bike_num_all , " +
        s" ${time.toString().substring(0,13)} compute_time  from order_info where orderStatus not in (0,10,31) group by country")*/
    val data = sparkSession.sql("select country,0 city_name,(case when country='Canada' then CanadacountAllUser(userId) when country='TWN' then TWNcountAllUser(userId) when country='USA' then USAcountAllUser(userId) end ) user_count,approx_count_distinct(userId) user_num,count(1) order_num,sum(case when orderStatus in (20,40,42,49,50) then 1 else 0 end) su_order_num" +
  ",  sum( case when paidPrice > 0 then 1 else 0 end) pay_order_num,sum(paidPrice) order_amount,approx_count_distinct(case when  orderStatus in (20,40,42,49,50) then bikeNo end)  su_bike_num,(case when  country='Canada' then  CanadacountSuBike(distinct case when  orderStatus in (20,40,42,49,50) then bikeNo end) when  country='TWN' then  TWNcountSuBike(distinct case when  orderStatus in (20,40,42,49,50) then bikeNo end) when  country='USA' then  USAcountSuBike(distinct case when  orderStatus in (20,40,42,49,50) then bikeNo end) end ) bike_num," +
  "approx_count_distinct(bikeNo) use_bike_num, (case when country='Canada' then CanadacountBike(bikeNo) when country='TWN' then TWNcountBike(bikeNo) when country='USA' then USAcountBike(bikeNo) end ) use_bike_num_all , " +
  s" ${time2} compute_time  from order_info where orderStatus not in (0,10,31) group by country")
      val map = new mutable.HashMap[String,String]()
      val saveModel = Integer.parseInt(ConfUtils.get(Constants.DATA_RESULT_SAVE_MODE,"0"))
      saveModel match {

        case 0 => {
          map.+=(("url",ConfUtils.get(Constants.BI_JDBC_URL)))
          map.+=(("user",ConfUtils.get(Constants.BI_JDBC_USER)))
          map.+=(("password",ConfUtils.get(Constants.BI_JDBC_PASSWORD)))
          map.+=(("dbtable",Constants.BI_TABLE))
        }
        case 1 => {
          map.+=(("url",ConfUtils.get(Constants.DATA_JDBC_URL)))
          map.+=(("user",ConfUtils.get(Constants.DATA_JDBC_USER)))
          map.+=(("password",ConfUtils.get(Constants.DATA_JDBC_PASSWORD)))
          map.+=(("dbtable",Constants.BI_TABLE))
        }
        case _ => throw  new RuntimeException("unkown  save result methods ")
      }
        try {
          data.coalesce(1).write.format("jdbc").mode(SaveMode.Append).options(map).save()
        }
        catch {
          case e=> throw new RuntimeException(e)
        }
      }
      catch {
        case e:Exception => throw new RuntimeException(e)
      }
      fm.writePartition(offsetRanges,false)
  }
    ssc.start()
    ssc.awaitTermination()

  }


}
