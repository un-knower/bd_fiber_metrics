package bd_fiber_metrics.utils
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class BloomFilterUDAF extends UserDefinedAggregateFunction {
  val i = 0
  /**
    * 指定输入类型
    *
    * @return
    */
  override def inputSchema: StructType = StructType(Array(StructField("info",StringType,nullable = true)))

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    println(input.size +"===="+ input.schema +" =====================")
    buffer(0) = buffer.getAs[Int](0) + 1
    println(buffer(0) + " update value =======================")
  }

  /**
    * 数据的结构类型
    *
    * @return
    */
  override def bufferSchema: StructType = StructType(Array(StructField("result", IntegerType,true),StructField("rq",IntegerType,true)))

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = { buffer1(0) = buffer1.getAs[Int](0) + buffer2.getAs[Int](0)
    println( buffer1(0) +" merge value =========================" +buffer1(1) )
    println( buffer2(0) +" merge value =========================" +buffer2(1) )
  }

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0)=i
    buffer(1)=0
  println(buffer(0) +" buffer value ======================"+buffer(1))}

  override def deterministic: Boolean = true

  override def evaluate(buffer: Row): Any = {
    println(buffer(0) +" evaluate value ======================"+buffer.schema + "  "+buffer.size +"  "+buffer.get(0) +"   "+ buffer(1))
    println(buffer+" evaluate value ======================")
    buffer.getAs[Int](0)
  }
  /**
    * 指定聚合操作时所要处理的数据结果的类型
    *
    * @return
    */
  override def dataType: DataType =IntegerType
}