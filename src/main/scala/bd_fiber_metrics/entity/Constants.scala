package bd_fiber_metrics.entity
object Constants{
  //展示相关的库
  val BI_JDBC_URL="bi.jdbc.url"
  val BI_JDBC_USER="bi.jdbc.user"
  val BI_JDBC_PASSWORD="bi.jdbc.password"
  val BI_TABLE="ubike_inter_metrics"

  val MYSQL_JDBC_DRIVER="mysql.jdbc.driver"
 //数据库来源层
  val DATA_JDBC_URL="data.jdbc.url"
  val DATA_JDBC_USER="data.jdbc.user"
  val DATA_JDBC_PASSWORD="data.jdbc.password"
  val DATA_TABLE="inter_order"

  //保存偏移量的
  val OFFSET_TABLE = "offset_store"

  val DATA_RESULT_SAVE_MODE="data.result.save.mode"

 // kafka的信息
  val BOOTSRAP_SERVERS="kafka.bootstrap.servers"
  val GROUP_ID  ="kafka.group.id"
  val TOPICS = "kafka.topics"

  //redis 的信息
  val REDIS_HOSTS="redis.hosts"
  val REDIS_PORTS="redis.ports"
  val REDIS_PASSWORD="redis.password"

}