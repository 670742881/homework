package com.thoughtworks.bigdata.datasource

import java.lang.reflect.Constructor
import java.util

import kafka.utils.Logging
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.InputDStream


class DBFactory[T] extends Logging{

  private val SOURCE_MAP: util.Map[String, String] = new util.HashMap[String, String]() {
    put("00", "BaseDb")
    put("file", "FileDataSource")
    put("kafka", "KafkaDataSource")
    put("phoenix", "PhoenixDb")
  }


  /**
   *
   * @param dbType
   * @param sparkSession
   * @param config
   * @throws
   * @return
   */
  @throws[Exception]
  def createDatabase[T](dbType: String, sparkSession: SparkSession, config: java.util.Map[String, String],t:Class[T]): BaseDb[T] = {
   var baseDb: BaseDb[T] = null
    if (SOURCE_MAP.containsKey(dbType)) {
    //  try {
        val cla: Class[_] = Class.forName(classOf[BaseDb[T]].getPackage.getName + "." + SOURCE_MAP.get(dbType))
        val constructor: Constructor[BaseDb[T]] = cla.getConstructor(classOf[java.util.Map[String, String]], classOf[SparkSession])
          .asInstanceOf[Constructor[BaseDb[T]]]
         baseDb = constructor.newInstance(config, sparkSession).asInstanceOf[BaseDb[T]]
      }/* catch {
        case e: Exception =>
          logger.error("create BaseDb failed!", e)
      }
    }
    else {
      val error = "暂时没有该数据库的操作类:" + dbType
      logger.error(error)
      throw new RuntimeException(error)
    }*/
    baseDb
  }


}

object DBFactory {
  val conf = new SparkConf()
    .setAppName("DataAnalysisTest测试")
  conf.set(
    "spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.sql.crossJoin.enabled", "true")
  val spark = SparkSession.builder()
    .master("local[*]")
   // .enableHiveSupport()
    .config(conf)
    .getOrCreate()

  val context = spark.sparkContext
  context.setLogLevel(
    "ERROR"
  )

  ////"(SELECT s.*,u.name FROM t_score s JOIN t_user u ON s.id=u.score_id) t_score"
  //        // val url = "jdbc:mysql://127.0.0.1:3306/test?user=root&password=root"
  val map = new util.HashMap[String, String]()
  map.put("mysql.url", "jdbc:mysql://172.19.25.12:3306/test?characterEncoding=UTF-8&&zeroDateTimeBehavior=convertToNull&autoReconnect=true&failOverReadOnly=false&connectTimeout=0")
  map.put("mysql.table", "CateRelationAnalysis")
  map.put("mysql.user", "root")
  map.put("mysql.password", "Wmi@2019")
  map.put("hBase.table", "Goods_Relation_Analysis")
  map.put("hBase.bulkLoad", "true")
  map.put("zk.url", "hdp-master,hdp-node1,hdp-node2:2181")
  map.put("phoenix.table", "GOOD_RELATION")

  def main(args: Array[String]): Unit = {
    val db: BaseDb[DataFrame] = new DBFactory()
      .createDatabase("file", spark, map,classOf[DataFrame])

    val db1: BaseDb[InputDStream[ConsumerRecord[String, String]]] = new DBFactory()
      .createDatabase("kafka", spark, map,classOf[InputDStream[ConsumerRecord[String, String]]])
    val value = db1.read("aa", "")
    value
    value.start()
 /*   val value: DataFrame = db.read("csv", "F:\\code\\实测脚本\\homework\\src\\main\\说明")
    value.show(10)*/
  }
}
