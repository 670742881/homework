package com.thoughtworks.bigdata.datasource

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Duration, StreamingContext}

/**
 * @Classname KafkaDataSource
 * @Description TODO
 * @Date 2021/9/16 20:49
 * @Created by imp
 */
class KafkaDataSource(config: java.util.Map[String, String], spark: SparkSession) extends BaseDb[InputDStream[ConsumerRecord[String, String]]](config, spark) {
  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "bigdata:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "offsetSaveRedis",
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
    "enable.auto.commit" -> (true: java.lang.Boolean)
  )

  override def read(dataType: String, table: String) = {

    val streamingContext = new StreamingContext(spark.sparkContext, Duration(2000))

    val topics = Array(table)
    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    stream
  }

  override def dataFrameWrite(dataType: String, dataFrame: DataFrame, table: String): Unit = ???

  /**
   *
   * @param rdd       rdd数据 里面行数据以","拼接
   * @param insertSql 插入的预编译sql    "insert into syslog(action, event, host, insertTime, userName, update_Time) values(?,?,?,?,?,?)"
   * @param map       方法配置项
   * @return
   */
  override def rddWrite[U](rdd: RDD[U], insertSql: String, map: Map[String, Any], table: String): Unit = ???


}
