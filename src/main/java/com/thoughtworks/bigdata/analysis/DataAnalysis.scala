package com.thoughtworks.bigdata.analysis

/**
 * @Classname DataAnalysis
 * @Description TODO
 * @Date 2021/9/16 20:17
 * @Created by imp
 */


import com.thoughtworks.bigdata.common.CommonParam
import com.thoughtworks.bigdata.datasource.DBFactory.{map, spark}
import com.thoughtworks.bigdata.datasource.{BaseDb, DBFactory}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.InputDStream

import scala.reflect.ClassTag

/**
 *
 * @ClassName: com.thoughtworks.bigdata.analysis.DataAnalysis
 * @Description: 数据分析接口类
 * @Author: 何军红
 * @Time: 20201/9/16 18:21
 * @Version: 1.0
 */
abstract class DataAnalysis(param: java.util.Map[String, String], sparkSession: SparkSession) extends CommonParam(param){

  val kakfaDataSource: BaseDb[InputDStream[ConsumerRecord[String, String]]] = new DBFactory()
    .createDatabase("kafka", spark, map, classOf[InputDStream[ConsumerRecord[String, String]]])

  /**
   *
   * @return
   */
  def getJobParam


  /**
   * 业务分析实现
   *
   * @return
   */
  def analysis(): Unit

  /**
   * 结果入库
   */

  def resultSave[U: ClassTag](rdd: RDD[U], insertDbType: String, tableName: String): Boolean

  def resultSave(df: DataFrame, insertDbType: String, tableName: String): Boolean
}

