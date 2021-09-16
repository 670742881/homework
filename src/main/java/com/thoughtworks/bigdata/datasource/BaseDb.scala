package com.thoughtworks.bigdata.datasource

import com.thoughtworks.bigdata.common.CommonParam
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 *
 * @ClassName: com.thoughtworks.bigdata.datasource.BaseDb
 * @Description: 数据库操作基类
 * @Author: 何军红
 * @Time: 2020/11/5 9:26
 * @Version: 1.0
 */
abstract class BaseDb[T <: Any](config: java.util.Map[String, String], spark: SparkSession) extends CommonParam(config) {


  def read(dataType: String, table: String): T

  /**
   *
   * @param rdd       rdd数据 里面行数据以","拼接
   * @param insertSql 插入的预编译sql    "insert into syslog(action, event, host, insertTime, userName, update_Time) values(?,?,?,?,?,?)"
   * @param map       方法配置项
   * @return
   */
  def rddWrite[U](rdd: RDD[U], insertSql: String, map: Map[String, Any], table: String): Unit


  def dataFrameWrite(dataType: String, dataFrame: DataFrame, table: String): Unit

}
