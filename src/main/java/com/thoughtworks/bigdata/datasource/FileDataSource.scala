package com.thoughtworks.bigdata.datasource

import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._


/**
 * @Classname FileDataSource
 * @Description TODO
 * @Date 2021/9/16 20:32
 * @Created by imp
 */
class FileDataSource(config: java.util.Map[String, String], spark: SparkSession) extends BaseDb[DataFrame](config, spark) {

  override def read(dataType: String, table: String): DataFrame = {
    if (StringUtils.isEmpty(table) || StringUtils.isEmpty(dataType)) {
      throw new RuntimeException(s"file is empty please check!!!")
    }
    dataType match {
      case "csv" => spark.read.csv(table)
      case "text" => spark.read.parquet(table)
      case _ => spark.read.text(table)
    }
  }

  override def dataFrameWrite(dataType: String, dataFrame: DataFrame, table: String): Unit = ???

  /**
   *
   * @param rdd rdd数据 里面行数据以","拼接
   * @param insertSql
   * @param map 方法配置项
   * @return
   */
  override def rddWrite[U](rdd: RDD[U], insertSql: String, map: Map[String, Any], table: String): Unit = ???
}
