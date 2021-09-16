package com.thoughtworks.bigdata.analysis

import com.thoughtworks.bigdata.entity.{OilBean, TemperatureBean}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag

/**
 * @Classname TemperatureMeasurementAnalysis
 * @Description 传感器检测业务分析
 * @Date 2021/9/16 22:31
 * @Created by imp
 */
class SensorMeasurementAnalysis(config: java.util.Map[String, String], spark: SparkSession) extends DataAnalysis(config, spark) {
  /**
   *
   * @return
   */
  override def getJobParam: Unit = ???

  /**
   * 业务分析实现
   *
   * @return
   */
  override def analysis(): Unit = {
    val dataSource = kakfaDataSource.read(null, "topic")

    val baseData: DStream[String] = dataSource.map(_.value())


    val filterData: DStream[Array[String]] = baseData.flatMap(_.split(";", -1).map(_.split(",", -1)))
      .filter(arr => arr.length > 0 && (arr(0).contains("T1") || arr(0).contains("Q1")))


    val temperatureData: DStream[TemperatureBean] = filterData.filter(arr => arr(0).contains("T1") && arr.length == 3)
      .map(arr => {
        //可以使用反射工具类来替换这种一个个赋值
        TemperatureBean(arr(0), arr(1), arr(2).toDouble)
      })

    val OilData: DStream[OilBean] = filterData.filter(arr => arr(0).contains("Q1") && arr.length == 6)
      .map(arr => {
        //可以使用反射工具类来替换这种一个个赋值
        OilBean(arr(0), arr(1), arr(2).toDouble, arr(3).toLong, arr(4).toFloat, arr(5).toDouble)
      })


    baseData.count().print()
    dataSource.start()


  }

  /**
   * 结果入库
   */
  override def resultSave[U: ClassTag](rdd: RDD[U], insertDbType: String, tableName: String): Boolean

  = ???

  override def resultSave(df: DataFrame, insertDbType: String, tableName: String): Boolean

  = ???
}

/*
如果温度传感器的某次温度值和当日当时平均温度的差别超过5度，需要提醒“温度过低”或者“温度过高”

如果润滑油质量检测传感器符合下面条件需要提醒：

任意指标连续两次以上都比前一个值高10%（只有一次的情况，则不需要报警，有可能是传感器本身的异常值，请参考下面的输入输出数据）*/
object SensorMeasurementAnalysis {

  /**
   * 温度检测
   *
   * @param temperatureData
   */
  def temperatureMeasurement(temperatureData: DStream[TemperatureBean]) = {

    val dayData: DStream[(String, TemperatureBean)] = temperatureData.map(temp => {
      val day = temp.dateTime.substring(0, 11)
      (day, temp)
    })

    val avg: DStream[(String, Double)] = dayData.map {
      case (day, temp) => {
        (day, (temp.value, 1))
      }
    } //叠加和计数同时进行
      .updateStateByKey(updateFunction)
      .map(t => (t._1, (t._2._1 / t._2._2)))

    //T1,2020-01-30 19:00:02,29; 温度过高
    val checkResult: DStream[String] = dayData.join(avg)
      .map {
        case (day, (temp, avg)) => {
          //大于五度
          val builder = new StringBuilder(" ")
          if (temp.value - avg > 5) temp.toString.addString(builder.append("温度过高"))
          else if (temp.value - avg < 5) temp.toString.addString(builder.append("温度过低"))
          }.toString()
      }
    val avgResult = avg.map {
      case (day, avg) => {
        val builder = new StringBuilder("温度: ")
        builder.append(day).append(" ").append(avg).toString()
      }
    }
    (checkResult, avgResult)
  }


  def updateFunction(currentValues: Seq[(Double, Int)], preValues: Option[(Double, Int)]): Option[(Double, Int)] = {
    val currentSum: Double = currentValues.map(t => t._1).sum
    val currentNum: Int = currentValues.map(t => t._2).sum //seq列表中所有value求和
    val preSum: Double = preValues.map(t => t._1).getOrElse(0.0) //获取上一状态值
    val preNum: Int = preValues.map(t => t._2).getOrElse(0)
    Some((currentSum + preSum, currentNum + preNum))
  }


  def oilCheck(temperatureData: DStream[OilBean]): Unit = {
      temperatureData.transformWith(t=>)
  }

}