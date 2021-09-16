package com.thoughtworks.bigdata.entity

/**
 * @Classname TemperatureBean
 * @Description TODO
 * @Date 2021/9/16 23:15
 * @Created by imp
 */

//T1,2020-01-30 19:30:40,27.65;
/**
 * 温度检测对象bean
 *
 * @param sensorType 传感器类型
 * @param dateTime   时间
 * @param value      温度值
 */
case class TemperatureBean(sensorType: String, dateTime: String, value: Double) {
  override def toString: String = s"${sensorType},${dateTime},${value}"
}


/** 油检测bean
 *
 * @param sensorType
 * @param dateTime
 * @param AB
 * @param AE
 * @param CE
 * @param value
 */
//Q1,2020-01-30 19:30:40,AB:38.9,AE:221323,CE:0.00001;
case class OilBean(sensorType: String, dateTime: String, AB: Double, AE: Long, CE: Float, value: Double) {
  override def toString: String = s"${sensorType},${dateTime},${value};"
}