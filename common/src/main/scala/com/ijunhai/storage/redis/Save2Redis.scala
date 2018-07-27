package com.ijunhai.storage.redis

import com.ijunhai.common.logsystem.JunhaiLog
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.bson.Document

object Save2Redis {
  val FORMAT_STR = "yyyy-MM-dd"
  val ORDER = "order"
  val LOGIN = "login"
  val REGISTER = "register"
  val PARTITION_NUM = 10

  /**
    * 将数据存储到redis中
    *
    * @param rdd     数据
    * @param service 服务 独代，SDK，渠道
    *                数据源（实时以及批处理）
    *                独代日志（login）
    *                大蓝/海外 login日志（login）,user(register)/order(order)数据库
    */
//  def saveDStream(rdd: RDD[(Document)], service: String, redisSinkCluster: Broadcast[RedisSink]): RDD[Document] = {
////    if (!rdd.isEmpty()) {
////      rdd.repartition(PARTITION_NUM).mapPartitions(p => {
////        val result = p.map(log => {
////          val event = log.getString(JunhaiLog.event)
////          val agent = log.get(JunhaiLog.agent).asInstanceOf[Document]
////          val game = log.get(JunhaiLog.game).asInstanceOf[Document]
////          val headInfo = event match {
////            case JunhaiLog.eventActive | JunhaiLog.eventCrash =>
////              if (JunhaiLog.agentName.contains(service)) {
////                agent.getString(JunhaiLog.channel_id) + "_" + JunhaiLog.getSecondColumnString(log, JunhaiLog.device, JunhaiLog.device_id)
////              } else if (JunhaiLog.channelsWithOutDalan.contains(service)) {
////                service + "_" + JunhaiLog.getSecondColumnString(log, JunhaiLog.game, JunhaiLog.game_id) + "_" + JunhaiLog.getSecondColumnString(log, JunhaiLog.device, JunhaiLog.device_id)
////              } else if (service == JunhaiLog.serviceSDK) {
////                game.getString(JunhaiLog.game_name) + "_" + JunhaiLog.getSecondColumnString(log, JunhaiLog.junhai_sdk, JunhaiLog.app_id) + "_" + JunhaiLog.getSecondColumnString(log, JunhaiLog.device, JunhaiLog.device_id)
////              } else {
////                service + log.get(JunhaiLog.device).asInstanceOf[Document].getString(JunhaiLog.device_id)
////              }
////
////            case _ =>
////              if (JunhaiLog.agentName.contains(service)) {
////                if (log.get(JunhaiLog.user) == null || agent == null)
////                  println(log)
////                agent.getString(JunhaiLog.channel_id) + "_" + JunhaiLog.getSecondColumnString(log, JunhaiLog.game, JunhaiLog.game_id) + "_" + JunhaiLog.getSecondColumnString()
////              }
////          }
////
////        })
////        result
////      })
////    }
//  }

  /**
    * 判断任务是否正在运行
    *
    * @param key  streaming应用标识
    * @param uuid 当前streaming任务的间隔时间
    * @param second
    * @param redisSinkCluster
    * @return
    */
  def isRunning(key: String, uuid: String, second: Int, redisSinkCluster: Broadcast[RedisSink]): Boolean = {
    val value = redisSinkCluster.value.get(key)
    val result = if (value == null) {
      //这个uuid是传过来的，如果redis中不存在，就设置
      redisSinkCluster.value.setex(key, second * 2, uuid)
      uuid
    } else {
      //如果redis已经存在，就取出该uuid
      value
    }
    result != uuid
  }


}
