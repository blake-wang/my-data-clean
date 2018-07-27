package com.ijunhai.process

import com.ijunhai.common.TimeUtil
import com.ijunhai.common.logsystem.JunhaiLog
import com.ijunhai.storage.hbase.HbaseSink
import com.ijunhai.storage.redis.RedisSink
import org.apache.spark.broadcast.Broadcast
import org.bson.Document

/**
  * 独代
  * 支付/登录 ：userId_gameChannelId  orderTs  loginTs
  * 激活：deviceId_gameChannelId activeTs
  *
  * 实时处理先去redis取数据，LoginTs为""则去hbase找数据，再为""则直接插入
  */
object saveFirstEvent {


  val FORMAT_STR = "yyyy-MM-dd"
  val ORDER = "order"
  val LOGIN = "login"
  val REGISTER = "register"
  val PARTITION_NUM = 10
  val COLUMNFAMILY = "cf"
  val LOGINTS = "loginTs"
  val ORDERTS = "orderTs"

  val AGENTTABLENAME = "agent_new"
  val HWTABLENAME = "haiwai_new"

  def saveAll(log: Document, hbaseSink: Broadcast[HbaseSink], redisSink: Broadcast[RedisSink], boolean: Boolean=true): Boolean = {
    if(boolean){
      val serverTs = JunhaiLog.getTimestamp(log,JunhaiLog.server_ts)
      val payStatus = JunhaiLog.getString(log,JunhaiLog.pay_status)
      val status = JunhaiLog.getString(log,JunhaiLog.status)
      val isTest = log.getString(JunhaiLog)
      val event = log.getString(JunhaiLog.event)
      val serverDate = TimeUtil.time2DateString(FORMAT_STR,serverTs,TimeUtil.SECOND)
      val (userId, gameChannelId) = if (JunhaiLog.getString(log, JunhaiLog.event) == JunhaiLog.eventLogin) { //支付字段名不同的问题
        (JunhaiLog.getSecondColumnString(log, JunhaiLog.user, JunhaiLog.user_id).toLowerCase,
          JunhaiLog.getSecondColumnString(log, JunhaiLog.agent, JunhaiLog.game_channel_id))
      } else {
        (JunhaiLog.getString(log, JunhaiLog.user_id).toLowerCase,
          JunhaiLog.getString(log, JunhaiLog.game_channel_id))
      }

      val key = event match {
        case JunhaiLog.eventActive =>
          log.append("user-user_id","")
          JunhaiLog.getSecondColumnString(log,JunhaiLog.device,JunhaiLog.device_id) + "_"+gameChannelId
        case JunhaiLog.eventOrder | JunhaiLog.eventLogin | JunhaiLog.eventRegister =>
          userId +"_"+ gameChannelId
        case _ =>
          ""
      }
      val values = redisSink.value.hmget(key,LOGINTS,ORDERTS)
      val redisLoginTs = values.get(0)
      val redisOrderTs = values.get(1)
      val (loginTs, orderTs) = if (redisLoginTs == null) {
        val map = hbaseSink.value.get(AGENTTABLENAME, key, COLUMNFAMILY, LOGINTS, ORDERTS)
        val hbaseLoginTs = map.get(LOGINTS)
        val hbaseOrderTs = map.get(ORDERTS)
        if (hbaseLoginTs != "" && hbaseOrderTs != "") { //hbase有数据，redis没数据，则先插入redis
          redisSink.value.hmset(key, map)
        } else if (hbaseLoginTs != "") {
          redisSink.value.hset(key, LOGINTS, hbaseLoginTs)
        }
        (hbaseLoginTs, hbaseOrderTs)
      } else {
        (redisLoginTs, if (redisOrderTs == null) "" else redisOrderTs)
      }

      val oldTs = if (event.equals(JunhaiLog.eventOrder)) orderTs else loginTs
    }
  }


}
