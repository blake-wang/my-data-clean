package com.ijunhai.process.agent

import java.util
import java.util.Date

import com.ijunhai.common.TimeUtil
import com.ijunhai.common.ip.{IP, IPAddress}
import com.ijunhai.common.logsystem.JunhaiLog
import com.ijunhai.process.CommProcess
import com.ijunhai.process.agent.ChannelTransform.HWGameMap
import com.ijunhai.process.agent.KafkaLogProcess._
import com.ijunhai.storage.redis.RedisSink
import org.apache.spark.broadcast.Broadcast
import org.bson.Document

object ProcessImpl extends Serializable {

//  def changePhase(doc: Document): (Document, Boolean) = {
//    val appId = JunhaiLog.getSecondColumnString(doc, JunhaiLog.game, JunhaiLog.app_id)
//    val gameId = JunhaiLog.getSecondColumnString(doc, JunhaiLog.game, JunhaiLog.game_id)
//    val document = new Document()
//    for (key <- doc.keySet()) {
//      if (JunhaiLog.game != key) {
//        document.put(key, doc.get(key))
//      }
//    }
//    val newGame = new Document()
//    newGame.put(JunhaiLog.game_id, gameId)
//    newGame.put(JunhaiLog.app_id, appId)
//    newGame.put(JunhaiLog.game_ver, JunhaiLog.getSecondColumnString(doc, JunhaiLog.game, JunhaiLog.game_ver))
//    newGame.put(JunhaiLog.game_name, JunhaiLog.getSecondColumnString(doc, JunhaiLog.game, JunhaiLog.game_name))
//    newGame.put(JunhaiLog.company_id, JunhaiLog.getSecondColumnString(doc, JunhaiLog.game, JunhaiLog.company_id))
//    document.put(JunhaiLog.game, newGame)
//    if (newGame.getString(JunhaiLog.app_id).equals("100000011")) {
//      //海外的特殊belong_game_id,走网页支付，直接过滤掉
//      (document, false)
//    } else {
//      (document, true)
//    }
//  }

  def loginJoin(doc: Document,
                source: String,
                bAcMap: Broadcast[Map[String, String]],
                bGgMap: Broadcast[Map[String, String]],
                bGcMap: Broadcast[Map[String, String]],
                bScMap: Broadcast[Map[String, String]],
                bBscMap: Broadcast[Map[String, (String, String, String)]],
                bGcmMap: Broadcast[Map[String, String]],
                bGameChannel: Broadcast[Map[String, (String, String, String)]],
                bRmb: Broadcast[Map[String, Double]],
                bUsd: Broadcast[Map[String, Double]],
                bIOS128: Broadcast[Set[String]], booleanB: Boolean = true): (Document, Boolean) = {
    if (booleanB) {
      var boolean = true
      source match {
        case "haiwaiLoginSrc" => {
          boolean = doc.get(JunhaiLog.channel_platform) != null &&
            JunhaiLog.getSecondColumnString(doc, JunhaiLog.channel_platform, JunhaiLog.ad_id) != "" &&
            JunhaiLog.getSecondColumnString(doc, JunhaiLog.game, JunhaiLog.game_id) != ""
          val game = doc.get(JunhaiLog.game).asInstanceOf[Document]
          val agent = JunhaiLog.getDocument(doc, JunhaiLog.agent)
          var belongGameId = JunhaiLog.getString(game, JunhaiLog.game_id)
          belongGameId = HWGameMap.getOrElse(belongGameId, belongGameId)
          val (gameChannelId, channelId, gameId) = bGameChannel.value.getOrElse(belongGameId, (error, error, error))
          game.put("app_id", gameId) //这条语句必须要在上面
          game.put(JunhaiLog.company_id, bGcmMap.value.getOrElse(gameId, error))
          agent.put(JunhaiLog.channel_id, channelId)
          agent.put(JunhaiLog.game_channel_id, gameChannelId)
          doc.put(JunhaiLog.agent, agent)
          doc.put(JunhaiLog.game, game)
          JunhaiLog.getDocument(doc, JunhaiLog.device).put(JunhaiLog.os_type, bAcMap.value.getOrElse(channelId, ""))
//          changePhase(doc)
          (doc,true)
        }
        case "dalanLoginSrc" =>
          val game = doc.get(JunhaiLog.game).asInstanceOf[Document]
          val agent = JunhaiLog.getDocument(doc, JunhaiLog.agent)
          val columnValue = JunhaiLog.getString(game, JunhaiLog.game_id)
          val values = bGameChannel.value.getOrElse(columnValue, (error, error, error))
          val gameChannelId = values._1
          val channelId = values._2
          val gameId = values._3
          game.put("app_id", gameId)
          game.put(JunhaiLog.company_id, bGcmMap.value.getOrElse(gameId, error))
          agent.put(JunhaiLog.channel_id, channelId)
          agent.put(JunhaiLog.game_channel_id, gameChannelId)
          doc.put(JunhaiLog.agent, agent)
          doc.remove(JunhaiLog.game)
          doc.put(JunhaiLog.game, game)
          JunhaiLog.getDocument(doc, JunhaiLog.device).put(JunhaiLog.os_type, bAcMap.value.getOrElse(channelId, ""))

          try {
            if (gameId.toInt < 128 && CommProcess.DALAN_CHANNEL.contains(channelId)) {
//              changePhase(doc)
              (doc, true)
            } else {
              (doc, false)
            }
          } catch {
            case _: Exception =>
              (doc, false)
          }

        case "agentLoginSrc" =>
          val game = doc.get(JunhaiLog.game).asInstanceOf[Document]
          val gameId = game.getString(JunhaiLog.game_id)
          val device = doc.get(JunhaiLog.device).asInstanceOf[Document]
          boolean = doc.get(JunhaiLog.agent) != null &&
            gameId != "" && !(doc.get(JunhaiLog.agent).asInstanceOf[Document].getString(JunhaiLog.channel_id) == ""
            && doc.get(JunhaiLog.agent).asInstanceOf[Document].getString(JunhaiLog.game_channel_id) == "")
          val agent = doc.get(JunhaiLog.agent).asInstanceOf[Document]
          val channelId = agent.getString(JunhaiLog.channel_id)
          val gameChannelId = agent.getString(JunhaiLog.game_channel_id)
          if (boolean) {
            if (channelId.equals("")) {
              val agentChannelId = bGgMap.value.getOrElse(gameId + gameChannelId, error)
              agent.put(JunhaiLog.channel_id, agentChannelId)
              val pf = bAcMap.value.getOrElse(agentChannelId, "")
              device.put(JunhaiLog.os_type, pf)
            } else if (gameChannelId.equals("") || gameChannelId.equals("0")) {
              //独代日志，game_id>=63,gameChannelId为空的已经在前面全部过滤了
              if (!bScMap.value.contains(channelId)) {
                //http://game.data.ijunhai.com/Gamedata/api/getAgentSubChannel
                //此时order表中的channel_id就是channel_id
                val value = bGcMap.value.getOrElse(gameId + channelId, error)
                val pf = bAcMap.value.getOrElse(channelId, "")
                agent.put(JunhaiLog.game_channel_id, value)
                device.put(JunhaiLog.os_type, pf)
              } else {
                //有子渠道subchannel的关联
                val agentChannelId = bScMap.value.getOrElse(channelId, error) //通过subchannel获得channel_id
                agent.put(JunhaiLog.channel_id, agentChannelId)
                //出现subchannelId匹配不到的情况，则直接使用agentchannelId去匹配得到gamechannelid
                val agentValue = bGcMap.value.getOrElse(gameId + agentChannelId, error)
                agent.put(JunhaiLog.game_channel_id, agentValue)
                val pf = bAcMap.value.getOrElse(agentChannelId, "")
                device.put(JunhaiLog.os_type, pf)
              }
            }
            //            val companyId = bGcmMap.value.getOrDefault(gameId, error) //游戏公司关联
            //            game.put(JunhaiLog.company_id, companyId)
          }
          //提出haiwai渠道的数据
//          boolean = !JunhaiLog.hawaiChannelId.contains(doc.get(JunhaiLog.agent).asInstanceOf[Document].getString(JunhaiLog.channel_id))
          (doc, boolean)
        case _ =>
          (new Document(), false)
      }
    } else {
      (doc, false)
    }
  }


  val error = "error"
  val firstStr = "NORMAL: ["
  val secondStr = "] CIP["

  val BYTES = "bytes"
  val NUMS = "nums"
  val ERROR = "error"


  def getData(Str: String, thirdStr: String, append: Boolean = false): (Document, Boolean) = {
    try {
      val doc = Document.parse(Str)
      val msg = JunhaiLog.getString(doc, "message")
      val hostName = JunhaiLog.getString(doc, "host")
      if (append) { //是否获得ip和ts
        val time = msg.substring(msg.indexOf(firstStr) + firstStr.length, msg.indexOf(secondStr))
        (Document.parse(msg.substring(msg.indexOf(thirdStr) + thirdStr.length, msg.length - 1))
          .append("client_ip", msg.substring(msg.indexOf(secondStr) + secondStr.length, msg.indexOf(thirdStr) - 2))
          .append("server_ts", TimeUtil.dateString2Time("yyyy-MM-dd HH:mm:ss", time, TimeUtil.SECOND).toInt)
          .append(JunhaiLog.host_name, hostName), true)
      } else {
        val json_str = msg.substring(msg.indexOf(thirdStr) + thirdStr.length, msg.length - 1)
        (Document.parse(json_str).append(JunhaiLog.host_name, hostName), true)
      }
    } catch {
      case _: Exception =>
        (new Document().append("msg", Str), false)
    }
  }

  //IP关联地域
  def addRegion(doc: Document, ip: IP): Unit = {
    try {
      if (doc.getString(JunhaiLog.client_ip) != "") {
        val clientIp = JunhaiLog.getString(doc, JunhaiLog.client_ip)
        val geo: util.HashMap[String, String] = IPAddress.getIPAddress(if (clientIp == "") doc.getString(JunhaiLog.create_ip) else clientIp, ip)
        for (key <- geo.keySet()) {
          var value: String = geo.get(key)
          if (value.contains("\"")) {
            value = value.replaceAll("\"", "")
          }
          doc.put(key, value)
        }
      }
    } catch {
      case _: Exception =>
        doc.put("province", "")
        doc.put("country", "")
        doc.put("city", "")
    }
  }


  def loginFormat(log: String, source: String,
                  bDataCleanConfig: Broadcast[Document],
                  ipClass: IP): (Document, Boolean) = {
    var result = true
    var jsonParse = true
    var flag = true
    var hostName = ""
    val document =
      source match {
        case "agentLoginSrc" =>
          val info: (Document, Int, String) = try {
            val doc = Document.parse(log)
            val msg = JunhaiLog.getString(doc, "message")
            hostName = JunhaiLog.getString(doc, "host")
            JunhaiLog.agentOldLog2Bson(msg)
          } catch {
            case _: Exception => {
              jsonParse = false
              (new Document().append(LOG, log), 0, error)
            }
          }

          val tmpDoc = info._1
          if (JunhaiLog.getString(tmpDoc, JunhaiLog.game_channel_id).equals("") && JunhaiLog.getInt(tmpDoc, JunhaiLog.game_id) >= 63) {
            flag = false
          }

          val userDoc = new Document()
            .append(JunhaiLog.user_id, JunhaiLog.getString(tmpDoc, JunhaiLog.user_id))
            .append(JunhaiLog.user_name, "").append(JunhaiLog.gender, "")
            .append(JunhaiLog.birth, "").append(JunhaiLog.age, "")
          val gameDoc = new Document()
            .append(JunhaiLog.game_id, JunhaiLog.getString(tmpDoc, JunhaiLog.game_id))
            .append(JunhaiLog.game_name, "").append(JunhaiLog.game_ver, "")
          val agentDoc = new Document()
            .append(JunhaiLog.channel_id, JunhaiLog.getString(tmpDoc, JunhaiLog.channel_id))
            .append(JunhaiLog.game_channel_id, JunhaiLog.getString(tmpDoc, JunhaiLog.game_channel_id))
            .append(JunhaiLog.access_token, tmpDoc.getOrDefault(JunhaiLog.access_token, ""))
          val deviceDoc = new Document().append(JunhaiLog.screen_height, "")
            .append(JunhaiLog.screen_width, "").append(JunhaiLog.device_id, "")
            .append(JunhaiLog.ios_idfa, "").append(JunhaiLog.android_imei, "")
            .append(JunhaiLog.android_adv_id, "").append(JunhaiLog.android_id, "")
            .append(JunhaiLog.device_name, "").append(JunhaiLog.os_ver, "")
            .append(JunhaiLog.sdk_ver, "").append(JunhaiLog.package_name, "")
            .append(JunhaiLog.os_type, "").append(JunhaiLog.net_type, "").append(JunhaiLog.user_agent, "")
          val document = new Document().append(JunhaiLog.user, userDoc)
            .append(JunhaiLog.game, gameDoc).append(JunhaiLog.agent, agentDoc)
            .append(JunhaiLog.device, deviceDoc)
            .append(JunhaiLog.event, "login").append(JunhaiLog.is_test, "regular")
            .append(JunhaiLog.data_ver, "1.0").append(JunhaiLog.client_time_zone, "")
            .append(JunhaiLog.server_time_zone, "+08:00").append(JunhaiLog.client_ts, 0)
            .append(JunhaiLog.server_ts, info._2).append(JunhaiLog.client_ip, info._3)
            .append(JunhaiLog.host_name, hostName)
          (document, true)
        case "dalanLoginSrc" =>
          (Document.parse(log), true)
        case _ =>
          getData(log, "DATA[")
      }

    val doc = document._1
    result = doc.get(JunhaiLog.event) != null &&
      JunhaiLog.getInt(doc, JunhaiLog.server_ts) != -1 &&
      doc.get(JunhaiLog.server_time_zone) != null &&
      doc.get(JunhaiLog.device) != null &&
      doc.get(JunhaiLog.game) != null &&
      doc.get(JunhaiLog.is_test) != null &&
      doc.get(JunhaiLog.client_ts) != null &&
      doc.get(JunhaiLog.client_time_zone) != null &&
      doc.get(JunhaiLog.data_ver) != null && jsonParse && document._2 && flag

    if (result) {
      val eventRule = bDataCleanConfig.value.get(CONFIG_HEAD).asInstanceOf[Document]
        .get(doc.get(JunhaiLog.event)).asInstanceOf[Document]
      if (eventRule != null) {
        val firstColumns = eventRule.keySet()
        if (firstColumns != null) {
          firstColumns.forEach(firstColumn => {
            result &&= doc.get(firstColumn) != null
            if (result) {
              val firstColumnRule = eventRule.get(firstColumn).asInstanceOf[Document]
              val secondColumns = firstColumnRule.keySet()
              secondColumns.forEach(secondColumn => {
                var value = doc.get(firstColumn).asInstanceOf[Document].get(secondColumn)
                result &&= value != null
                if (result) {
                  val cleanType = firstColumnRule.getString(secondColumn)
                  val tempResult = cleanType match {
                    case "0" =>
                      true
                    case "1" =>
                      value = JunhaiLog.getSecondColumnString(doc, firstColumn, secondColumn).asInstanceOf[AnyRef]
                      JunhaiLog.getSecondColumnString(doc, firstColumn, secondColumn) != ""
                    case "2" =>
                      value = JunhaiLog.getSecondColumnInteger(doc, firstColumn, secondColumn).asInstanceOf[AnyRef]
                      JunhaiLog.getSecondColumnInteger(doc, firstColumn, secondColumn) >= 0
                    case "3" =>
                      value = JunhaiLog.getSecondColumnDouble(doc, firstColumn, secondColumn).asInstanceOf[AnyRef]
                      JunhaiLog.getSecondColumnDouble(doc, firstColumn, secondColumn) > 0.0
                    case _ =>
                      value = JunhaiLog.getSecondColumnString(doc, firstColumn, secondColumn).asInstanceOf[AnyRef]
                      value.asInstanceOf[String].matches(cleanType)
                  }
                  result &&= tempResult
                }
              })
            }
          })
        }
      }
      addRegion(doc, ipClass) //添加ip地域
    }
    result = if (JunhaiLog.getString(doc, JunhaiLog.event) != JunhaiLog.eventLogin) false else result
    (doc, result)
  }

  def monitor(srcLog: String, redisSink: Broadcast[RedisSink], tag: String = "others") = {
    val dateTag = TimeUtil.time2DateString("MMdd", new Date().getTime, TimeUtil.MILLISECOND)
    val log = Document.parse(srcLog)
    val key = JunhaiLog.getString(log, "host") + "_" + JunhaiLog.getString(log, "source")
    val msg = JunhaiLog.getString(log, "message")
    val value = msg.getBytes("utf-8").length + 1
    if (tag.equals(ERROR)) {
      redisSink.value.hIncrBy("monitor_" + dateTag, key + "_error", 1)
    } else {
      redisSink.value.hIncrBy("monitor_" + dateTag, key + "_bytes", value)
      redisSink.value.hIncrBy("monitor_" + dateTag, key + "_nums", 1)
    }
  }

}
