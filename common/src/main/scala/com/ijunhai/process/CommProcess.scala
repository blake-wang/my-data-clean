package com.ijunhai.process

import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Date, Random}

import com.ijunhai.common.CleanConstants._
import com.ijunhai.common.ip.{IP, IPAddress}
import com.ijunhai.common.logsystem.JunhaiLog
import com.ijunhai.common.logsystem.JunhaiLog._
import com.ijunhai.common.{HDFSUtil, TimeUtil}
import com.ijunhai.process.agent.ProcessImpl._
import com.ijunhai.process.agent.{AgentProcess, ProcessImpl}
import com.ijunhai.storage.greenplum.GreenPlumSink
import com.ijunhai.storage.hbase.HbaseSink
import com.ijunhai.storage.kafka.KafkaSink
import com.ijunhai.storage.redis.RedisSink
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.bson.Document

object CommProcess extends Serializable {
  val DALAN_CHANNEL = util.Arrays.asList("10033", "10133") //大蓝ios正版 走独代和走大蓝 的渠道id


  def agentLogin(rdd: RDD[String], source: String,
                 redisSink: Broadcast[RedisSink],
                 kafkaSink: Broadcast[KafkaSink],
                 gpSink: Broadcast[GreenPlumSink],
                 hBaseSink: Broadcast[HbaseSink], reRun: Boolean) = {
    val topicTag = if (reRun) "Re" else ""
    val sparkContext = rdd.sparkContext
    val cleanConf: Broadcast[Document] = sparkContext.broadcast(HDFSUtil.readConfigFromHdfs(configPath))
    val bBytes = sparkContext.broadcast(IPAddress.init(ipDatabasePath))
    val agcTuple = AgentProcess.getAgentGameChannel(hdfsCachePath)
    val bAcMap: Broadcast[Map[String, String]] = sparkContext.broadcast(AgentProcess.getAgentChannel(hdfsCachePath))
    val bGgMap: Broadcast[Map[String, String]] = sparkContext.broadcast(agcTuple._1)
    val bGcMap: Broadcast[Map[String, String]] = sparkContext.broadcast(agcTuple._2)
    val bScMap: Broadcast[Map[String, String]] = sparkContext.broadcast(AgentProcess.getSubChannel(hdfsCachePath))
    val bBscMap: Broadcast[Map[String, (String, String, String)]] = sparkContext.broadcast(AgentProcess.getAgent2GameChannel(hdfsCachePath))
    val bGcmMap: Broadcast[Map[String, String]] = sparkContext.broadcast(AgentProcess.getAgentGame(hdfsCachePath))
    val bGameChannel: Broadcast[Map[String, (String, String, String)]] = sparkContext.broadcast(AgentProcess.getAgent2GameChannel(hdfsCachePath, source))
    val (rmb, usd) = AgentProcess.getRate(hdfsCachePath)
    val bRmb: Broadcast[Map[String, Double]] = sparkContext.broadcast(rmb)
    val bUsd: Broadcast[Map[String, Double]] = sparkContext.broadcast(usd)
    val bIOS128: Broadcast[Set[String]] = sparkContext.broadcast(AgentProcess.getIOS128(hdfsCachePath))

    val dest = source.substring(0, source.length - 3)

    rdd.foreachPartition(p => {
      val ipClass = new IP
      ipClass.load(bBytes.value)
      p.foreach(log => {
        val (afterFormat, booleanA) = ProcessImpl.loginFormat(log, source, cleanConf, ipClass)
        val (afterJoin, booleanB) = ProcessImpl.loginJoin(afterFormat, source, bAcMap, bGgMap, bGcMap, bScMap, bBscMap, bGcmMap, bGameChannel, bRmb, bUsd, bIOS128, booleanA)
        var booleanC = true;
        //        val (finalLog, topicName) = source match { //获得最终的数据和发送的topic名
        //          case JunhaiLog.agentLoginSrc =>
        //            if (!reRun) monitor(log, redisSink) //重跑数据不监控
        //            booleanC = saveFirstEvent.saveAll(afterJoin, hBaseSink, redisSink, booleanB)
        //
        //        }
      })

    })


  }

  //------------------------------->>>>>>>写到这里<<<<
  def detail2GP(ds: DStream[String], kafkaSink: Broadcast[KafkaSink], monitorGPSink: Broadcast[GreenPlumSink], gpSink: Broadcast[GreenPlumSink], redisSink: Broadcast[RedisSink]): Unit = {
    ds.foreachRDD(rdd => {
      val todaySplit = TimeUtil.time2DateString("yyyy-MM-dd", new Date().getTime, TimeUtil.MILLISECOND).split("-")
      val today = todaySplit(1) + todaySplit(2)
      var errorMsg = ""
      val orderSnList = new util.ArrayList[String]()
      if (gpSink.value.validateTableExist("order")) {
        //解决测试订单数据重复的问题
        val resultSet = gpSink.value.select("select distinct order_sn from order" + today + " where channel_id='10081';")
        while (resultSet.next()) {
          orderSnList.add(resultSet.getString(1))
        }
      }

      rdd.map(f = log => {
        var boolean = true
        var tableTag = ""
        val finalDoc = try {
          val doc: Document = Document.parse(log)
          //采集端的监控数据存redis,key为：monitor_0101
          if (log.contains("monitorSrc")) {
            val host = JunhaiLog.getString(doc, "host")
            val list = doc.getString("message").replaceAll("\\},\\{", "\\}||\\{").replaceAll("\\[", "").replaceAll("\\]", "").split("\\|\\|")
            for (str <- list) {
              val doc = Document.parse(str);
              val source = JunhaiLog.getString(doc, "source")
              val offset = JunhaiLog.getString(doc, "offset")
              val todayTag = todaySplit(1) + todaySplit(2)
              val todayRegex = ".*" + todaySplit(0).substring(2, 4) + "." + todaySplit(1) + "." + todaySplit(2) + ".*"
              val cal = Calendar.getInstance
              cal.add(Calendar.DATE, -1)
              val yesterdaySplit = new SimpleDateFormat("yyyy-MM-dd").format(cal.getTime).split("-")
              val yesterdayTag = yesterdaySplit(1) + yesterdaySplit(2)
              val yesterdayRegex = ".*" + yesterdaySplit(0).substring(2, 4) + "." + yesterdaySplit(1) + "." + yesterdaySplit(2) + ".*"
              if (source.matches(todayRegex) || source.contains(yesterdayRegex)) {
                val monitorDate = if (source.contains(todayTag)) todayTag else yesterdayTag
                val map = new util.HashMap[String, String]
                map.put(host + "|" + source + "|" + "bytesSrc", source)
                redisSink.value.hmset("monitor_" + monitorDate, map)
              }
            }
            boolean = false
            new Document()
          } else if (JunhaiLog.getString(doc, "system") != "") {
            boolean = false
            monitorGPSink.value.insert(doc, "monitor_error")
          } else if (!JunhaiLog.getDocument(doc, "user").isEmpty) {
            //agent_login_new  新独代登录
            tableTag = "n"
            new Document()
              .append("user_id", JunhaiLog.getSecondColumnString(doc, "user", "user_id"))
              .append("user_name", JunhaiLog.getSecondColumnString(doc, "user", "user_name"))
              .append("gender", JunhaiLog.getSecondColumnString(doc, "user", "gender"))
              .append("birth", JunhaiLog.getSecondColumnString(doc, "user", "birth"))
              .append("age", JunhaiLog.getSecondColumnString(doc, "user", "age"))
              .append("game_id", JunhaiLog.getSecondColumnString(doc, "game", "game_id"))
              .append("game_name", JunhaiLog.getSecondColumnString(doc, "game", "game_name"))
              .append("game_ver", JunhaiLog.getSecondColumnString(doc, "game", "game_ver"))
              .append("channel_id", JunhaiLog.getSecondColumnString(doc, "agent", "channel_id"))
              .append("game_channel_id", JunhaiLog.getSecondColumnString(doc, "agent", "game_channel_id"))
              .append("access_token", JunhaiLog.getSecondColumnString(doc, "agent", "access_token"))
              .append("screen_height", JunhaiLog.getSecondColumnString(doc, "device", "screen_height"))
              .append("screen_width", JunhaiLog.getSecondColumnString(doc, "device", "screen_width"))
              .append("device_id", JunhaiLog.getSecondColumnString(doc, "device", "device_id"))
              .append("ios_idfa", JunhaiLog.getSecondColumnString(doc, "device", "ios_idfa"))
              .append("android_imei", JunhaiLog.getSecondColumnString(doc, "device", "android_imei"))
              .append("android_adv_id", JunhaiLog.getSecondColumnString(doc, "device", "android_adv_id"))
              .append("android_id", JunhaiLog.getSecondColumnString(doc, "device", "android_id"))
              .append("device_name", JunhaiLog.getSecondColumnString(doc, "device", "device_name"))
              .append("os_ver", JunhaiLog.getSecondColumnString(doc, "device", "os_ver"))
              .append("sdk_ver", JunhaiLog.getSecondColumnString(doc, "device", "sdk_ver"))
              .append("package_name", JunhaiLog.getSecondColumnString(doc, "device", "package_name"))
              .append("os_type", JunhaiLog.getSecondColumnString(doc, "device", "os_type"))
              .append("net_type", JunhaiLog.getSecondColumnString(doc, "device", "net_type"))
              .append("user_agent", JunhaiLog.getSecondColumnString(doc, "device", "user_agent"))
              .append("event", JunhaiLog.getString(doc, "event"))
              .append("is_test", JunhaiLog.getString(doc, "is_test"))
              .append("data_ver", JunhaiLog.getString(doc, "data_ver"))
              .append("client_time_zone", JunhaiLog.getString(doc, "client_time_zone"))
              .append("server_time_zone", JunhaiLog.getString(doc, "server_time_zone"))
              .append("client_ts", JunhaiLog.getString(doc, "client_ts"))
              .append("server_ts", JunhaiLog.getString(doc, "server_ts"))
              .append("client_ip", JunhaiLog.getString(doc, "client_ip"))
              .append("custom", JunhaiLog.getDocument(doc, "custom").toJson())
              .append("ad_id", JunhaiLog.getSecondColumnString(doc, "custom", "ad_id"))
              .append("login_time", TimeUtil.time2SqlDate(JunhaiLog.getTimestamp(doc, server_ts), TimeUtil.SECOND))
          } else {
            boolean = doc.getString(JunhaiLog.is_test) == "regular"
            val tmpDoc = new Document()
              .append(server_date_day, JunhaiLog.getString(doc, server_date_day))
              .append(server_date_hour, JunhaiLog.getString(doc, server_date_hour))
              .append(reg_date, JunhaiLog.getString(doc, reg_date))
              .append(first_order_date, JunhaiLog.getString(doc, first_order_date))
              .append(reg_hour, JunhaiLog.getInt(doc, reg_hour))

            JunhaiLog.getString(doc, JunhaiLog.event) match {
              case JunhaiLog.eventLogin => {
                tableTag = "l"
                var osType = JunhaiLog.getString(doc, "device-os_type")
                osType = if (osType.trim == "") "android" else osType
                tmpDoc.append(JunhaiLog.server_ts, TimeUtil.time2SqlDate(JunhaiLog.getTimestamp(doc, JunhaiLog.server_ts), TimeUtil.SECOND))
                  .append(JunhaiLog.game_id, JunhaiLog.getString(doc, "game-game_id"))
                  .append(JunhaiLog.channel_id, JunhaiLog.getString(doc, "agent-channel_id"))
                  .append(JunhaiLog.game_channel_id, JunhaiLog.getString(doc, "agent-game_channel_id"))
                  .append(JunhaiLog.os_type, osType)
                  .append(JunhaiLog.user_id, JunhaiLog.getString(doc, "user-user_id"))
                  .append(JunhaiLog.company_id, JunhaiLog.getString(doc, "game-company_id"))

                if (JunhaiLog.getString(doc, "host_name").toLowerCase().contains("hw")) {
                  tmpDoc.append("source", "haiwai")
                } else {
                  tmpDoc.append("source", "agent")
                }
              }
              case JunhaiLog.eventOrder => {
                tableTag = "o"
                if (JunhaiLog.getInt(doc, JunhaiLog.pay_status) == 1 && JunhaiLog.getInt(doc, JunhaiLog.status) == 2) {
                  var osType = JunhaiLog.getString(doc, JunhaiLog.os_type)
                  osType = if (osType.trim == "") "android" else osType
                  tmpDoc.append(JunhaiLog.device_id, JunhaiLog.getString(doc, JunhaiLog.device_id))
                    .append(JunhaiLog.order_sn, JunhaiLog.getString(doc, JunhaiLog.order_sn))
                    .append(JunhaiLog.game_id, JunhaiLog.getString(doc, JunhaiLog.game_id))
                    .append(JunhaiLog.channel_id, JunhaiLog.getString(doc, JunhaiLog.channel_id))
                    .append(JunhaiLog.game_channel_id, JunhaiLog.getString(doc, JunhaiLog.game_channel_id))
                    .append(JunhaiLog.os_type, osType)
                    .append(JunhaiLog.user_id, JunhaiLog.getString(doc, JunhaiLog.user_id))
                    .append(JunhaiLog.direct_pay, JunhaiLog.getInt(doc, JunhaiLog.direct_pay))
                    .append(JunhaiLog.company_id, JunhaiLog.getString(doc, JunhaiLog.company_id))

                  if (JunhaiLog.getString(tmpDoc, JunhaiLog.channel_id).equals("10081") && orderSnList.contains(JunhaiLog.getString(tmpDoc, JunhaiLog.order_sn))) {
                    boolean = false
                  }
                  if (log.contains(JunhaiLog.cny_amount)) {
                    tmpDoc.append("source", "haiwai").append(JunhaiLog.money, JunhaiLog.getDouble(doc, JunhaiLog.cny_amount))
                      .append(JunhaiLog.update_time, TimeUtil.time2SqlDate(JunhaiLog.getTimestamp(doc, server_ts), TimeUtil.SECOND))
                  } else {
                    tmpDoc.append("source", "agent").append(JunhaiLog.money, JunhaiLog.getDouble(doc, JunhaiLog.money))
                      .append(JunhaiLog.update_time, TimeUtil.time2SqlDate(JunhaiLog.getTimestamp(doc, update_time), TimeUtil.SECOND))
                  }
                } else {
                  boolean = false
                  new Document()
                }
              }
              //active0101  独代激活
              case JunhaiLog.eventActive => {
                tableTag = "a"
                val finalDoc = new Document()
                for (k <- doc.keySet()) {
                  //                  val split: Array[String] = k.split("-")
                  val split: Array[String] = null
                  val key: String = if (split.length == 2) split(1) else split(0)
                  finalDoc.append(key, JunhaiLog.getString(doc, k))
                }
                finalDoc
              }
              case _ =>
                errorMsg = "event error"
                doc
            }
          }
        } catch {
          case _: Exception => {
            boolean = false
            kafkaSink.value.send("detailError", errorMsg + "   " + log)
            new Document()
          }
        }

        val dateTime = JunhaiLog.getString(finalDoc, "server_date_day")
        if (!dateTime.equals("")) {
          val dateSp = dateTime.split("-")
          boolean = if (dateSp.length >= 2) {
            val dateDate = dateSp(1) + dateSp(2)
            if (today != dateDate) false else true
          } else {
            false
          }
        }
        (finalDoc, tableTag, boolean)
      }).filter(_._3).distinct().map(log => {
        (new Random().nextInt(3) + log._2, log._1) //3个随机数+tableTag(a,o,l,n) ,通过groupByKey将数据分成12份，分12批插入
      }).groupByKey().foreach((log: (String, Iterable[Document])) => {
        val tableName =
          if (log._1.contains("n")) {
            "agent_login_new"
          } else {
            (if (log._1.contains("o")) {
              "order"
            } else if (log._1.contains("l")) {
              "login"
            } else {
              "active"
            }) + "today"
          }
        if (!gpSink.value.validateTableExist(tableName)) {
          gpSink.value.createAdvDetailTable(tableName)
        }
        gpSink.value.insertBatch(log._2, tableName)
      })
    })
  }

}
