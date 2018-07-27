package com.ijunhai.process.agent

import java.util
import java.util.{Date, Random}

import com.ijunhai.common.CleanConstants._
import com.ijunhai.common.ip.{IP, IPAddress}
import com.ijunhai.common.logsystem.JunhaiLog
import com.ijunhai.common.logsystem.JunhaiLog._
import com.ijunhai.common.{HDFSUtil, TimeUtil}
import com.ijunhai.storage.greenplum.GreenPlumSink
import com.ijunhai.storage.hbase.HbaseSink
import com.ijunhai.storage.kafka.KafkaSink
import com.ijunhai.storage.redis.RedisSink
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.bson.Document
import com.ijunhai.process.agent.ProcessImpl._
import com.ijunhai.process.saveFirstEvent

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
    val bGameChannel: Broadcast[Map[String, (String, String, String)]] = sparkContext.broadcast(AgentProcess.getAgent2GameChannel(hdfsCachePath, source))()
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
        val (finalLog, topicName) = source match { //获得最终的数据和发送的topic名
          case JunhaiLog.agentLoginSrc =>
            if(!reRun) monitor(log,redisSink) //重跑数据不监控
            booleanC = saveFirstEvent.saveAll(afterJoin,hBaseSink,redisSink,booleanB)

        }
      })

    })


  }

  //------------------------------->>>>>>>写到这里<<<<
  def detail2GP(ds: DStream[String], gpSink: Broadcast[GreenPlumSink]): Unit = {
    ds.foreachRDD(rdd => {
      val split = TimeUtil.time2DateString("yyyy-MM-dd", new Date().getTime, TimeUtil.MILLISECOND).split("-")
      val date = split(1) + split(2)
      val partitionNums: Int = rdd.getNumPartitions

      rdd.map(log => {
        var boolean = true
        var tableName = ""
        val doc = try {
          Document.parse(log)
        } catch {
          case _: Exception =>
            boolean = false
            new Document()
        }
        boolean = doc.getString(JunhaiLog.is_test) == "regular"
        val tmpDoc = new Document()
          .append(server_date_day, JunhaiLog.getString(doc, server_date_day))
          .append(server_date_hour, JunhaiLog.getInt(doc, server_date_hour))
          .append(reg_date, JunhaiLog.getString(doc, reg_date))
          .append(first_order_date, JunhaiLog.getString(doc, first_order_date))
          .append(reg_hour, JunhaiLog.getInt(doc, reg_hour))

        val finalDoc = JunhaiLog.getString(doc, JunhaiLog.event) match {
          case JunhaiLog.eventLogin =>
            tableName = "1"
            var osType = JunhaiLog.getString(doc, "device-os_type")
            osType = if (osType.trim == "") "android" else osType
            tmpDoc.append(JunhaiLog.game_id, JunhaiLog.getString(doc, "game-game_id"))
              .append(JunhaiLog.channel_id, JunhaiLog.getString(doc, "agent-channel_id"))
              .append(JunhaiLog.game_channel_id, JunhaiLog.getString(doc, "agent-game_channel_id"))
              .append(JunhaiLog.os_type, osType)
              .append(JunhaiLog.user_id, JunhaiLog.getString(doc, "user-user_id"))
              .append(JunhaiLog.company_id, JunhaiLog.getString(doc, "game-company_id"))

          case JunhaiLog.eventOrder =>
            tableName = "o"
            if (JunhaiLog.getInt(doc, JunhaiLog.pay_status) == 1 && JunhaiLog.getInt(doc, JunhaiLog.status) == 2) {
              var osType = JunhaiLog.getString(doc, JunhaiLog.os_type)
              osType = if (osType.trim == "") "android" else osType
              tmpDoc.append(JunhaiLog.game_id, JunhaiLog.getString(doc, JunhaiLog.game_id))
                .append(JunhaiLog.channel_id, JunhaiLog.getString(doc, JunhaiLog.channel_id))
                .append(JunhaiLog.game_channel_id, JunhaiLog.getString(doc, JunhaiLog.game_channel_id))
                .append(JunhaiLog.os_type, osType)
                .append(JunhaiLog.user_id, JunhaiLog.getInt(doc, JunhaiLog.user_id))
                .append(JunhaiLog.direct_pay, JunhaiLog.getInt(doc, JunhaiLog.direct_pay))
                .append(JunhaiLog.company_id, JunhaiLog.getString(doc, JunhaiLog.company_id))
                .append(JunhaiLog.money, JunhaiLog.getDouble(doc, if (log.contains(JunhaiLog.cny_amount)) JunhaiLog.cny_amount else JunhaiLog.money))
            } else {
              boolean = false
              new Document()
            }
          case _ =>
            boolean = false
            new Document()
        }

        val dataSp = JunhaiLog.getString(finalDoc, "server_date_day").split("-")
        boolean = if (dataSp.length >= 2) {
          val dateDate = dataSp(1) + dataSp(2)
          if (date != dateDate) false else true
        } else {
          false
        }
        (finalDoc, tableName, boolean)
      }).filter(_._3).map(log => {
        (new Random().nextInt(3) + log._2, log._1)
      }).groupByKey().foreach(log => {
        val tableName = (if (log._1.contains("o")) "order" else "login") + date
        if (!gpSink.value.validateTableExist(tableName)) {
          gpSink.value.createAdvDetailTable(tableName)
        }
        gpSink.value.insertBatch(log._2, tableName)
      })

    })
  }

}
