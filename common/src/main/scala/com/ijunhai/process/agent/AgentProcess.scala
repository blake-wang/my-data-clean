package com.ijunhai.process.agent

import java.util
import java.util.Date

import com.ijunhai.common.HDFSUtil
import com.ijunhai.common.http.{HttpClientUtil, Result}
import com.ijunhai.common.logsystem.JunhaiLog
import org.apache.http.client.utils.HttpClientUtils
import org.bson.Document

object AgentProcess extends Serializable {


  /**
    * 君海自有渠道，走独代的数据接口获取
    *
    * @param hdfsCachePath
    * @return
    */
  def getIOS128(hdfsCachePath: String): Set[String] = {
    val game_channel_id = "game_channel_id"
    val fileName = "IOS128.json"
    val url = "http://bigdata.ijunhai.com/api/getIOS128"
    val content = "content"

    val contentStr = getContent(hdfsCachePath + fileName)
    val resultDoc = try {
      Document.parse(contentStr)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        Document.parse(getContent(url, hdfsCachePath + fileName))
      }
    }
    val referenceDocument = resultDoc.get(content).asInstanceOf[util.ArrayList[Document]]
    val set = referenceDocument.toArray.map(document => {
      val doc = document.asInstanceOf[Document]
      doc.getString(game_channel_id)
    }).toSet
    set
  }


  /**
    * 君海自有渠道，走独代的数据接口获取
    *
    * @param hdfsCachePath
    * @return
    */
  def getRate(hdfsCachePath: String): (Map[String, Double], Map[String, Double]) = {
    val from_currency = "from_currency"
    val to_currency = "to_currency"
    val exchange_rate = "exchange_rate"
    val fileName = "rate.json"
    val url = "http://bigdata.ijunhai.com/api/getRate"
    val content = "content"

    val contentStr = getContent(hdfsCachePath + fileName)

    val resultDoc = try {
      Document.parse(contentStr)
    } catch {
      case e: Exception => {
        e.printStackTrace()
        Document.parse(getContent(url, hdfsCachePath + fileName))
      }
    }
    val referenceDocument = resultDoc.get(content).asInstanceOf[util.ArrayList[Document]]
    val RMBmap = referenceDocument.toArray.map(document => {
      val doc = document.asInstanceOf[Document]
      (doc.getString(from_currency), doc.getString(to_currency), JunhaiLog.getDouble(doc, exchange_rate))
    }).filter(_._2 == "CNY").map(line => (line._1, line._3)).toMap //转成中文

    val USDmap = referenceDocument.toArray.map(document => {
      val doc = document.asInstanceOf[Document]
      (doc.getString(from_currency), doc.getString(to_currency), JunhaiLog.getDouble(doc, exchange_rate))
    }).filter(_._2 == "USD").map(line => (line._1, line._3)).toMap
    (RMBmap, USDmap)
  }

  /**
    * 通过game_id获取游戏公司id
    *
    * @param hdfsCachePath
    * @return
    */
  def getAgentGame(hdfsCachePath: String): Map[String, String] = {
    val game_id = "game_id"
    val company_id = "company_id"
    val fileName = "agent_game.json"
    val url = "http://game.data.ijunhai.com/Gamedata/api/getAgentGame"
    val content = "content"

    val contentStr = getContent(hdfsCachePath + fileName)

    val resultDoc = try {
      Document.parse(contentStr)
    } catch {
      //hdfs拿不到则去接口拿
      case e: Exception =>
        e.printStackTrace()
        Document.parse(getContent(url, hdfsCachePath + fileName))
    }
    val referenceDocument = resultDoc.get(content).asInstanceOf[util.ArrayList[Document]]
    val gcMap = referenceDocument.toArray.map(document => {
      val doc = document.asInstanceOf[Document]
      (doc.getString(game_id), doc.getString(company_id))
    }).toMap
    gcMap
  }


  /**
    * 君海自有渠道，走独代的数据接口获取
    */
  def getAgent2GameChannel(hdfsCachePath: String, service: String = ""): Map[String, (String, String, String)] = {
    val channel_id = "channel_id" //名字叫channel_id实际放的是sub_channel_id的值，所以要当做sub_channel_id来用
    val game_channel_id = "game_channel_id"
    val belong_game_id = "belong_game_id"
    val game_id = "game_id"
    val fileName = "agent_2_game_channel.json"
    val url = "http://bigdata.ijunhai.com/api/getBelongGameToGameChannelId"
    val content = "content"

    val contentStr = getContent(hdfsCachePath + fileName)

    val resultDoc = try {
      Document.parse(contentStr)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        Document.parse(getContent(url, hdfsCachePath + fileName))
    }
    val referenceDocument: util.ArrayList[Document] = resultDoc.get(content).asInstanceOf[util.ArrayList[Document]]

    val map = referenceDocument.toArray.map(document => {
      val doc = document.asInstanceOf[Document]
      if (service.equals("haiwai")) {
        if (doc.getString("sub_pf").equals("4") || doc.getString("sub_pf").equals("5")) {
          //海外的belong_game_id会对应多个值，要根据sub_pf确定唯一
          (doc.getString(belong_game_id), (doc.getString(game_channel_id), doc.getString(channel_id), doc.getString(game_id)))
        } else {
          ("", ("", "", ""))
        }
      } else {
        (doc.getString(belong_game_id), (doc.getString(game_channel_id), doc.getString(channel_id), doc.getString(game_id)))
      }
    }).toMap
    map
  }


  /**
    * 通过sub_channel_id拿到channel_id
    *
    * @param hdfsCachePath
    * @return
    */
  def getSubChannel(hdfsCachePath: String): Map[String, String] = {
    val sub_channel_id = "sub_channel_id"
    val channel_id = "channel_id"
    val fileName = "sub_channel.json"
    val url = "http://game.data.ijunhai.com/Gamedata/api/getAgentSubChannel"
    val content = "content"

    val contentStr = getContent(hdfsCachePath + fileName)
    val resultDoc =
      try {
        Document.parse(content)
      } catch {
        case e: Exception =>
          e.printStackTrace()
          Document.parse(getContent(url, hdfsCachePath + fileName))
      }
    val referenceDocument = resultDoc.get(content).asInstanceOf[util.ArrayList[Document]]
    val map = referenceDocument.toArray.map(document => {
      val doc = document.asInstanceOf[Document]
      (doc.getString(sub_channel_id), doc.getString(channel_id))
    }).toMap
    map
  }


  /**
    * sub_pf是操作系统类型，任何情况下都应该通过渠道ID关联sub_pf
    */
  val osTypeMap = Map("0" -> "android", "1" -> "ios")

  def getAgentChannel(hdfsCachePath: String): Map[String, String] = {
    val pf = "pf"
    val sub_pf = "pf"
    val channel_id = "channel_id"
    val fileName = "agent_channel.json"
    val url = "http://game_data.ijunhai.com/Gamedata/api/getAgentChannel"
    val content = "content"
    val contentStr = getContent(hdfsCachePath + fileName)
    val resultDoc =
      try {
        Document.parse(contentStr)
      } catch {
        case e: Exception =>
          e.printStackTrace()
          Document.parse(getContent(url, hdfsCachePath + fileName))
      }
    val referenceDocument = resultDoc.get(content).asInstanceOf[util.ArrayList[Document]]
    val map = referenceDocument.toArray.map(document => {
      val doc = document.asInstanceOf[Document]
      (doc.getString(channel_id), osTypeMap)
    })
    null
  }


  /**
    * 形成两个map结构,通过game_channel_id拿channel_id或者通过channel_id拿game_channel_id
    *
    * @param hdfsCachePath
    * @param tag
    * @return
    */
  def getAgentGameChannel(hdfsCachePath: String, tag: Int = 0): (Map[String, String], Map[String, String]) = {
    val game_id = "game_id"
    val channel_id = "channel_id"
    val game_channel_id = "game_channel_id"
    val fileName = "game_channel_id.json"
    val url = "http://game.data.ijunhai.com/Gamedata/api/getAgentGameChannel"
    val content = "content"

    val contentStr = getContent(hdfsCachePath + fileName)

    val resultDoc =
      try {
        Document.parse(contentStr)
      } catch {
        case e: Exception =>
          e.printStackTrace()
          println("get from url")
          Document.parse(getContent(url, hdfsCachePath + fileName))
      }
    val referenceDocument = resultDoc.get(content).asInstanceOf[util.ArrayList[Document]]
    val ggMap = referenceDocument.toArray.map(document => {
      val doc = document.asInstanceOf[Document]
      if (tag == 1) {
        (doc.getString(game_channel_id), doc.getString(channel_id) + "|" + doc.getString(game_id))
      } else {
        (doc.getString(game_id) + doc.getString(game_channel_id), doc.getString(channel_id))
      }
    }).toMap //game_id+game_channel_id 获得  channel_id
    val gcMap = referenceDocument.toArray.map(document => {
      val doc = document.asInstanceOf[Document]
      (doc.getString(game_id) + doc.getString(channel_id), doc.getString(game_channel_id))
    }).toMap
    (ggMap, gcMap)
  }


  /**
    * 直接从hdfs上读数据
    *
    * @param cacheFile hdfs缓存文件
    */
  def getContent(cacheFile: String) = {
    new String(HDFSUtil.readFromHdfs(cacheFile))
  }

  /**
    * 获取http上的请求默认连接超时时间为3000毫秒，数据传输时间为4000毫秒
    * 如果http请求超时或者错误，则去上次缓存的地方拿，目前存在hdfs上
    * 拿到的话就覆盖hdfs上的内容
    * 如果hdfs上也没有就报错
    *
    * @param url       请求的url连接
    * @param cacheFile hdfs缓存文件
    * @return
    */
  def getContent(url: String, cacheFile: String): String = {
    val hcu = new HttpClientUtil()
    val result = try {
      hcu.doGet(url)
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        System.err.println(new Date + " Error: get url=" + url + "fail")
        new Result()
      }
    }

    if (result.getStatusCode != 200) {
      System.out.println(new Date + " Error: the status code of url=" + url + " is " + result.getStatusCode)
      new String(HDFSUtil.readFromHdfs(cacheFile))
    } else {
      HDFSUtil.uploadToHdfs(cacheFile, result.getContent.getBytes())
      result.getContent
    }
  }

}
