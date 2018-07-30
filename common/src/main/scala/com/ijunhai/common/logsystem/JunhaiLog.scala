package com.ijunhai.common.logsystem

import com.ijunhai.common.TimeUtil
import com.ijunhai.common.logsystem.JunhaiLog.isNumber
import org.bson.Document

class JunhaiLog {
  //一级标签
  var user = ""
  var game = ""
  var agent = ""
  var device = ""
  var role = ""
  var mission = ""
  var copy = ""
  var trade = ""
  var order = ""
  var click = ""
  var custom = ""
  var channel_platform = ""
  var junhai_sdk = ""

  val reg_date = "reg_date"
  val reg_hour = "reg_hour"

  val first_order_date = "first_order_date"
  val server_date = "server_date"
  val host_name = "host_name"

  val server_date_day = "server_date_day"
  val server_date_hour = "server_date_hour"


  var sub_pf = ""
  var status = ""
  var server_first_date = ""
  var is_new_user = ""

  var event = ""
  var is_test = "regular"
  var data_ver = "1.0"
  var client_time_zone = "+08:00"
  var client_ts = 0
  var server_time_zone = "+08:00"
  var server_ts = 0
  var client_ip = ""
  var country = ""
  var province = ""
  var city = ""
  var area_code = ""
  var remark = ""

  //二级标签
  var user_id = ""
  var user_name = ""
  var gender = ""
  var birth = ""
  var age = ""
  var phone_num = ""
  var email_num = ""
  var login_type = ""
  var open_id = ""

  var game_id = ""
  var game_name = ""
  var game_ver = ""

  var channel_id = ""
  var game_channel_id = ""
  var access_token = ""

  var screen_height = ""
  var screen_width = ""
  var device_id = ""
  var ios_idfa = ""
  var android_imei = ""
  var android_adv_id = ""
  var android_id = ""
  var device_name = ""
  var os_ver = ""
  var sdk_ver = ""
  var package_name = ""
  var os_type = ""
  var net_type = ""
  var user_agent = ""
  var pay_status = ""
  var role_level = ""
  var role_name = ""
  var role_server = ""
  var role_id = ""
  var role_type = ""
  var role_gender = ""
  var association_id = ""
  var association_name = ""
  var association_rank = ""

  var mission_id = ""
  var mission_name = ""
  var mission_type = ""
  var mission_process = ""
  var mission_step = ""

  var copy_id = ""
  var copy_name = ""
  var copy_level = ""
  var copy_diffculty = ""
  var copy_process = ""

  var trade_type = ""
  var trade_amount = ""
  var remain_amount = ""
  var item_name = ""
  var item_amount = ""
  var trade_id = ""
  var trade_desc = ""
  var is_bind = ""

  var order_sn = ""
  var cp_trade_sn = ""
  var channel_trade_sn = ""
  var request_url = ""
  var http_code = ""
  var request_result = ""
  var order_step = ""
  var cny_amount = ""
  var currency_amount = 0.0
  var currency_type = ""
  var usd_amount = ""
  var order_status = ""
  var order_type = ""


  var event_tag = ""
  var click_time = 0

  var ad_id = ""

  var app_id = ""
  val imei = ""
}

object JunhaiLog {

  /**
    * 一层清洗
    * sql.Date  在Document中不能识别，所以需要清洗
    *
    * @param document
    * @return
    */
  def documentClean2Json(document: Document): Document = {
    val keys = document.keySet()
    for (key <- keys) {
      document.get(key) match {
        case value: java.sql.Date =>
          document.put(key, TimeUtil.time2DateString("yyyy-MM-dd HH:mm:ss", value.getTime, TimeUtil.MILLISECOND))
        case _ =>
      }
    }
    document
  }

  val agentLoginSrc = "agentLoginSrc"
  val agentOldLoginSrc = "agentOldLoginSrc"
  val agentDBSrc = "agentDBSrc"
  val dalanLoginSrc = "dalanLoginSrc"
  val dalanDBSrc = "dalanDBSrc"
  val haiwaiLoginSrc = "haiwaiLoginSrc"
  val haiwaiDBSrc = "haiwaiDBSrc"
  val COPY = "Copy"
  val TEST = "Test"
  val OTHER = "Other"
  val FILTER = "Filter"
  val USER = "User"

  val MONITOR_TOPIC = "monitor"
  val LOG = "log"
  val DATA = "data"
  val junhaiChannelsId = Set("199", "8084", "10033", "10081", "10082", "10085", "10086", "10087",
    "10088", "10089", "10099", "10100", "10107", "10133", "10150", "10151", "10157", "10162", "6001") //属于大蓝和海外渠道
  val haiwaiChannelId = Set("6001", "10081", "10082", "10085", "10086", "10087", "10088", "10089", "10099", "10100", "10107")
  val dalanChannelId = Set("199", "8084", "10033", "10133", "10150", "10151", "10157", "10162")
  val DALAN = "dalan"
  val SHENQI = "shenqi"
  val LIANYUN = "lianyun"
  val HAIWAI = "haiwai"
  val CHUMENG = "chumeng"
  val channelsWithOutDalan = Set(SHENQI, LIANYUN, HAIWAI, CHUMENG)
  val junhaiChannelsName = Set(DALAN, SHENQI, LIANYUN, HAIWAI, CHUMENG)
  val agentName = Set(DALAN, "agent")

  val APP_KEY = "app_key" //由于独代和自有渠道数据混合，导致game下的gameID字段不够用，添加该字段用来表示自有渠道的游戏ID
  //日志级别
  val ERROR = "ERROR"
  val WARM = "WARM"
  val INFO = "INFO"
  //event
  val eventLogin = "login"
  val eventOrder = "order"
  val eventActive = "active"
  val eventRegister = "register"
  val eventCrash = "crash"
  //service
  val serviceSDK = "sdk"
  val serviceAgent = "agent"
  val serviceChannel = "channel"
  val serviceDalan = "dalan"

  //一级标签
  val user = "user"
  val game = "game"
  val agent = "agent"
  val device = "device"
  val role = "role"
  val mission = "mission"
  val copy = "copy"
  val trade = "trade"
  val order = "order"
  val click = "click"
  val custom = "custom"
  val channel_platform = "channel_platform"
  val junhai_sdk = "junhai_sdk"

  val reg_date = "reg_date"
  val reg_hour = "reg_hour"

  val first_order_date = "first_order_date"
  val server_date = "server_date"
  val host_name = "host_name"

  val server_date_day = "server_date_day"
  val server_date_hour = "server_date_hour"


  val status = "status"
  val event = "event"
  val is_test = "is_test"
  val data_ver = "data_ver"
  val client_time_zone = "client_time_zone"
  val client_ts = "client_ts"
  val server_time_zone = "server_time_zone"
  val server_ts = "server_ts"
  val client_ip = "client_ip"
  val register_time = "register_time"

  val create_ip = "create_ip"
  val direct_pay = "direct_pay"

  val country = "country"
  val province = "province"
  val city = "city"
  val area_code = "area_code"
  val remark = "remark"
  //二级标签
  val user_id = "user_id"
  val user_name = "user_name"
  val gender = "gender"
  val birth = "birth"
  val age = "age"
  val phone_num = "phone_num"
  val email_num = "email_num"
  val login_type = "login_type"
  val open_id = "open_id"

  val pay_status = "pay_status"
  val update_time = "update_time"
  val create_time = "create_time"


  val game_id = "game_id"
  val game_name = "game_name"
  val game_ver = "game_ver"
  val company_id = "company_id"
  val channel_id = "channel_id"
  val game_channel_id = "game_channel_id"
  val access_token = "access_token"
  val screen_height = "screen_height"
  val screen_width = "screen_width"
  val device_id = "device_id"
  val ios_idfa = "ios_idfa"
  val android_imei = "android_imei"
  val android_adv_id = "android_adv_id"
  val android_id = "android_id"
  val device_name = "device_name"
  val os_ver = "os_ver"
  val sdk_ver = "sdk_ver"
  val package_name = "package_name"
  val os_type = "os_type"
  val net_type = "net_type"
  val user_agent = "user_agent"
  val role_level = "role_level"
  val role_name = "role_name"
  val role_server = "role_server"
  val role_id = "role_id"
  val role_type = "role_type"
  val role_gender = "role_gender"
  val association_id = "association_id"
  val association_name = "association_name"
  val association_rank = "association_rank"
  val mission_id = "mission_id"
  val mission_name = "mission_name"
  val mission_type = "mission_type"
  val mission_process = "mission_process"
  val mission_step = "mission_step"
  val copy_id = "copy_id"
  val copy_name = "copy_name"
  val copy_level = "copy_level"
  val copy_diffculty = "copy_diffculty"
  val copy_process = "copy_process"
  val trade_type = "trade_type"
  val trade_amount = "trade_amount"
  val remain_amount = "remain_amount"
  val item_name = "item_name"
  val item_amount = "item_amount"
  val trade_id = "trade_id"
  val trade_desc = "trade_desc"
  val is_bind = "is_bind"
  val order_sn = "order_sn"
  val cp_trade_sn = "cp_trade_sn"
  val channel_trade_sn = "channel_trade_sn"
  val request_url = "request_url"
  val http_code = "http_code"
  val request_result = "request_result"
  val order_step = "order_step"
  val cny_amount = "cny_amount"
  val currency_amount = "currency_amount"
  val currency_type = "currency_type"
  val usd_amount = "usd_amount"
  val order_status = "order_status"
  val order_type = "order_type"
  val event_tag = "event_tag"
  val click_time = "click_time"
  val ad_id = "ad_id"
  val app_id = "app_id"
  val event_type = "event_type"
  val event_order = "event_order"
  val eventAf = "install"
  val imei = "imei"
  val money = "money"

  def getString(document: Document, key: String): String = {
    try {
      document.get(key) match {
        case value: Integer =>
          value + ""
        case value: String =>
          value
        case value: java.lang.Long =>
          value + ""
        case value: java.lang.Double =>
          String.valueOf(value)
        case _ =>
          ""
      }
    } catch {
      case e: Exception => {
        ""
      }
    }
  }

  def getDouble(document: Document, key: String): Double = {
    document.get(key) match {
      case value: Integer =>
        value.toDouble
      case value: String =>
        value.toDouble
      case value: java.lang.Long =>
        value.toDouble
      case value: java.lang.Double =>
        value
      case _ =>
        0.0
    }
  }

  def getIntString(document: Document, key: String): String = {
    document.get(key) match {
      case value: Integer =>
        value + ""
      case value: String =>
        value
      case value: java.lang.Long =>
        value + ""
      case value: java.lang.Double =>
        val str = String.valueOf(value)
        str.substring(0, str.indexOf("."))
      case _ =>
        //        println(s"Can not recognize the key=$key value=${document.get(key)} ")
        "0"
    }
  }

  def getInt(document: Document, key: String): Int = {
    try {
      document.get(key) match {
        case value: Integer =>
          value
        case value: String =>
          value.toInt
        case value: java.lang.Long =>
          value.toInt
        case value: java.lang.Double =>
          value.toInt
        case _ =>
          //        println(s"Can not recognize the key=$key value=${document.get(key)} ")
          -1
      }
    } catch {
      case _: Exception =>
        -1
    }
  }

  def getLongString(document: Document, key: String): String = {
    document.get(key) match {
      case value: Integer =>
        value + ""
      case value: String =>
        value
      case value: java.lang.Long =>
        value + ""
      case value: java.lang.Double =>
        val str = String.valueOf(value)
        str.substring(0, str.indexOf("."))
      case _ =>
        //        println(s"Can not recognize the key=$key value=${document.get(key)} ")
        "0"
    }
  }

  def getLong(document: Document, key: String): Long = {
    document.get(key) match {
      case value: Integer =>
        value.toLong
      case value: String =>
        value.toLong
      case value: java.lang.Long =>
        value.toLong
      case value: java.lang.Double =>
        value.toLong
      case _ =>
        //        println(s"Can not recognize the key=$key value=${document.get(key)} ")
        0
    }
  }

  def getTimestamp(document: Document, key: String, formatString: String = "yyyy-MM-dd HH:mm:ss"): Int = {
    document.get(key) match {
      case value: Integer =>
        value
      case value: java.lang.Long =>
        if (value > Integer.MAX_VALUE) {
          (value / 1000).toInt
        } else {
          value.toInt
        }
      case value: String =>
        if (isNumber(value)) {
          if (value.length == 10)
            value.toInt
          else
            (value.toLong / 1000).toInt
        } else {
          TimeUtil.dateString2Time(formatString, value, TimeUtil.SECOND).toInt
        }
      case _ =>
        //        println(s"Can not recognize the key=$key value=${document.get(key)} ")
        0
    }
  }

  def getDocument(document: Document, key: String): Document = {
    document.get(key) match {
      case value: Document =>
        value
      case null =>
        new Document()
      case _ =>
        new Document().append(key, document.get(key))
    }
  }

  def getSecondColumnString(document: Document, first_key: String, second_key: String): String = {
    getString(getDocument(document, first_key), second_key)
  }

  def getSecondColumnDouble(document: Document, first_key: String, second_key: String): Double = {
    getDouble(getDocument(document, first_key), second_key)
  }

  def getSecondColumnInteger(document: Document, first_key: String, second_key: String): Int = {
    getInt(getDocument(document, first_key), second_key)
  }

  def getSecondColumnLong(document: Document, first_key: String, second_key: String): Long = {
    getLong(getDocument(document, first_key), second_key)
  }

  def isNumber(string: String): Boolean = {
    var flag = true
    for (i <- string.indices) {
      if (string.charAt(i) < '0' || string.charAt(i) > '9') {
        flag = false
      }
    }
    flag
  }

  def agentOldLog2Bson(log: String): (Document, Int, String) = {

    //NORMAL: [2018-01-08 12:11:04] CIP[119.54.199.236] CONTENT[{"user_id":"1000920627","channel_id":"10169","game_id":"136","channel_name":"h5agent","game_channel_id":""}]

    val firstStr = "NORMAL: ["
    val secondStr = "] CIP["
    val thirdStr = "CONTENT["

    val json_str = log.substring(log.indexOf(thirdStr) + thirdStr.length, log.length - 1)
    val time = log.substring(log.indexOf(firstStr) + firstStr.length, log.indexOf(secondStr))
    val ip = log.substring(log.indexOf(secondStr) + secondStr.length, log.indexOf(thirdStr) - 2)
    val tmpDoc = Document.parse(json_str)
    (tmpDoc, TimeUtil.dateString2Time("yyyy-MM-dd HH:mm:ss", time, TimeUtil.SECOND).toInt, ip)
  }


  def log2bson(log: String): (Document, Boolean) = { //包括AF日志 AD_AF_LOG[
    val startStr = "DATA["
    if (log.contains(startStr)) {
      val json_str = log.substring(log.indexOf(startStr) + startStr.length, log.length - 1)
      (Document.parse(json_str), true)
    } else {
      (new Document().append("AD_AF_LOG", log), false)
    }
  }

  def BsonParser(log: String, thirdStr: String): (Document, Int, String) = {
    val firstStr = "NORMAL: ["
    val secondStr = "] CIP["
    // NORMAL: [2018-03-02 19:12:30] CIP[182.84.195.55] ACTIVE_LOG
    // [
    //  {
    //   "extra_data":
    //     {"screen_size":"960|540","device_id":"00000000-3be3-e5e3-dfc7-96034d1a4a02","device_name":"RwoZI nPuU617",
    //     "imsi":"865166027042699","imei":"865166027042699","system_version":"5.1.1","sdk_version":"2.1",
    //     "package_name":"com.junhai.shj2.dalan","application_name":"山海经2","application_version":181},
    //   "app_data":
    //     {"agent_game_id":"146","game_channel_id":"103278","action_type":"start","time":"1519989148"}
    //  }
    // ]

    val activeLog = log.substring(log.indexOf(thirdStr) + thirdStr.length, log.length - 1)
    val time = log.substring(log.indexOf(firstStr) + firstStr.length, log.indexOf(secondStr))
    val ip = log.substring(log.indexOf(secondStr) + secondStr.length, log.indexOf(thirdStr) - 2)
    (Document.parse(activeLog), TimeUtil.dateString2Time("yyyy-MM-dd HH:mm:ss", time, TimeUtil.SECOND).toInt, ip)
  }


}





















