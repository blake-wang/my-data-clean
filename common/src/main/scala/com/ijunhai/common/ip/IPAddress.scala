package com.ijunhai.common.ip

import java.util

import com.ijunhai.common.HDFSUtil
import com.ijunhai.common.logsystem.JunhaiLog

object IPAddress extends Serializable {
  val IP_DAT_PATH = "ip.dat.path"

  def init(path: String): Array[Byte] = {
    HDFSUtil.readFromHdfs(path)
  }

  def getIPAddress(ip:String,ipClass:IP): java.util.HashMap[String,String] ={
    val map=new util.HashMap[String,String]()
    val result: Array[String] =ipClass.find(ip)
    map.put(JunhaiLog.country,result(0))
    map.put(JunhaiLog.province,result(1))
    map.put(JunhaiLog.city,result(2))
    map
  }
}
