package com.ijunhai.storage.greenplum

import java.sql.{Connection, PreparedStatement, ResultSet, SQLException, Timestamp}

import com.ijunhai.common.TimeUtil
import com.ijunhai.common.logsystem.JunhaiLog
import org.bson.Document
import org.postgresql.util.PSQLException

class GreenPlumSink(getConn: () => Connection) extends Serializable {
  def insertBatch(doc: Iterable[Document], table: String): Unit = {
    var ps: PreparedStatement = null
    doc.foreach(f = document => {
      if (ps == null) {
        var sqlKey = ""
        var values = ""
        val keys = document.keySet()
        for (key <- keys) {
          sqlKey += key + ","
          values += "?,"
        }
        //去掉最后一个逗号
        sqlKey = sqlKey.substring(0, sqlKey.length - 1)
        values = values.substring(0, values.length - 1)
        val sql = s"insert into $table ($sqlKey) values($values)"
        ps = connection.prepareStatement(sql)
      }
      var index = 1
      for (key <- document.keySet()) {
        document.get(key) match {
          case value: Integer =>
            ps.setInt(index, value)
          case value: java.lang.Long =>
            ps.setLong(index, value)
          case value: String =>
            ps.setString(index, value)
          case value: java.lang.Double =>
            ps.setDouble(index, value)
          case value: java.sql.Date =>
            ps.setTimestamp(index, new Timestamp(value.getTime))
          case _ =>
            ps.setObject(index, document.get(key))
        }
        index += 1
      }
      ps.addBatch()
    })
    try {
      ps.executeBatch()
    } catch {
      case e: PSQLException =>
        println(e.printStackTrace())
    }
  }

  lazy val connection = getConn()

  def insert(document: Document, table: String): Document = {
    var sqlKey = ""
    var values = ""
    val keys = document.keySet()
    for (key <- keys) {
      sqlKey += key + ","
      document.get(key) match {
        case value: java.sql.Date =>
          values += "\'" + TimeUtil.time2DateString("yyyy-MM-dd HH:mm:ss", value.getTime, TimeUtil.MILLISECOND) + "\',"
        case _ =>
          values += "?,"
      }
    }

    sqlKey = sqlKey.substring(0, sqlKey.length - 1)
    values = values.substring(0, values.length - 1)
    val sql = s"insert into $table ($sqlKey) values($values)"
    val pstmt = connection.prepareStatement(sql)
    var index = 1
    for (key <- keys) {
      document.get(key) match {
        case value: Integer =>
          pstmt.setInt(index, value)
        case value: java.lang.Long =>
          pstmt.setLong(index, value)
        case value: String =>
          pstmt.setString(index, value)
        case value: java.lang.Double =>
          pstmt.setDouble(index, value)
        case value: java.sql.Date =>
          index -= 1
        case _ =>
          pstmt.setObject(index, document.get(key))
      }
      index += 1
    }
    try {
      pstmt.executeUpdate()
    } catch {
      case e: Exception =>
        document.append("PSQLException", e.printStackTrace())
    }
    JunhaiLog.documentClean2Json(document)
  }

  def select(sql: String): ResultSet = {
    val pstmt = connection.prepareStatement(sql)
    pstmt.executeQuery()
  }

  def update(sql: String): Int = {
    val pstmt = connection.prepareStatement(sql)
    pstmt.executeUpdate()
  }

  def validateTableExist(table: String): Boolean = {
    var flag = true
    try {
      val meta = connection.getMetaData
      val type_ : scala.Array[String] = scala.Array("TABLE")
      val rs: ResultSet = meta.getTables(null, null, table, type_)
      flag = rs.next()
    } catch {
      case e: SQLException =>
        e.printStackTrace()
    }
    flag
  }

  def createAdvDetailTable(tableName: String): Unit = {
    val stmt = connection.createStatement()
    val tag = if (tableName.contains("order")) {
      "money decimal(20,2) DEFAULT '0.00', " +
        "direct_pay smallint NOT NULL DEFAULT '0' , "
    } else {
      ""
    }
    val sql =
      "CREATE TABLE " + tableName +
        " (user_id varchar(100) DEFAULT '', " +
        tag +
        "channel_id varchar(50) DEFAULT '', " +
        "game_id varchar(50) DEFAULT '', " +
        "game_channel_id varchar(50) DEFAULT '', " +
        "os_type varchar(50) DEFAULT '', " +
        "company_id varchar(50) DEFAULT '', " +
        "first_order_date varchar(50) DEFAULT '', " +
        "server_date_day varchar(50) DEFAULT '', " +
        "server_date_hour smallint NOT NULL DEFAULT '0' , " +
        "reg_date varchar(50) DEFAULT '', " +
        "reg_hour smallint NOT NULL DEFAULT '0' , " +
        "ext1 varchar(255) DEFAULT '' " +
        ")DISTRIBUTED BY (user_id); "
    stmt.executeUpdate(sql)
  }

  def select(document: Document, table: String): Int = {
    var num = 0
    //    val package_name = JunhaiLog.getString()
    1
  }


}

object GreenPlumSink {
  def apply(databases: String): GreenPlumSink = {
    val f = () => {
      val conn = GreenPlumManager.getConnect(databases)
      sys.addShutdownHook {
        conn.close()
      }
      conn
    }
    new GreenPlumSink(f)
  }
}