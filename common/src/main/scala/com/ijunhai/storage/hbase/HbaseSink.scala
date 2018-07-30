package com.ijunhai.storage.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}


class HbaseSink(getConn: () => Connection) extends Serializable {
  def get(AGENTTABLENAME: String, key: String, COLUMNFAMILY: String, LOGINTS: String, ORDERTS: String) = {

  }


}

object HbaseSink {
  def apply(): HbaseSink = {
    val f = () => {

      val conf = HBaseConfiguration.create()
      conf.set("hbase.rootdir", "hdfs://Ucluster/hbase")
      conf.set("hbase.zookeeper.quorum", "uhadoop-1neej2-master1:2181,uhadoop-1neej2-master2:2181,uhadoop-1neej2-core1:2181")

      val conn: Connection = ConnectionFactory.createConnection(conf)
      sys.addShutdownHook {
        conn.close()
      }
      conn
    }
    new HbaseSink(f)
  }

  def apply(conf: Configuration): HbaseSink = {
    val f = () => {
      val conn: Connection = ConnectionFactory.createConnection(conf)
      sys.addShutdownHook {
        conn.close()
      }
      conn
    }
    new HbaseSink(f)
  }

}