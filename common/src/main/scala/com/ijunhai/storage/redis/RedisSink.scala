package com.ijunhai.storage.redis

import java.util

import com.ijunhai.common.redis.PropertiesUtils
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}
import com.ijunhai.common.redis.RedisClientConstants._
import org.apache.commons.pool2.impl.GenericObjectPoolConfig.DEFAULT_MIN_IDLE

class RedisSink(getConnect: () => Jedis) extends Serializable {
  lazy val jedis = getConnect()

  def expire(key: String, value: Int): Unit = {
    jedis.expire(key, value)
  }

  def incrBy(key: String, value: Long): Unit = {
    jedis.incrBy(key, value)
  }

  def hIncrBy(key: String, field: String, value: Long): Unit = {
    jedis.hincrBy(key, field, value)
  }

  def incr(key: String): Unit = {
    jedis.incr(key)
  }

  def set(key: String, value: String): String = {
    jedis.set(key, value)
  }

  def setex(key: String, second: Int, value: String): String = {
    jedis.setex(key, second, value)
  }

  def hmset(key: String, value: util.Map[String, String]): Any = {
    try {
      jedis.hmset(key, value)
    } catch {
      case _: Exception =>
        jedis.close()
    }
  }

  def hgetAll(key: String): util.Map[String, String] = {
    jedis.hgetAll(key)
  }

  def get(key: String): String = {
    jedis.get(key)
  }

  def hmget(key: String, second: String): util.List[String] = {
    try {
      jedis.hmget(key, second)
    } catch {
      case _: Exception =>
        jedis.close();
        new util.concurrent.CopyOnWriteArrayList[String]() {
          {
            add(null)
          }
        }
    }
  }

  def hmget(key: String, second: String, third: String): util.List[String] = {
    try {
      jedis.hmget(key, second, third)
    } catch {
      case _: Exception =>
        jedis.close()
        new util.concurrent.CopyOnWriteArrayList[String]() {
          {
            add(null)
            add(null)
          }
        }
    }
  }

  def hmget(key: String, second: String, third: String, forth: String): util.List[String] = {
    jedis.hmget(key, second, third, forth)
  }

  def hmget(key: String, second: String, third: String, forth: String, five: String): util.List[String] = {
    jedis.hmget(key, second, third, forth, five)
  }

  def hset(key: String, second: String, third: String): java.lang.Long = {
    try {
      jedis.hset(key, second, third)
    } catch {
      case _: Exception =>
        jedis.close()
        0.toLong
    }
  }

  def exists(key: String): Boolean = {
    jedis.exists(key)
  }

  def close(): Unit = {
    if(jedis != null){
      jedis.close()
    }
  }

}

object RedisSink {
  def apply(): RedisSink = {
    val f = () => {
      val config: JedisPoolConfig = new JedisPoolConfig
      val host: String = "10.13.134.171"
      val port: Int = 6379
      config.setMaxTotal(PropertiesUtils.getInt(REDIS_MaxTotal, 1000))
      config.setMaxIdle(PropertiesUtils.getInt(REDIS_MaxIdle, 1000))
      config.setMinIdle(PropertiesUtils.getInt(REDIS_MinIdle, DEFAULT_MIN_IDLE))
      config.setMaxWaitMillis(PropertiesUtils.getLong(REDIS_MaxWaitMillis, 60000))
      config.setTestOnBorrow(true)
      val REDIS_TIMEOUT: Int = 3000
      val pool = new JedisPool(config, host, port, REDIS_TIMEOUT)
      val jedis = pool.getResource
      sys.addShutdownHook {
        jedis.close()
      }
      jedis
    }
    new RedisSink(f)
  }

  def apply(host: String, port: Int): RedisSink = {
    val f = () => {
      val config: JedisPoolConfig = new JedisPoolConfig

      config.setMaxTotal(PropertiesUtils.getInt(REDIS_MaxTotal, 1000))
      config.setMaxIdle(PropertiesUtils.getInt(REDIS_MaxIdle, 1000))
      config.setMinIdle(PropertiesUtils.getInt(REDIS_MinIdle, DEFAULT_MIN_IDLE))
      config.setMaxWaitMillis(PropertiesUtils.getLong(REDIS_MaxWaitMillis, 60000))
      config.setTestOnBorrow(true)
      val REDIS_TIMEOUT: Int = 3000
      val pool = new JedisPool(config, host, port, REDIS_TIMEOUT)
      val jedis = pool.getResource
      sys.addShutdownHook {
        jedis.close()
      }
      jedis
    }
    new RedisSink(f)
  }
}
