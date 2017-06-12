package org.gooru.nucleus.handlers.dataclass.api.app.components;


import org.gooru.nucleus.handlers.dataclass.api.bootstrap.shutdown.Finalizer;
import org.gooru.nucleus.handlers.dataclass.api.bootstrap.startup.Initializer;
import org.gooru.nucleus.handlers.dataclass.api.constants.MessageConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisClient implements Initializer, Finalizer {

  private static final Logger LOGGER = LoggerFactory.getLogger(RedisClient.class);

  private JedisPool pool = null;

  @Override
  public void initializeComponent(Vertx vertx, JsonObject config) {

    JsonObject redisConfig = config.getJsonObject(MessageConstants.REDIS);
    LOGGER.debug("redis host : {} - port : {} ", redisConfig.getString(MessageConstants.HOST), redisConfig.getInteger(MessageConstants.PORT));
    JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
    jedisPoolConfig.setMaxTotal(redisConfig.getInteger(MessageConstants.REDIS_MAX_SIZE));
    jedisPoolConfig.setMaxIdle(redisConfig.getInteger(MessageConstants.REDIS_MAX_IDLE));
    jedisPoolConfig.setMinIdle(redisConfig.getInteger(MessageConstants.REDIS_MIN_IDLE));
    jedisPoolConfig.setMaxWaitMillis(redisConfig.getInteger(MessageConstants.REDIS_MAX_WAIT_MILLIS));
    jedisPoolConfig.setTestOnBorrow(true);
    try {
      pool = new JedisPool(jedisPoolConfig, redisConfig.getString(MessageConstants.HOST), redisConfig.getInteger(MessageConstants.PORT));
    } catch (Exception e) {
      LOGGER.error("Exception while initializing redis: "+ e);
    }
    LOGGER.debug("redis initialized successfully...");
  }

  public static RedisClient getInstance() {
    return Holder.INSTANCE;
  }

  public JsonObject getJsonObject(final String key) {
    JsonObject result = null;
    Jedis jedis = null;
    try {
      jedis = getJedis();
      String json = jedis.get(key);
      if (json != null) {
        result = new JsonObject(json);
      }
    } finally {
      if (jedis != null) {
        jedis.close();
      }
    }
    return result;
  }

  public String get(final String key) {
    String value = null;
    Jedis jedis = null;
    try {
      jedis = getJedis();
      value = jedis.get(key);
    } finally {
      if (jedis != null) {
        jedis.close();
      }
    }
    return value;
  }

  public void del(String key) {
    Jedis jedis = null;
    try {
      jedis = getJedis();
      jedis.del(key);
    } finally {
      if (jedis != null) {
        jedis.close();
      }
    }
  }

  public void expire(String key, int seconds) {
    Jedis jedis = null;
    try {
      jedis = getJedis();
      jedis.expire(key, seconds);
    } finally {
      if (jedis != null) {
        jedis.close();
      }
    }
  }

  public void set(String key, String value, int expireInSeconds) {
    Jedis jedis = null;
    try {
      jedis = getJedis();
      jedis.set(key, value);
      jedis.expire(key, expireInSeconds);
    } finally {
      if (jedis != null) {
        jedis.close();
      }
    }
  }

  public void set(String key, String value) {
    Jedis jedis = null;
    try {
      jedis = getJedis();
      jedis.set(key, value);
    } finally {
      if (jedis != null) {
        jedis.close();
      }
    }
  }

  private Jedis getJedis() {

    return pool.getResource();
  }

  @Override
  public void finalizeComponent() {
    if (pool != null) {
      pool.destroy();
    }
  }

  private static final class Holder {
    private static final RedisClient INSTANCE = new RedisClient();
  }

}
