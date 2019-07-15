package cn.crawler.util;

import org.apache.flink.types.StringValue;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Created by liuliang
 * on 2019/5/28
 */
public class RedisUtil {

    private static JedisPool jedisPool;


    public RedisUtil() {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(20);
        config.setMaxIdle(8);
        config.setMaxWaitMillis(100000L);
        config.setTestOnBorrow(true);
        config.setTestOnReturn(true);
        this.jedisPool = new JedisPool(config, "www.iamcrawler.cn", 6379, 30000);
    }


    public void setValue(String key, String value) {
        Jedis jedis = jedisPool.getResource();
        jedis.set("agent:tree:" + key, value);
        jedis.close();
    }


}
