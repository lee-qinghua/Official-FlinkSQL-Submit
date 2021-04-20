package com.otis.flink.udaf;

import com.otis.flink.util.JedisPoolUtil;
import org.apache.flink.table.functions.ScalarFunction;
import redis.clients.jedis.Jedis;

public class UpdateRedis extends ScalarFunction {

    private Jedis jedis;

    @Override
    public void close() throws Exception {
        JedisPoolUtil.close(jedis);
    }

    public String eval(String key, int value, int days) {
        jedis = JedisPoolUtil.getJedis();

        jedis.set(key, Integer.toString(value));
        jedis.expire(key, days * 24 * 60 * 60);
        return "ok";
    }
}
