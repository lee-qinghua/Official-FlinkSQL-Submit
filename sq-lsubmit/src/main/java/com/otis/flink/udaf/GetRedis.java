package com.otis.flink.udaf;

import com.otis.flink.util.JedisPoolUtil;
import org.apache.flink.table.functions.ScalarFunction;
import redis.clients.jedis.Jedis;

public class GetRedis extends ScalarFunction {
    private Jedis jedis;

    @Override
    public void close() throws Exception {
        JedisPoolUtil.close(jedis);
    }

    public String eval(String key) {
        jedis = JedisPoolUtil.getJedis();

        return jedis.get(key);
    }
}
