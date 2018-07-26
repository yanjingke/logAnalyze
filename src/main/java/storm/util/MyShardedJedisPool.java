package storm.util;

import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;

import java.util.LinkedList;
import java.util.List;

public class MyShardedJedisPool {
    private static ShardedJedisPool shardedJedisPool;

    // 静态代码初始化池配置
    static {
        //change "maxActive" -> "maxTotal" and "maxWait" -> "maxWaitMillis" in all examples
        JedisPoolConfig config = new JedisPoolConfig();
        //控制一个pool最多有多少个状态为idle(空闲的)的jedis实例。
        config.setMaxIdle(5);
        //控制一个pool可分配多少个jedis实例，通过pool.getResource()来获取；
        //如果赋值为-1，则表示不限制；如果pool已经分配了maxActive个jedis实例，则此时pool的状态为exhausted(耗尽)。
        //在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；
        config.setMaxTotal(-1);
        //表示当borrow(引入)一个jedis实例时，最大的等待时间，如果超过等待时间，则直接抛出JedisConnectionException；
        config.setMaxWaitMillis(5);
        config.setTestOnBorrow(true);
        config.setTestOnReturn(true);
        //创建四个redis服务实例，并封装在list中
        List<JedisShardInfo> list = new LinkedList<JedisShardInfo>();
        list.add(new JedisShardInfo("hadoop", 6379));
        //创建具有分片功能的的Jedis连接池
        shardedJedisPool = new ShardedJedisPool(config, list);
    }
    public static ShardedJedisPool getShardedJedisPool() {
        return shardedJedisPool;
    }
    public static void main(String[] args) {
        ShardedJedis jedis = MyShardedJedisPool.getShardedJedisPool().getResource();
        jedis.sadd("user1","liuling");
        jedis.sadd("user1","xinxin");
        jedis.sadd("user1","ling");
        jedis.sadd("user1","zhangxinxin");
        jedis.sadd("user1","zhangxinxin");
        jedis.sadd("user1","zhangxinxin");
        jedis.sadd("user1","zhangxinxin");
        jedis.sadd("user1","who");
        //移除noname
        jedis.srem("user1","who");
        System.out.println(jedis.smembers("user1"));//获取所有加入的value
        System.out.println(jedis.sismember("user1", "who"));//判断 who 是否是user集合的元素
        System.out.println(jedis.srandmember("user1"));
        System.out.println(jedis.scard("user1"));//返回集合的元素个数
    }
}
