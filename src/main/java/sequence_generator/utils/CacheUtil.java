/**
 * 
 */
package sequence_generator.utils;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;
import redis.clients.util.Hashing;
import redis.clients.util.Sharded;

/**
 * 缓存工具类
 * 
 */
public class CacheUtil {

	private static final Logger log = LoggerFactory.getLogger(CacheUtil.class);

	private static JedisPoolConfig config;
	
	public static ShardedJedisPool sharedJedisPool;

	/**
	 * 初始化Redis连接
	 * 
	 * @param redisuri
	 */
	public static void initRedis(String redisuri) {
		log.info("初始化缓存：{}", redisuri);
		// 生成多机连接List
		List<JedisShardInfo> shards = new ArrayList<JedisShardInfo>();
		// shards.add( new JedisShardInfo("localhost", 6379) );
		// shards.add( new JedisShardInfo("localhost", 6380) );
		try {
			shards.add(new JedisShardInfo(new URI(redisuri))); // 密码@地址:端口
			// shards.add(new JedisShardInfo(new URI("redis://:foobared@localhost:6380")));
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}

		// 初始化连接池配置对象

		config = new JedisPoolConfig();
		config.setMaxIdle(1000);
		config.setMaxTotal(10000); // 30
		config.setMaxWaitMillis(10*000); // 3*1000

		// 实例化连接池
		sharedJedisPool = new ShardedJedisPool(config, shards, Hashing.MURMUR_HASH, Sharded.DEFAULT_KEY_TAG_PATTERN);
		// new ShardedJedisPool(config, shards);
	}

	/**
	 * 关闭缓存连接
	 * 
	 * @param shardedJedis
	 * @param sharedJedisPool
	 */
	public static void close(ShardedJedis shardedJedis) {
		if (shardedJedis != null && sharedJedisPool != null) {
			sharedJedisPool.returnResource(shardedJedis);
		}
		if (sharedJedisPool != null) {
			log.debug("关闭缓存: {}", sharedJedisPool);
			sharedJedisPool.destroy();
		}
	}

	public static ShardedJedis getJedis() {
		return CacheUtil.sharedJedisPool.getResource();
	}
}
