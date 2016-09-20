package sequence_generator;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

import redis.clients.jedis.ShardedJedis;
import sequence_generator.config.Config;
import sequence_generator.utils.CacheUtil;
import sequence_generator.utils.MongoUtil;

public class SequenceGenerator {

	private static final Logger log = LoggerFactory.getLogger(SequenceGenerator.class);
	
	private static final String collection = "sys_serialid";
	
	private static final String SERIAL_CACHE = "sys_serial_id";
	
	// 触发 查询并添加新的序列
	private static final Long CACHE_LIMIT = 100L;
	
//	每次添加序列数
	private static final int CACHE_STEP = 1000;
	
	private static List<String> members = new ArrayList<String>();
	
	private static ReentrantLock lock = new ReentrantLock();
	
	static {
		CacheUtil.initRedis(Config.redisuri);
		try {
			MongoUtil.initMongoClient(Config.uri);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
	}
	
	public static String generate(String pattern) {
		
		ShardedJedis jedis = CacheUtil.getJedis();
		String key = String.format("%s_%s", SERIAL_CACHE, pattern);
		lock.lock();
		Long count = jedis.llen(key);
		
//		log.debug(String.format("%d sequence remind", count));
		if (count < CACHE_LIMIT) {
			cacheSequence(pattern, key);
		}
		lock.unlock();
		Long value = Long.parseLong(jedis.lpop(key));
		String serial = String.format("%s%06d", pattern, value);
//		log.debug(String.format("get serial %d generate sequence %s", value, serial));
		return  serial;
	}
	
	// 生成并添加序列缓存
	private static void cacheSequence(String pattern, String key) {
		Long seqMax = incr(pattern, CACHE_STEP);
		long i = seqMax - CACHE_STEP + 1;
		members.clear();
		while (i <= seqMax) {
			members.add(String.valueOf(i));
			i++;
		}
		ShardedJedis jedis = CacheUtil.getJedis();
		String[] a = new String[CACHE_STEP];
		Long n = jedis.rpush(key, members.toArray(a));
		log.debug(String.format("add %d to %s", n, key));
	}
	
	// 修改序列数据库
	private static Long incr(String pattern, int step) {
		DBCollection col = MongoUtil.getCollection(collection);
		DBObject query = new BasicDBObject("yyyymm", pattern);
		DBObject fields = new BasicDBObject("serialid", true);
		DBObject sort = new BasicDBObject();
		DBObject update = new BasicDBObject();
        update.put("$inc", new BasicDBObject("serialid", step));		
		Object object = col.findAndModify(query, fields, sort, false, update, true, true).get("serialid");
		
		return Long.parseLong(object.toString());
	}
	
	public static void destory() {
		CacheUtil.close(null);
		MongoUtil.close();
	}
}
