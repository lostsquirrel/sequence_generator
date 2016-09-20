package sequence_generator.utils;

import java.net.UnknownHostException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;

public class MongoUtil {

	private static final Logger log = LoggerFactory.getLogger(MongoUtil.class);
	
	private static MongoClient mongo = null;
	
	private static MongoClientURI mongoClientURI = null;
	
	/**
	 * 获取集合（表）
	 * 
	 * @param collectionName
	 */
	public static DBCollection getCollection(String collectionName) {
		return getDB(mongoClientURI.getDatabase()).getCollection(collectionName);
	}

	/**
	 * 获取DB连接
	 * 
	 * @param dbName
	 * @return
	 */
	public static DB getDB(String dbName) {
		if (mongo == null) {
			log.error("在MongoClient没有初始化的情况下获取DB");
			throw new RuntimeException("MongoClient没有初始化");
		}
		return mongo.getDB(dbName);
	}
	
	/**
	 * 初始化连接池
	 * 
	 * @param uri
	 *            mongo连接URI,格式如：mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[database[.collection]][?options]]
	 * @throws UnknownHostException
	 */
	public static void initMongoClient(String uri) throws UnknownHostException {
		// 其他参数根据实际情况进行添加
		MongoClientOptions.Builder builder = MongoClientOptions.builder().connectionsPerHost(2000).threadsAllowedToBlockForConnectionMultiplier(500).connectTimeout(5000).maxWaitTime(120000)
				.socketTimeout(0).readPreference(ReadPreference.secondaryPreferred()).writeConcern(WriteConcern.REPLICA_ACKNOWLEDGED).socketKeepAlive(true).minConnectionsPerHost(20);
		mongoClientURI = new MongoClientURI(uri, builder);

		mongo = new MongoClient(mongoClientURI);

		log.info("初始化数据库连接成功：{}", uri);
	}

	/**
	 * 关闭数据库连接
	 */
	public static void close() {
		if (mongo != null) {
			log.info("关闭数据库连接：{}", mongoClientURI.getURI());
			mongo.close();
		}
	}
}
