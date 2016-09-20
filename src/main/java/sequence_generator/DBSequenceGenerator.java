package sequence_generator;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSource;

public class DBSequenceGenerator {

	private static Map<String,String> dbms;
	private static Map<String,String> drivers;
	private static Map<String,String> portNumber;
	
	private static String serverName = "192.168.1.139";
	
	
	private static String username = "generator";
	
	private static String password = "generator123";
	
	private static String dbName = "sequence_standalone";
	
	private static DataSource connectionPool = null;
	
	private static String db = "postgresql";
	
	static {
		dbms = new HashMap<>();
		drivers = new HashMap<>();
		portNumber = new HashMap<>();
		dbms.put("mysql", "mysql");
		drivers.put("mysql", "org.mariadb.jdbc.Driver");
		portNumber.put("mysql", "16001");
		dbms.put("postgresql", "postgresql");
		drivers.put("postgresql", "org.postgresql.Driver");
		portNumber.put("postgresql", "15001");
		createPool();
	}
	
	private static void createPool() {
		BasicDataSource ds = new BasicDataSource();
		
		String dbUrl = String.format("jdbc:%s://%s:%s/%s", dbms.get(db), serverName, portNumber.get(db), dbName);
        
		ds.setDriverClassName(drivers.get(db));
		ds.setUrl(dbUrl);
		ds.setInitialSize(10);
		ds.setUsername(username);
		ds.setPassword(password);
		ds.setMaxTotal(10);
		connectionPool = ds;
	}
	
	private static Connection getConnection() throws SQLException {

	    Connection conn = connectionPool.getConnection();
	    
	    return conn;
	}
	
	public static Long generate() {
		if ("mysql".equals(db)) {
			return mysqlGenerator();
		} else if ("postgresql".equals(db)){
			return postgresGenerator();
		}
		throw new RuntimeException("db not exist");
	}

	private static Long mysqlGenerator() {
		Connection conn = null;
		try {
			conn = getConnection();
			String sql = "{CALL generate()}";
			CallableStatement prepareCall = conn.prepareCall(sql);
			
			boolean hadResults = prepareCall.execute();
			if (hadResults) {
				ResultSet rs = prepareCall.getResultSet();
				if (rs.next()) {
					return rs.getLong("LAST_INSERT_ID()");
				}
				throw new RuntimeException("generate failed");
			}
			throw new RuntimeException("generate failed");
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} finally {
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}
	private static Long postgresGenerator() {
		Connection conn = null;
		try {
			conn = getConnection();
			String sql = "SELECT nextval('ticket_seq')";
			CallableStatement prepareCall = conn.prepareCall(sql);
			
			boolean hadResults = prepareCall.execute();
			if (hadResults) {
				ResultSet rs = prepareCall.getResultSet();
				if (rs.next()) {
					return rs.getLong("nextval");
				}
				throw new RuntimeException("generate failed");
			}
			throw new RuntimeException("generate failed");
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} finally {
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	
}
