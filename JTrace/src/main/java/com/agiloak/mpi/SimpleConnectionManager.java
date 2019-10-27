package com.agiloak.mpi;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.dbcp.PoolingDataSource;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.agiloak.mpi.trace.persistence.TraceDAO;

public class SimpleConnectionManager {

	private final static Logger logger = LoggerFactory.getLogger(TraceDAO.class);

	private static String DB_DRIVER = "org.postgresql.Driver";
	private static final String DB_CONNECTION = "jdbc:postgresql://";

	private static String DB_USER = "postgres";
	private static String DB_PASSWORD = "postgres";
	private static String DB_SERVER = "localhost:5432";
	private static String DB_NAME = "JTRACE";
	private static String DB_SCHEMA = null;
	private static GenericObjectPool gPool = null;
	private static DataSource dataSource = null;
	
	private final static int defaultPoolSize = 10;
	
	// For test only - default the db connection settings
	public static void reset() throws MpiException {
		logger.info("JTRACE database configuration reset to default:");
		DB_USER = "postgres";
		DB_PASSWORD = "postgres";
		DB_SERVER = "localhost:5432";
		DB_NAME = "JTRACE";
		DB_SCHEMA = null;
		
		try {
			dataSource = setUpPool(defaultPoolSize);
		} catch (Exception e) {
			logger.error("Failed to setup connection pool");
			throw new MpiException("CONN POOL ERROR:"+e.getMessage());
		}
	}
	
	/**
	 * Legacy support for earlier version - default poolsize to defaultPoolSize
	 * Allow the connection to be configured externally. Hardcoded values remain as defaults (for now).
	 */
	public static void configure(String user, String password, String server, String port, String dbName) throws MpiException {
		logger.warn("Using default pool size:"+defaultPoolSize);

		configure(user, password, server, port, dbName, defaultPoolSize);
	}

	/**
	 * Legacy support for earlier version - default poolsize to defaultPoolSize and schema to null
	 * Allow the connection to be configured externally. Hardcoded values remain as defaults (for now).
	 */
	public static void configure(String user, String password, String server, String port, String dbName, int poolSize) throws MpiException {
		logger.warn("Using default pool size:"+defaultPoolSize);

		configure(user, password, server, port, dbName, null, defaultPoolSize);
	}

	/**
	 * Allow the connection to be configured externally. Hardcoded values remain as defaults (for now).
	 * @param user
	 * @param password
	 * @param server
	 * @param port
	 * @param dbName
	 * @param poolsize
	 * @param schema
	 */
	public static void configure(String user, String password, String server, String port, String dbName, String schema, int poolSize) throws MpiException {
		
		if (user==null || password == null || server == null || port == null || dbName == null) {
			logger.error("Invalid database configuration. 1 or more null parameters");
			throw new MpiException("Invalid database configuration. 1 or more null parameters");
		}
		if (user.length()==0 || password.length()==0 || server.length()==0l || port.length()==0 || dbName.length()==0 ) {
			logger.error("Invalid database configuration. 1 or more empty parameters");
			throw new MpiException("Invalid database configuration. 1 or more empty parameters");
		}
		
		logger.info("JTRACE database configuration change:");
		DB_USER = user;
		DB_PASSWORD = password;
		DB_SERVER = server + ":" + port;
		DB_NAME = dbName;
		DB_SCHEMA = schema;
		try {
			dataSource = setUpPool(poolSize);
		} catch (Exception e) {
			logger.error("Failed to setup connection pool");
			throw new MpiException("CONN POOL ERROR:"+e.getMessage());
		}
	}
	public static DataSource setUpPool(int poolSize) throws Exception{
		Class.forName(DB_DRIVER);
        // Creates an Instance of GenericObjectPool That Holds Our Pool of Connections Object!
        gPool = new GenericObjectPool();
        gPool.setMaxActive(poolSize);

        // Creates a ConnectionFactory Object Which Will Be Use by the Pool to Create the Connection Object!
        String dbURL = DB_CONNECTION + DB_SERVER + "/" +DB_NAME;
        if (DB_SCHEMA != null) {
        	dbURL += "?currentSchema="+DB_SCHEMA;
        }
        ConnectionFactory cf = new DriverManagerConnectionFactory(dbURL, DB_USER, DB_PASSWORD);

		// Creates a PoolableConnectionFactory That Will Wraps the Connection Object Created by the ConnectionFactory to Add Object Pooling Functionality!
        new PoolableConnectionFactory(cf, gPool, null, null, false, true);
        return new PoolingDataSource(gPool);

	}
	/**
	 * Get the Database Connection using configured connection properties 
	 *  
	 *  NOTE: May need a pooling mechanism to scale
	 *  
	 * @return  Connection  A Connection to the Database
	 */
	public static Connection getDBConnection() throws MpiException {
		Connection conn = null;
		try {
			conn = dataSource.getConnection();
		} catch (SQLException e) {
			logger.error("Failed to get connection from pool");
			throw new MpiException("CONN POOL ERROR:"+e.getMessage());
		}
		logger.debug("Returning Connection:"+((Object)conn).hashCode());
		return conn;
	}
	/* 
	 * Helper methods to streamline the connection management and error response handling in API functions
	 */
	public static void closeConnection(Connection conn) throws MpiException {
		// Tidy up the connection
		if( conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				logger.error("Failure closing Connection",e);
				throw new MpiException("Failure closing Connection"+e.getMessage());
			}
		}
	}
	public static void rollback(Connection conn, Exception ex) throws Exception {
		conn.rollback();
		throw ex;
	}
	public static Connection getConnection() throws Exception {
		Connection conn = SimpleConnectionManager.getDBConnection();
		conn.setAutoCommit(false);
		return conn;
	}
	/*
	 * End of Helper methods
	 */	

}
