package com.agiloak.mpi;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

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
	private static boolean defaultConnection = true;

	private static Connection dbConn;
	
	// For test only - default the db connection settings
	public static void reset() {
		logger.info("JTRACE database configuration reset to default:");
		DB_USER = "postgres";
		DB_PASSWORD = "postgres";
		DB_SERVER = "localhost:5432";
		DB_NAME = "JTRACE";
		defaultConnection = true;
		dbConn = null; // force reconnect		
	}
	
	/**
	 * Allow the connection to be configured externally. Hardcoded values remain as defaults (for now).
	 * @param user
	 * @param password
	 * @param server
	 * @param port
	 * @param dbName
	 */
	public static void configure(String user, String password, String server, String port, String dbName) throws MpiException {
		
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
		defaultConnection = false;
		dbConn = null; // force reconnect
		getDBConnection();
	}
	
	/**
	 * Get the Database Connection using configured connection properties 
	 *  
	 *  NOTE: May need a pooling mechanism to scale
	 *  
	 * @return  Connection  A Connection to the Database
	 */
	public static Connection getDBConnection() {
		if (dbConn!=null) {
			try {
				if (dbConn.isValid(1)){
					return dbConn;
				}
			} catch (SQLException e) {
				logger.error("Error getting connection:",e);
			}
		} else {
			logger.debug("No connection - get new connection");
		}
		 
		dbConn = null;
	
		try {
	
			Class.forName(DB_DRIVER);
	
			logger.info("JTRACE database connection reset:");
			logger.info("User:"+DB_USER+"/*****");
			logger.info("Endpoint:"+DB_SERVER+"/"+DB_NAME);
			if (defaultConnection) {
				logger.warn("***** DEFAULT DATABASE CONNECTION DETAILS IN USE **********");
				logger.warn("***** DEFAULT DATABASE CONNECTION DETAILS IN USE **********");
				logger.warn("***** DEFAULT DATABASE CONNECTION DETAILS IN USE **********");
			}

		} catch (ClassNotFoundException e) {
			logger.error("Failure Load JDBC Driver",e);
	
		}
	
		try {
			String connectionString = DB_CONNECTION+DB_SERVER+"/"+DB_NAME;
			dbConn = DriverManager.getConnection(
					connectionString, DB_USER, DB_PASSWORD);
			return dbConn;
	
		} catch (SQLException e) {
			logger.error("Failed to get Database Connection",e);
			// Consider throwing MpiException here. Currently will fail in the DAO instead.
		}
		
		return dbConn;
	
	}
}
