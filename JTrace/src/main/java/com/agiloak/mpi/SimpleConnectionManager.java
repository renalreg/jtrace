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

	// TODO - Externalise
	private static String DB_USER = "postgres";
	private static String DB_PASSWORD = "postgres";
	private static String DB_SERVER = "W2K16-FHIRALARM:5432";
	private static String DB_NAME = "JTRACE";

	private static Connection dbConn;
	
	/**
	 * Get the Database Connection using configured connection properties 
	 *  
	 *  NOTE: FOR POC ONLY - WILL NEED PROPER CONNECTION MANAGEMENT & POS PERSISTENCE FRAMEWORK FOR LIVE
	 *  
	 * @return  Connection  A Connection to the Database
	 */
	public static Connection getDBConnection() {
		//logger.info("GetConnection");
		if (dbConn!=null) {
			try {
				if (dbConn.isValid(1)){
					//logger.debug("Connection valid - reuse");
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
	
		}
		
		return dbConn;
	
	}
}
