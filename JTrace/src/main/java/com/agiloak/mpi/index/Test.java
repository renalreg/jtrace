package com.agiloak.mpi.index;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
 
public class Test {
	
	private static String DB_DRIVER = "org.postgresql.Driver";
	private static final String DB_CONNECTION = "jdbc:postgresql://";
	private static String DB_USER = "postgres";
	private static String DB_PASSWORD = "postgres";
	private static String DB_SERVER = "w2k3-02:5432";
	private static String DB_NAME = "";


	public static void main(String[] args) {
 
		String endpoint="";
		
		System.out.println(System.currentTimeMillis());

		endpoint = getEndpoint("/data");
		System.out.println(endpoint);
		System.out.println(System.currentTimeMillis());

		endpoint = getEndpoint("/data");
		System.out.println(endpoint);
		System.out.println(System.currentTimeMillis());
		
		endpoint = getEndpoint("/data");
		System.out.println(endpoint);
		System.out.println(System.currentTimeMillis());
	}
	/**
	 * Gets a physical endpoint 
	 *  
	 * @return 	A physical endpoint
	 * 
	 */
	public static String getEndpoint(String resource) {

		String query = "Select \"PhysicalURL\" FROM \"Routing\" where \"Resource\" = ? ";
		PreparedStatement preparedStatement = null;
		Connection conn = null;
		ResultSet rs = null;
		
		String endpoint = ""; 
		try {

			conn = getDBConnection();
			preparedStatement = conn.prepareStatement(query);
			preparedStatement.setString(1, resource);
			rs = preparedStatement.executeQuery();

			while (rs.next()){
				endpoint = rs.getString("physicalURL");
			}


		} catch (SQLException e) {
			System.out.println("Failure in selecting route");

		} finally {

			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					//logger.error("Failure In Cleanup",e);
				}
			}
			
			if (preparedStatement != null) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					//logger.error("Failure In Cleanup",e);
				}
			}

			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					//logger.error("Failure In Cleanup",e);
				}
			}

		}

		return endpoint;
	}
		
	/**
	 * Get the Database Connection using configured connection properties 
	 *  
	 * @return  Connection  A Connection to the MomTech Database
	 */
	public static Connection getDBConnection() {
		 
		Connection dbConnection = null;
	
		try {
	
			Class.forName(DB_DRIVER);
	
		} catch (ClassNotFoundException e) {
			System.out.println("Failure to load driver");
			//logger.error("Failure Load JDBC Driver",e);
	
		}
	
		try {
			String connectionString = DB_CONNECTION+DB_SERVER+"/"+DB_NAME;
			dbConnection = DriverManager.getConnection(
					connectionString, DB_USER, DB_PASSWORD);
			return dbConnection;
	
		} catch (SQLException e) {
			System.out.println("Failure to connect");
			//logger.error("Failed to get Database Connection",e);
	
		}
		
		System.out.println(dbConnection);
		
		return dbConnection;
	
	}
 
}