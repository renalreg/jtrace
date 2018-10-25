package com.agiloak.mpi.index.persistence;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.SimpleConnectionManager;

public abstract class NumberAllocatingDAO {

	final static Logger logger = LoggerFactory.getLogger(NumberAllocatingDAO.class);

	protected static String allocateSequence(String sequenceName) throws MpiException {
	
		logger.debug("Starting");
	
		String nextValSQL = "select nextval('"+sequenceName+"')";
		
		String  allocatedId = "";
	
		PreparedStatement preparedStatement = null;
		Connection conn = null;
		ResultSet rs = null;
		
		try {
	
			conn = SimpleConnectionManager.getDBConnection();
			
			preparedStatement = conn.prepareStatement(nextValSQL);
	
			rs = preparedStatement.executeQuery();
			
			if (rs.next()){
				allocatedId = new Integer(rs.getInt("nextval")).toString();
			}
			
		} catch (Exception e) {
			logger.error("Failure querying:"+sequenceName,e);
			throw new MpiException("Failure querying:"+sequenceName+", "+e.getMessage());
		} finally { 
	
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					logger.error("Failure closing resultset.",e);
					throw new MpiException("Failure closing resultset. "+e.getMessage());
				}
			}
			
			if (preparedStatement != null) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					logger.error("Failure closing prepared statement.",e);
					throw new MpiException("Failure closing prepared statement. "+e.getMessage());
				}
			}
			if(conn!= null) {
				try {
					conn.close();
				} catch (SQLException e) {
					logger.error("Failure closing Connection",e);
					throw new MpiException(sequenceName+" allocate failed. "+e.getMessage());
				}
			}
	
		}
		logger.debug("Complete");
		return allocatedId;
		
	}

	public NumberAllocatingDAO() {
		super();
	}

}