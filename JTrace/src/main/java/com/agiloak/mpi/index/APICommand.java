package com.agiloak.mpi.index;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.SimpleConnectionManager;

public abstract class APICommand {

	private final static Logger logger = LoggerFactory.getLogger(APICommand.class);

	public abstract UKRDCIndexManagerResponse execute(UKRDCIndexManager im, Connection conn) throws Exception;

	/* 
	 * Helper methods to streamline the connection management and error response handling in API functions
	 */
	private void closeConnection(Connection conn) throws MpiException {
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
	private void rollback(Connection conn, Exception ex) throws Exception {
		conn.rollback();
		throw ex;
	}
	private Connection getConnection() throws Exception {
		Connection conn = SimpleConnectionManager.getDBConnection();
		conn.setAutoCommit(false);
		return conn;
	}
	private UKRDCIndexManagerResponse getErrorResponse(Exception ex) {
		UKRDCIndexManagerResponse resp = new UKRDCIndexManagerResponse();
		resp.setStatus(UKRDCIndexManagerResponse.FAIL);
		resp.setMessage(ex.getMessage());
		resp.setStackTrace(ExceptionUtils.getStackTrace(ex));
		return resp;
	}
	/*
	 * End of Helper methods
	 */	

	public UKRDCIndexManagerResponse executeAPICommand(UKRDCIndexManager im) {
		
		UKRDCIndexManagerResponse resp = null;
		Connection conn = null;
		try {
			try {
				conn = getConnection();
				resp = execute(im, conn);
				conn.commit();
			} catch (Exception ex) {
				rollback(conn, ex);
			} finally {
				closeConnection(conn);
			}
		} catch (Exception ex) {
			resp = getErrorResponse(ex);
		}
		return resp;		
	}
}
