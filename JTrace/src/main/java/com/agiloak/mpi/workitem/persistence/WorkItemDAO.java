package com.agiloak.mpi.workitem.persistence;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.agiloak.mpi.SimpleConnectionManager;
import com.agiloak.mpi.workitem.WorkItem;

public class WorkItemDAO {
	
	private final static Logger logger = LoggerFactory.getLogger(WorkItemDAO.class);

	public static void insert(WorkItem workItem) {

		
		String insertSQL = "Insert into jtrace.workitem "+
				"(description, status, creationdate)"+ 
				" values (?,?,?)";
		
		PreparedStatement preparedStatement = null;
		Connection conn = null;
		
		try {

			conn = SimpleConnectionManager.getDBConnection();
			
			preparedStatement = conn.prepareStatement(insertSQL, Statement.RETURN_GENERATED_KEYS);
			preparedStatement.setString(1, workItem.getDescription());
			preparedStatement.setInt(2, workItem.getStatus());
			preparedStatement.setTimestamp(3,new Timestamp(workItem.getCreationTime().getTime()));

			int affectedRows = preparedStatement.executeUpdate();
			logger.debug("Affected Rows:"+affectedRows);
			
		    try (ResultSet generatedKeys = preparedStatement.getGeneratedKeys()) {
	            if (generatedKeys.next()) {
	            	logger.debug("WorkItem ID:"+generatedKeys.getLong(1));
	            }
	            else {
	    			logger.error("Creating WorkItem failed, no ID obtained.");
	                throw new SQLException("Creating WorkItem failed, no ID obtained.");
	            }
		    }
		 
		} catch (SQLException e) {
			logger.error("Failure inserting WorkItem:",e);

		} finally {

			if (preparedStatement != null) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					logger.error("Failure closing Prepared Statement:",e);
				}
			}

		}

	}
	
}
