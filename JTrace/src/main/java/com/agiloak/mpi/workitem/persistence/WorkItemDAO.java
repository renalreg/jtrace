package com.agiloak.mpi.workitem.persistence;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.SimpleConnectionManager;
import com.agiloak.mpi.workitem.WorkItem;

public class WorkItemDAO {
	
	private final static Logger logger = LoggerFactory.getLogger(WorkItemDAO.class);

	public static void create(WorkItem workItem) throws MpiException {

		
		String insertSQL = "Insert into jtrace.workitem "+
				"(personid, type, description, status, creationdate)"+ 
				" values (?,?,?,?,?)";
		
		PreparedStatement preparedStatement = null;
		Connection conn = null;
		
		try {

			conn = SimpleConnectionManager.getDBConnection();
			
			preparedStatement = conn.prepareStatement(insertSQL, Statement.RETURN_GENERATED_KEYS);
			preparedStatement.setInt(1, workItem.getPersonId());
			preparedStatement.setInt(2, workItem.getType());
			preparedStatement.setString(3, workItem.getDescription());
			preparedStatement.setInt(4, workItem.getStatus());
			preparedStatement.setTimestamp(5,new Timestamp(workItem.getCreationTime().getTime()));

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
			throw new MpiException("LinkRecord insert failed");

		} finally {

			if (preparedStatement != null) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					logger.error("Failure closing Prepared Statement:",e);
					throw new MpiException("LinkRecord insert failed");
				}
			}

		}

	}
	
}
