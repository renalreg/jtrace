package com.agiloak.mpi.workitem.persistence;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.SimpleConnectionManager;
import com.agiloak.mpi.workitem.WorkItem;

public class WorkItemDAO {
	
	private final static Logger logger = LoggerFactory.getLogger(WorkItemDAO.class);

	public static void create(WorkItem workItem) throws MpiException {

		
		String insertSQL = "Insert into jtrace.workitem "+
				"(personid, type, description, status, lastupdated)"+ 
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
			preparedStatement.setTimestamp(5,new Timestamp(workItem.getLastUpdated().getTime()));

			int affectedRows = preparedStatement.executeUpdate();
			logger.debug("Affected Rows:"+affectedRows);
			
		    try (ResultSet generatedKeys = preparedStatement.getGeneratedKeys()) {
	            if (generatedKeys.next()) {
	            	logger.debug("WorkItem ID:"+generatedKeys.getInt(1));
	            	workItem.setId(generatedKeys.getInt(1));
	            }
	            else {
	    			logger.error("Creating WorkItem failed, no ID obtained.");
	                throw new SQLException("Creating WorkItem failed, no ID obtained.");
	            }
		    }
		 
		} catch (SQLException e) {
			logger.error("Failure inserting WorkItem:",e);
			throw new MpiException("WorkItem insert failed");

		} finally {

			if (preparedStatement != null) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					logger.error("Failure closing Prepared Statement:",e);
					throw new MpiException("WorkItem insert failed");
				}
			}

		}

	}
	
	public static void deleteByPerson(int personId) throws MpiException {
		
		String deleteSQL = "delete from jtrace.workitem where personid = ?";
		
		PreparedStatement preparedStatement = null;
		Connection conn = null;
		
		try {

			conn = SimpleConnectionManager.getDBConnection();
			
			preparedStatement = conn.prepareStatement(deleteSQL);
			preparedStatement.setInt(1, personId);

			int affectedRows = preparedStatement.executeUpdate();
			logger.debug("Affected Rows:"+affectedRows);
					 
		} catch (SQLException e) {
			logger.error("Failure deleting WorkItem:",e);
			throw new MpiException("WorkItem delete failed");

		} finally {

			if (preparedStatement != null) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					logger.error("Failure closing Prepared Statement:",e);
					throw new MpiException("WorkItem delete failed");
				}
			}

		}

	}	
	
	public static List<WorkItem> findByPerson(int personId) throws MpiException {

		String findSQL = "select * from jtrace.workitem where personid = ? ";
		
		PreparedStatement preparedStatement = null;
		Connection conn = null;
		ResultSet rs = null;
		
		List<WorkItem> workItems = new ArrayList<WorkItem>();
		
		try {

			conn = SimpleConnectionManager.getDBConnection();
			
			preparedStatement = conn.prepareStatement(findSQL);
			preparedStatement.setInt(1, personId);

			rs = preparedStatement.executeQuery();
			
			while (rs.next()){
				int pid = rs.getInt("personid");
				int type = rs.getInt("type");
				String desc = rs.getString("description");
				WorkItem item = new WorkItem(type, pid, desc);
				item.setStatus(rs.getInt("status"));
				item.setId(rs.getInt("id"));
				item.setLastUpdated(rs.getTimestamp("lastUpdated"));
				
				workItems.add(item);
			}
			
		} catch (Exception e) {
			logger.error("Failure querying WorkItem.",e);
			throw new MpiException("Failure querying WorkItem. "+e.getMessage());
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

		}
		
		return workItems;

	}	
}
