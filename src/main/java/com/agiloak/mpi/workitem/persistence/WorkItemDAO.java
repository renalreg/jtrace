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
import com.agiloak.mpi.workitem.WorkItem;

public class WorkItemDAO {
	
	private final static Logger logger = LoggerFactory.getLogger(WorkItemDAO.class);

	public static void create(Connection conn, WorkItem workItem) throws MpiException {

		logger.debug("Starting");
		
		String insertSQL = "Insert into workitem "+
				"(personid, masterid, type, description, attributes, status, updatedby, updatedesc)"+ 
				" values (?,?,?,?,?,?,?,?)";
		
		PreparedStatement preparedStatement = null;
		
		try {

			preparedStatement = conn.prepareStatement(insertSQL, Statement.RETURN_GENERATED_KEYS);
			preparedStatement.setInt(1, workItem.getPersonId());
			preparedStatement.setInt(2, workItem.getMasterId());
			preparedStatement.setInt(3, workItem.getType());
			preparedStatement.setString(4, workItem.getDescription());
			preparedStatement.setString(5, workItem.getAttributesJson());
			preparedStatement.setInt(6, workItem.getStatus());
			preparedStatement.setString(7, workItem.getUpdatedBy());
			preparedStatement.setString(8, workItem.getUpdateDesc());

			int affectedRows = preparedStatement.executeUpdate();
			logger.debug("Affected Rows:"+affectedRows);
			
		    try (ResultSet generatedKeys = preparedStatement.getGeneratedKeys()) {
	            if (generatedKeys.next()) {
	            	logger.debug("WorkItem ID:"+generatedKeys.getInt(1));
	            	workItem.setId(generatedKeys.getInt("id"));
	            	workItem.setCreationDate(generatedKeys.getTimestamp("creationdate"));
	            	workItem.setLastUpdated(generatedKeys.getTimestamp("lastupdated"));
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
	
	public static void update(Connection conn, WorkItem workItem) throws MpiException {

		logger.debug("Starting");

		workItem.setLastUpdated(new Timestamp(System.currentTimeMillis()));
		String updateSQL = "update workitem "
				+"set status =?, lastupdated=?, updatedby=?, updatedesc=? "
				+"where personid=? and masterid=?"; 
		
		PreparedStatement preparedStatement = null;
		
		try {

			preparedStatement = conn.prepareStatement(updateSQL);
			preparedStatement.setInt(1, workItem.getStatus());
			preparedStatement.setTimestamp(2, workItem.getLastUpdated());
			preparedStatement.setString(3, workItem.getUpdatedBy());
			preparedStatement.setString(4, workItem.getUpdateDesc());
			preparedStatement.setInt(5, workItem.getPersonId());
			preparedStatement.setInt(6, workItem.getMasterId());

			int affectedRows = preparedStatement.executeUpdate();
			logger.debug("Affected Rows:"+affectedRows);
					 
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
	
	public static void deleteByPerson(Connection conn, int personId) throws MpiException {

		logger.debug("Starting");

		String deleteSQL = "delete from workitem where personid = ?";
		
		PreparedStatement preparedStatement = null;
		
		try {

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
	
	public static void deleteByMasterId(Connection conn, int masterId) throws MpiException {

		logger.debug("Starting");

		String deleteSQL = "delete from workitem where masterid = ?";
		
		PreparedStatement preparedStatement = null;
		
		try {

			preparedStatement = conn.prepareStatement(deleteSQL);
			preparedStatement.setInt(1, masterId);

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
	
	public static WorkItem get(Connection conn, int id) throws MpiException {

		logger.debug("Starting");
		
		String findSQL = "select * from workitem where id = ? ";
		
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		
		WorkItem workItem = null;
	
		try {

			preparedStatement = conn.prepareStatement(findSQL);
			preparedStatement.setInt(1, id);

			rs = preparedStatement.executeQuery();
			
			if (rs.next()){
				int pid = rs.getInt("personid");
				int mid = rs.getInt("masterid");
				int type = rs.getInt("type");
				String desc = rs.getString("description");
				String attributes = rs.getString("attributes");
				workItem = new WorkItem(type, pid, mid, desc, attributes);
				workItem.setStatus(rs.getInt("status"));
				workItem.setId(rs.getInt("id"));
				workItem.setLastUpdated(rs.getTimestamp("lastUpdated"));
				workItem.setCreationDate(rs.getTimestamp("creationDate"));
				workItem.setUpdatedBy(rs.getString("updatedby"));
				workItem.setUpdateDesc(rs.getString("updatedesc"));
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
		
		return workItem;

	}

	public static List<WorkItem> findByPerson(Connection conn, int personId) throws MpiException {

		logger.debug("Starting");

		String findSQL = "select * from workitem where personid = ? ";
		
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		
		List<WorkItem> workItems = new ArrayList<WorkItem>();
		
		try {

			preparedStatement = conn.prepareStatement(findSQL);
			preparedStatement.setInt(1, personId);

			rs = preparedStatement.executeQuery();
			
			while (rs.next()){
				int pid = rs.getInt("personid");
				int mid = rs.getInt("masterid");
				int type = rs.getInt("type");
				String desc = rs.getString("description");
				String attributes = rs.getString("attributes");
				WorkItem item  = new WorkItem(type, pid, mid, desc, attributes);
				item.setStatus(rs.getInt("status"));
				item.setId(rs.getInt("id"));
				item.setLastUpdated(rs.getTimestamp("lastUpdated"));
				item.setCreationDate(rs.getTimestamp("creationDate"));
				item.setUpdatedBy(rs.getString("updatedby"));
				item.setUpdateDesc(rs.getString("updatedesc"));
				
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
	public static WorkItem findByPersonAndMaster(Connection conn, int personId, int masterId) throws MpiException {

		logger.debug("Starting");
		
		String findSQL = "select * from workitem where personid = ? and masterid = ?";
		
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		
		WorkItem workItem = null;
	
		try {

			preparedStatement = conn.prepareStatement(findSQL);
			preparedStatement.setInt(1, personId);
			preparedStatement.setInt(2, masterId);

			rs = preparedStatement.executeQuery();
			
			if (rs.next()){
				int pid = rs.getInt("personid");
				int mid = rs.getInt("masterid");
				int type = rs.getInt("type");
				String desc = rs.getString("description");
				String attributes = rs.getString("attributes");
				workItem = new WorkItem(type, pid, mid, desc, attributes);
				workItem.setStatus(rs.getInt("status"));
				workItem.setId(rs.getInt("id"));
				workItem.setLastUpdated(rs.getTimestamp("lastUpdated"));
				workItem.setCreationDate(rs.getTimestamp("creationDate"));
				workItem.setUpdatedBy(rs.getString("updatedby"));
				workItem.setUpdateDesc(rs.getString("updatedesc"));
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
		
		return workItem;

	}		
}
