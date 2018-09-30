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

		logger.debug("Starting");
		
		String insertSQL = "Insert into jtrace.workitem "+
				"(personid, masterid, type, description, status, lastupdated, updatedby, updatedesc)"+ 
				" values (?,?,?,?,?,?,?,?)";
		
		PreparedStatement preparedStatement = null;
		Connection conn = null;
		
		try {

			conn = SimpleConnectionManager.getDBConnection();
			
			preparedStatement = conn.prepareStatement(insertSQL, Statement.RETURN_GENERATED_KEYS);
			preparedStatement.setInt(1, workItem.getPersonId());
			preparedStatement.setInt(2, workItem.getMasterId());
			preparedStatement.setInt(3, workItem.getType());
			preparedStatement.setString(4, workItem.getDescription());
			preparedStatement.setInt(5, workItem.getStatus());
			preparedStatement.setTimestamp(6,new Timestamp(workItem.getLastUpdated().getTime()));
			preparedStatement.setString(7, workItem.getUpdatedBy());
			preparedStatement.setString(8, workItem.getUpdateDesc());

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
			if(conn!= null) {
				try {
					conn.close();
				} catch (SQLException e) {
					logger.error("Failure closing Connection",e);
					throw new MpiException("WorkItem insert failed. "+e.getMessage());
				}
			}
		}

	}
	
	public static void update(WorkItem workItem) throws MpiException {

		logger.debug("Starting");

		String updateSQL = "update jtrace.workitem "
				+"set status =?, lastupdated=?, updatedby=?, updatedesc=? "
				+"where personid=? and masterid=?"; 
		
		PreparedStatement preparedStatement = null;
		Connection conn = null;
		
		try {

			conn = SimpleConnectionManager.getDBConnection();
			
			preparedStatement = conn.prepareStatement(updateSQL);
			preparedStatement.setInt(1, workItem.getStatus());
			preparedStatement.setTimestamp(2,new Timestamp(workItem.getLastUpdated().getTime()));
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
			if(conn!= null) {
				try {
					conn.close();
				} catch (SQLException e) {
					logger.error("Failure closing Connection",e);
					throw new MpiException("WorkItem insert failed. "+e.getMessage());
				}
			}

		}

	}
	
	public static void deleteByPerson(int personId) throws MpiException {

		logger.debug("Starting");

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
			if(conn!= null) {
				try {
					conn.close();
				} catch (SQLException e) {
					logger.error("Failure closing Connection",e);
					throw new MpiException("WorkItem delete failed. "+e.getMessage());
				}
			}

		}

	}	
	
	public static void deleteByMasterId(int masterId) throws MpiException {

		logger.debug("Starting");

		String deleteSQL = "delete from jtrace.workitem where masterid = ?";
		
		PreparedStatement preparedStatement = null;
		Connection conn = null;
		
		try {

			conn = SimpleConnectionManager.getDBConnection();
			
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
			if(conn!= null) {
				try {
					conn.close();
				} catch (SQLException e) {
					logger.error("Failure closing Connection",e);
					throw new MpiException("WorkItem delete failed. "+e.getMessage());
				}
			}

		}

	}	
	
	public static WorkItem get(int id) throws MpiException {

		logger.debug("Starting");
		
		String findSQL = "select * from jtrace.workitem where id = ? ";
		
		PreparedStatement preparedStatement = null;
		Connection conn = null;
		ResultSet rs = null;
		
		WorkItem workItem = null;
	
		try {

			conn = SimpleConnectionManager.getDBConnection();
			
			preparedStatement = conn.prepareStatement(findSQL);
			preparedStatement.setInt(1, id);

			rs = preparedStatement.executeQuery();
			
			if (rs.next()){
				int pid = rs.getInt("personid");
				int mid = rs.getInt("masterid");
				int type = rs.getInt("type");
				String desc = rs.getString("description");
				workItem = new WorkItem(type, pid, mid, desc);
				workItem.setStatus(rs.getInt("status"));
				workItem.setId(rs.getInt("id"));
				workItem.setLastUpdated(rs.getTimestamp("lastUpdated"));
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
			if(conn!= null) {
				try {
					conn.close();
				} catch (SQLException e) {
					logger.error("Failure closing Connection",e);
					throw new MpiException("WorkItem read failed. "+e.getMessage());
				}
			}

		}
		
		return workItem;

	}		
	public static List<WorkItem> findByPerson(int personId) throws MpiException {

		logger.debug("Starting");

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
				int mid = rs.getInt("masterid");
				int type = rs.getInt("type");
				String desc = rs.getString("description");
				WorkItem item = new WorkItem(type, pid, mid, desc);
				item.setStatus(rs.getInt("status"));
				item.setId(rs.getInt("id"));
				item.setLastUpdated(rs.getTimestamp("lastUpdated"));
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
			if(conn!= null) {
				try {
					conn.close();
				} catch (SQLException e) {
					logger.error("Failure closing Connection",e);
					throw new MpiException("WorkItem read failed. "+e.getMessage());
				}
			}

		}
		
		return workItems;

	}	
	public static WorkItem findByPersonAndMaster(int personId, int masterId) throws MpiException {

		logger.debug("Starting");
		
		String findSQL = "select * from jtrace.workitem where personid = ? and masterid = ?";
		
		PreparedStatement preparedStatement = null;
		Connection conn = null;
		ResultSet rs = null;
		
		WorkItem workItem = null;
	
		try {

			conn = SimpleConnectionManager.getDBConnection();
			
			preparedStatement = conn.prepareStatement(findSQL);
			preparedStatement.setInt(1, personId);
			preparedStatement.setInt(2, masterId);

			rs = preparedStatement.executeQuery();
			
			if (rs.next()){
				int pid = rs.getInt("personid");
				int mid = rs.getInt("masterid");
				int type = rs.getInt("type");
				String desc = rs.getString("description");
				workItem = new WorkItem(type, pid, mid, desc);
				workItem.setStatus(rs.getInt("status"));
				workItem.setId(rs.getInt("id"));
				workItem.setLastUpdated(rs.getTimestamp("lastUpdated"));
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
			if(conn!= null) {
				try {
					conn.close();
				} catch (SQLException e) {
					logger.error("Failure closing Connection",e);
					throw new MpiException("WorkItem read failed. "+e.getMessage());
				}
			}

		}
		
		return workItem;

	}		
}
