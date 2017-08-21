package com.agiloak.mpi.index.persistence;

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
import com.agiloak.mpi.index.LinkRecord;

/**
 * Maintains a link between the MasterRecord and the Person
 * 
 * The need is to be able to:
 * 1) Add a link when the records are matched
 * 2) Remove the link when the match is invalid
 * 
 * @author Nick
 *
 */
public class LinkRecordDAO {
	
	private final static Logger logger = LoggerFactory.getLogger(LinkRecordDAO.class);

	public static void create(LinkRecord link) throws MpiException {
		
		String insertSQL = "Insert into jtrace.linkrecord "+
				"(masterid, personid, lastupdated)"+
				" values (?,?,?)";
		
		PreparedStatement preparedStatement = null;
		Connection conn = null;
		
		try {

			conn = SimpleConnectionManager.getDBConnection();
			
			preparedStatement = conn.prepareStatement(insertSQL, Statement.RETURN_GENERATED_KEYS);
			preparedStatement.setInt(1, link.getMasterId());
			preparedStatement.setInt(2, link.getPersonId());
			preparedStatement.setTimestamp(3,new Timestamp(link.getLastUpdated().getTime()));

			int affectedRows = preparedStatement.executeUpdate();
			logger.debug("Affected Rows:"+affectedRows);
			
		    try (ResultSet generatedKeys = preparedStatement.getGeneratedKeys()) {
	            if (generatedKeys.next()) {
	            	link.setId(generatedKeys.getInt(1));
	            	logger.debug("LINKID:"+link.getId());
	            }
	            else {
	    			logger.error("Creating LinkRecord failed, no ID obtained.");
	                throw new SQLException("Creating LinkRecord failed, no ID obtained.");
	            }
		    }
		 
		} catch (SQLException e) {
			logger.error("Failure inserting LinkRecord:",e);
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
	
	public static void delete(LinkRecord link) throws MpiException {
		
		String deleteSQL = "delete from jtrace.linkrecord where masterid = ? and personid = ?";
		
		PreparedStatement preparedStatement = null;
		Connection conn = null;
		
		try {

			conn = SimpleConnectionManager.getDBConnection();
			
			preparedStatement = conn.prepareStatement(deleteSQL);
			preparedStatement.setInt(1, link.getMasterId());
			preparedStatement.setInt(2, link.getPersonId());

			int affectedRows = preparedStatement.executeUpdate();
			logger.debug("Affected Rows:"+affectedRows);
					 
		} catch (SQLException e) {
			logger.error("Failure deleting LinkRecord:",e);
			throw new MpiException("LinkRecord delete failed");

		} finally {

			if (preparedStatement != null) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					logger.error("Failure closing Prepared Statement:",e);
					throw new MpiException("LinkRecord delete failed");
				}
			}

		}

	}	
	
	public static void deleteByPerson(int personId) throws MpiException {
		
		String deleteSQL = "delete from jtrace.linkrecord where personid = ?";
		
		PreparedStatement preparedStatement = null;
		Connection conn = null;
		
		try {

			conn = SimpleConnectionManager.getDBConnection();
			
			preparedStatement = conn.prepareStatement(deleteSQL);
			preparedStatement.setInt(1, personId);

			int affectedRows = preparedStatement.executeUpdate();
			logger.debug("Affected Rows:"+affectedRows);
					 
		} catch (SQLException e) {
			logger.error("Failure deleting LinkRecord:",e);
			throw new MpiException("LinkRecord delete failed");

		} finally {

			if (preparedStatement != null) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					logger.error("Failure closing Prepared Statement:",e);
					throw new MpiException("LinkRecord delete failed");
				}
			}

		}

	}		
	/**
	 * Only required for testing at the moment
	 * @param masterId
	 * @param personId
	 * @return
	 * @throws MpiException
	 */
	public static LinkRecord find(int masterId, int personId) throws MpiException {

		String findSQL = "select * from jtrace.linkrecord where masterid = ? and personid = ? ";
		
		PreparedStatement preparedStatement = null;
		Connection conn = null;
		ResultSet rs = null;
		
		LinkRecord link = null;
		
		try {

			conn = SimpleConnectionManager.getDBConnection();
			
			preparedStatement = conn.prepareStatement(findSQL);
			preparedStatement.setInt(1, masterId);
			preparedStatement.setInt(2, personId);

			rs = preparedStatement.executeQuery();
			
			if (rs.next()){
				int mid = rs.getInt("masterid");
				int pid = rs.getInt("personid");
				link = new LinkRecord(mid,pid);
				link.setId(rs.getInt("id"));
			}
			
		} catch (SQLException e) {
			logger.error("Failure querying LinkRecord.",e);
			throw new MpiException("Failure querying LinkRecord. "+e.getMessage());
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
		
		return link;

	}
	
	public static LinkRecord findByPersonAndType(int personId, String type) throws MpiException {

		String findSQL = "select * from jtrace.linkrecord lr, jtrace.masterrecord mr where "+
				         "lr.masterid = mr.id and lr.personid = ? and mr.nationalidtype = ? ";
		
		PreparedStatement preparedStatement = null;
		Connection conn = null;
		ResultSet rs = null;
		
		LinkRecord link =null;
		
		try {

			conn = SimpleConnectionManager.getDBConnection();
			
			preparedStatement = conn.prepareStatement(findSQL);
			preparedStatement.setInt(1, personId);
			preparedStatement.setString(2, type);

			rs = preparedStatement.executeQuery();
			
			if (rs.next()){
				int pid = rs.getInt("personid");
				int mid = rs.getInt("masterid");
				link = new LinkRecord(mid,pid);
				link.setLastUpdated(rs.getTimestamp("lastUpdated"));
				link.setId(rs.getInt("id"));
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
		
		return link;

	}		

	public static List<LinkRecord> findByPerson(int personId) throws MpiException {

		String findSQL = "select * from jtrace.linkrecord where personid = ? ";
		
		PreparedStatement preparedStatement = null;
		Connection conn = null;
		ResultSet rs = null;
		
		List<LinkRecord> linkRecords = new ArrayList<LinkRecord>();
		
		try {

			conn = SimpleConnectionManager.getDBConnection();
			
			preparedStatement = conn.prepareStatement(findSQL);
			preparedStatement.setInt(1, personId);

			rs = preparedStatement.executeQuery();
			
			while (rs.next()){
				int pid = rs.getInt("personid");
				int mid = rs.getInt("masterid");
				LinkRecord link = new LinkRecord(mid,pid);
				link.setLastUpdated(rs.getTimestamp("lastUpdated"));
				link.setId(rs.getInt("id"));
				
				linkRecords.add(link);
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
		
		return linkRecords;

	}		
	
	public static List<LinkRecord> findByMaster(int masterId) throws MpiException {

		String findSQL = "select * from jtrace.linkrecord where masterid = ? ";
		
		PreparedStatement preparedStatement = null;
		Connection conn = null;
		ResultSet rs = null;
		
		List<LinkRecord> linkRecords = new ArrayList<LinkRecord>();
		
		try {

			conn = SimpleConnectionManager.getDBConnection();
			
			preparedStatement = conn.prepareStatement(findSQL);
			preparedStatement.setInt(1, masterId);

			rs = preparedStatement.executeQuery();
			
			while (rs.next()){
				int pid = rs.getInt("personid");
				int mid = rs.getInt("masterid");
				LinkRecord link = new LinkRecord(mid,pid);
				link.setLastUpdated(rs.getTimestamp("lastUpdated"));
				link.setId(rs.getInt("id"));
				
				linkRecords.add(link);
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
		
		return linkRecords;

	}		
}
