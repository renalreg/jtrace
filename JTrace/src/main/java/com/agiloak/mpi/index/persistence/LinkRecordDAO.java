package com.agiloak.mpi.index.persistence;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.agiloak.mpi.MpiException;
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

	public static void create(Connection conn, LinkRecord link) throws MpiException {

		logger.debug("Starting");

		String insertSQL = "Insert into linkrecord "+
				"(masterid, personid, linktype, linkcode, linkdesc, updatedby)"+
				" values (?,?,?,?,?,?)";
		
		PreparedStatement preparedStatement = null;
		
		try {

			preparedStatement = conn.prepareStatement(insertSQL, Statement.RETURN_GENERATED_KEYS);
			preparedStatement.setInt(1, link.getMasterId());
			preparedStatement.setInt(2, link.getPersonId());
			preparedStatement.setInt(3, link.getLinkType());
			preparedStatement.setInt(4, link.getLinkCode());
			preparedStatement.setString(5, link.getLinkDesc());
			preparedStatement.setString(6, link.getUpdatedBy());

			int affectedRows = preparedStatement.executeUpdate();
			logger.debug("Affected Rows:"+affectedRows);
			
		    try (ResultSet generatedKeys = preparedStatement.getGeneratedKeys()) {
	            if (generatedKeys.next()) {
	            	link.setId(generatedKeys.getInt("id"));
	            	link.setCreationDate(generatedKeys.getTimestamp("creationdate"));
	            	link.setLastUpdated(generatedKeys.getTimestamp("lastupdated"));
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
	
	public static void delete(Connection conn, LinkRecord link) throws MpiException {

		logger.debug("Starting");

		String deleteSQL = "delete from linkrecord where masterid = ? and personid = ?";
		
		PreparedStatement preparedStatement = null;
		
		try {

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
	
	public static void deleteByPerson(Connection conn, int personId) throws MpiException {
		
		logger.debug("Starting");

		String deleteSQL = "delete from linkrecord where personid = ?";
		
		PreparedStatement preparedStatement = null;
		
		try {

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
	public static LinkRecord find(Connection conn, int masterId, int personId) throws MpiException {

		logger.debug("Starting");

		String findSQL = "select * from linkrecord where masterid = ? and personid = ? ";
		
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		
		LinkRecord link = null;
		
		try {

			preparedStatement = conn.prepareStatement(findSQL);
			preparedStatement.setInt(1, masterId);
			preparedStatement.setInt(2, personId);

			rs = preparedStatement.executeQuery();
			
			if (rs.next()){
				link = buildLinkRecord(rs);
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
	
	public static LinkRecord findByPersonAndType(Connection conn, int personId, String type) throws MpiException {

		logger.debug("Starting");

		String findSQL = "select * from linkrecord lr, masterrecord mr where "+
				         "lr.masterid = mr.id and lr.personid = ? and mr.nationalidtype = ? ";
		
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		
		LinkRecord link =null;
		
		try {

			preparedStatement = conn.prepareStatement(findSQL);
			preparedStatement.setInt(1, personId);
			preparedStatement.setString(2, type);

			rs = preparedStatement.executeQuery();
			
			if (rs.next()){
				link = buildLinkRecord(rs);
			}
			
		} catch (Exception e) {
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

	public static int countByMasterAndOriginator(Connection conn, int masterId, String originator) throws MpiException {

		logger.debug("Starting");

		String findSQL = "select count(*) as cnt from person p, linkrecord lr where "+
				         "p.id = lr.personid and lr.masterid = ? and p.originator = ? and p.skipduplicatecheck = false ";
		
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		int cnt = 0 ;
		
		try {

			preparedStatement = conn.prepareStatement(findSQL);
			preparedStatement.setInt(1, masterId);
			preparedStatement.setString(2, originator);

			rs = preparedStatement.executeQuery();
			
			if (rs.next()){
				cnt = rs.getInt("cnt");
			}
			
		} catch (Exception e) {
			
			logger.error("Failure querying countByMasterAndOriginator.",e);
			throw new MpiException("Failure querying countByMasterAndOriginator. "+e.getMessage());
			
		} finally {

			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					logger.error("Failure closing resultset countByMasterAndOriginator.",e);
					throw new MpiException("Failure closing resultset countByMasterAndOriginator. "+e.getMessage());
				}
			}
			
			if (preparedStatement != null) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					logger.error("Failure closing prepared statement countByMasterAndOriginator.",e);
					throw new MpiException("Failure closing prepared statement countByMasterAndOriginator. "+e.getMessage());
				}
			}

		}
		
		return cnt;

	}		

	public static int countByMasterAndOriginatorExcludingPid(Connection conn, int masterId, String originator, int personId) throws MpiException {

		logger.debug("Starting");

		String findSQL = "select count(*) as cnt from person p, linkrecord lr where "+
				         "p.id = lr.personid and lr.masterid = ? and p.originator = ? and p.skipduplicatecheck = false and p.id <> ? ";
		
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		int cnt = 0 ;
		
		try {

			preparedStatement = conn.prepareStatement(findSQL);
			preparedStatement.setInt(1, masterId);
			preparedStatement.setString(2, originator);
			preparedStatement.setInt(3, personId);

			rs = preparedStatement.executeQuery();
			
			if (rs.next()){
				cnt = rs.getInt("cnt");
			}
			
		} catch (Exception e) {
			
			logger.error("Failure querying countByMasterAndOriginatorExcludingPid.",e);
			throw new MpiException("Failure querying countByMasterAndOriginatorExcludingPid. "+e.getMessage());
			
		} finally {

			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					logger.error("Failure closing resultset countByMasterAndOriginatorExcludingPid.",e);
					throw new MpiException("Failure closing resultset countByMasterAndOriginatorExcludingPid. "+e.getMessage());
				}
			}
			
			if (preparedStatement != null) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					logger.error("Failure closing prepared statement countByMasterAndOriginatorExcludingPid.",e);
					throw new MpiException("Failure closing prepared statement countByMasterAndOriginatorExcludingPid. "+e.getMessage());
				}
			}
		}
		
		return cnt;

	}		

	public static List<LinkRecord> findByPerson(Connection conn, int personId) throws MpiException {

		logger.debug("Starting");

		String findSQL = "select * from linkrecord where personid = ? ";
		
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		
		List<LinkRecord> linkRecords = new ArrayList<LinkRecord>();
		
		try {

			preparedStatement = conn.prepareStatement(findSQL);
			preparedStatement.setInt(1, personId);

			rs = preparedStatement.executeQuery();
			
			while (rs.next()){
				LinkRecord link = buildLinkRecord(rs);
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
	
	public static List<LinkRecord> findByMaster(Connection conn, int masterId) throws MpiException {

		logger.debug("Starting");

		String findSQL = "select * from linkrecord where masterid = ? order by id desc";
		
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		
		List<LinkRecord> linkRecords = new ArrayList<LinkRecord>();
		
		try {

			preparedStatement = conn.prepareStatement(findSQL);
			preparedStatement.setInt(1, masterId);

			rs = preparedStatement.executeQuery();
			
			while (rs.next()){
				LinkRecord link = buildLinkRecord(rs);
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
	
	private static LinkRecord buildLinkRecord(ResultSet rs) throws MpiException {

		logger.debug("Starting");

		LinkRecord linkRecord = null;
		try {

				int pid = rs.getInt("personid");
				int mid = rs.getInt("masterid");
				linkRecord = new LinkRecord(mid,pid);
				linkRecord.setLastUpdated(rs.getTimestamp("lastupdated"));
				linkRecord.setCreationDate(rs.getTimestamp("creationdate"));
				linkRecord.setId(rs.getInt("id"));
				linkRecord.setLinkType(rs.getInt("linktype"));
				linkRecord.setLinkCode(rs.getInt("linkcode"));
				linkRecord.setUpdatedBy(rs.getString("linkdesc"));
				linkRecord.setUpdatedBy(rs.getString("updatedby"));

		} catch (Exception e) {
			logger.error("Failure querying LinkRecord.",e);
			throw new MpiException("Failure querying LinkRecord. "+e.getMessage());
		} 
		
		return linkRecord;

	}		
}
