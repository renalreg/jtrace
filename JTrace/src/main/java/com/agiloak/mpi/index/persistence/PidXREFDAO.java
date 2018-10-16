package com.agiloak.mpi.index.persistence;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.SimpleConnectionManager;
import com.agiloak.mpi.index.MasterRecord;
import com.agiloak.mpi.index.PidXREF;

/**
 * Maintains a link between the Person records and a single personId for a facility and extract.
 * 
 * 
 * @author Nick
 *
 */
public class PidXREFDAO extends NumberAllocatingDAO {
	
	final static Logger logger = LoggerFactory.getLogger(PidXREFDAO.class);
	
	public static PidXREF get(int id) throws MpiException {

		logger.debug("Starting");

		String getSQL = "select * from jtrace.pidxref where id = ? ";
		
		PreparedStatement preparedStatement = null;
		Connection conn = null;
		ResultSet rs = null;
		
		PidXREF pidxref = null;
		
		try {

			conn = SimpleConnectionManager.getDBConnection();
			
			preparedStatement = conn.prepareStatement(getSQL);
			preparedStatement.setInt(1, id);

			rs = preparedStatement.executeQuery();
			
			if (rs.next()){
				pidxref = buildRecord(rs);
			}
			
		} catch (Exception e) {
			logger.error("Failure getting PidXREF.",e);
			throw new MpiException("Failure getting PidXREF. "+e.getMessage());
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
					throw new MpiException("PidXREF read failed. "+e.getMessage());
				}
			}

		}
		
		return pidxref;

	}


	public static void create(PidXREF xref) throws MpiException {

		logger.debug("Starting");
		
		String pid = allocate();
		xref.setPid(pid);

		String insertSQL = "Insert into jtrace.pidxref "+
				"(pid, sendingFacility, sendingExtract, localId)"+
				" values (?,?,?,?)";
		
		PreparedStatement preparedStatement = null;
		Connection conn = null;
		
		try {

			conn = SimpleConnectionManager.getDBConnection();
			
			preparedStatement = conn.prepareStatement(insertSQL, Statement.RETURN_GENERATED_KEYS);
			preparedStatement.setString(1, xref.getPid());
			preparedStatement.setString(2, xref.getSendingFacility());
			preparedStatement.setString(3, xref.getSendingExtract());
			preparedStatement.setString(4, xref.getLocalId());

			int affectedRows = preparedStatement.executeUpdate();
			logger.debug("Affected Rows:"+affectedRows);
			
		    try (ResultSet generatedKeys = preparedStatement.getGeneratedKeys()) {
	            if (generatedKeys.next()) {
	            	xref.setId(generatedKeys.getInt(1));
	            	logger.debug("LINKID:"+xref.getId());
	            }
	            else {
	    			logger.error("Creating PidXREF failed, no ID obtained.");
	                throw new SQLException("Creating PidXREF failed, no ID obtained.");
	            }
		    }
		 
		} catch (SQLException e) {
			logger.error("Failure inserting PidXREF:",e);
			throw new MpiException("PidXREF insert failed");

		} finally {

			if (preparedStatement != null) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					logger.error("Failure closing Prepared Statement:",e);
					throw new MpiException("PidXREF insert failed");
				}
			}
			if(conn!= null) {
				try {
					conn.close();
				} catch (SQLException e) {
					logger.error("Failure closing Connection",e);
					throw new MpiException("PidXREF insert failed. "+e.getMessage());
				}
			}

		}

	}

	public static PidXREF findByLocalId(String sendingFacility, String sendingExtract, String localId) throws MpiException {
		
		logger.debug("Starting");

		String findSQL = "select * from jtrace.pidxref where sendingFacility = ? and sendingExtract = ? and localId = ? ";
		
		PreparedStatement preparedStatement = null;
		Connection conn = null;
		ResultSet rs = null;
		
		PidXREF xref = null;
		
		try {

			conn = SimpleConnectionManager.getDBConnection();
			
			preparedStatement = conn.prepareStatement(findSQL);
			preparedStatement.setString(1, sendingFacility);
			preparedStatement.setString(2, sendingExtract);
			preparedStatement.setString(3, localId);

			rs = preparedStatement.executeQuery();
			
			if (rs.next()){
				xref = buildRecord(rs);
			}
			
		} catch (Exception e) {
			logger.error("Failure querying PidXREF.",e);
			throw new MpiException("Failure querying PidXREF. "+e.getMessage());
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
					throw new MpiException("PidXREF read failed. "+e.getMessage());
				}
			}

		}
		
		return xref;

	}
	
	public static String allocate() throws MpiException {

		logger.debug("Starting");
		String patientId = allocateSequence("jtrace.patient_id");
		logger.debug("Complete");
		return patientId;
		
	}
	
	public static void deleteByLocalId(String sendingFacility, String sendingExtract, String localId) throws MpiException {
		
		logger.debug("Starting");

		String deleteSQL = "delete from jtrace.pidxref where sendingFacility = ? and sendingExtract = ? and localId = ?";
		
		PreparedStatement preparedStatement = null;
		Connection conn = null;
		
		try {

			conn = SimpleConnectionManager.getDBConnection();
			preparedStatement = conn.prepareStatement(deleteSQL);
			preparedStatement.setString(1, sendingFacility);
			preparedStatement.setString(2, sendingExtract);
			preparedStatement.setString(3, localId);
			
			int affectedRows = preparedStatement.executeUpdate();
			logger.debug("Affected Rows:"+affectedRows);
			
		} catch (Exception e) {
			logger.error("Failure deleting PIDXREF:",e);
			throw new MpiException("PIDXREF delete failed");
		} finally {

			if (preparedStatement != null) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					logger.error("Failure closing Prepared Statement:",e);
					throw new MpiException("PIDXREF delete failed");
				}
			}
			if(conn!= null) {
				try {
					conn.close();
				} catch (SQLException e) {
					logger.error("Failure closing Connection",e);
					throw new MpiException("PIDXREF delete failed. "+e.getMessage());
				}
			}

		}

	}
	
	private static PidXREF buildRecord(ResultSet rs) throws MpiException {

		logger.debug("Starting");

		PidXREF xref = null;
		try {

			xref = new PidXREF();
			xref.setId(rs.getInt("id"));
			xref.setPid(rs.getString("pid"));
			xref.setSendingFacility(rs.getString("sendingfacility"));
			xref.setSendingExtract(rs.getString("sendingextract"));
			xref.setLocalId(rs.getString("localid"));

		} catch (Exception e) {
			logger.error("Failure querying PidXREF.",e);
			throw new MpiException("Failure querying PidXREF. "+e.getMessage());
		} 
		
		return xref;

	}
	

}
