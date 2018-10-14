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

	public static void create(PidXREF xref) throws MpiException {

		logger.debug("Starting");

		String insertSQL = "Insert into jtrace.pidxref "+
				"(pid, sendingFacility, sendingExtract, localPatientId)"+
				" values (?,?,?,?)";
		
		PreparedStatement preparedStatement = null;
		Connection conn = null;
		
		try {

			conn = SimpleConnectionManager.getDBConnection();
			
			preparedStatement = conn.prepareStatement(insertSQL, Statement.RETURN_GENERATED_KEYS);
			preparedStatement.setString(1, xref.getPid());
			preparedStatement.setString(2, xref.getSendingFacility());
			preparedStatement.setString(3, xref.getSendingExtract());
			preparedStatement.setString(4, xref.getLocalPatientId());

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
	
	public static String allocate() throws MpiException {

		logger.debug("Starting");
		String patientId = allocateSequence("jtrace.patient_id");
		logger.debug("Complete");
		return patientId;
		
	}
	

}
