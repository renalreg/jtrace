package com.agiloak.mpi.index.persistence;

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
		
		String deleteSQL = "delete jtrace.linkrecord where masterid = ? and personid = ?";
		
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
}
