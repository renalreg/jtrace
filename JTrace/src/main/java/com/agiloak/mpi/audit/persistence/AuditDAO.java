package com.agiloak.mpi.audit.persistence;

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
import com.agiloak.mpi.audit.Audit;

public class AuditDAO {
	
	private final static Logger logger = LoggerFactory.getLogger(AuditDAO.class);

	public static void create(Audit audit) throws MpiException {

		
		String insertSQL = "Insert into jtrace.audit "+
				"(personid, masterid, type, description, lastupdated, updatedby)"+ 
				" values (?,?,?,?,?,?)";
		
		PreparedStatement preparedStatement = null;
		Connection conn = null;
		
		try {

			conn = SimpleConnectionManager.getDBConnection();
			
			preparedStatement = conn.prepareStatement(insertSQL, Statement.RETURN_GENERATED_KEYS);
			preparedStatement.setInt(1, audit.getPersonId());
			preparedStatement.setInt(2, audit.getMasterId());
			preparedStatement.setInt(3, audit.getType());
			preparedStatement.setString(4, audit.getDescription());
			preparedStatement.setTimestamp(5,new Timestamp(audit.getLastUpdated().getTime()));
			preparedStatement.setString(6, audit.getUpdatedBy());

			int affectedRows = preparedStatement.executeUpdate();
			logger.debug("Affected Rows:"+affectedRows);
			
		    try (ResultSet generatedKeys = preparedStatement.getGeneratedKeys()) {
	            if (generatedKeys.next()) {
	            	logger.debug("Audit ID:"+generatedKeys.getInt(1));
	            	audit.setId(generatedKeys.getInt(1));
	            }
	            else {
	    			logger.error("Creating Audit failed, no ID obtained.");
	                throw new SQLException("Creating Audit failed, no ID obtained.");
	            }
		    }
		 
		} catch (SQLException e) {
			logger.error("Failure inserting audit:",e);
			throw new MpiException("Audit insert failed");

		} finally {

			if (preparedStatement != null) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					logger.error("Failure closing Prepared Statement:",e);
					throw new MpiException("Audit insert failed");
				}
			}

		}

	}
	public static void deleteByPerson(int personId) throws MpiException {
		
		String deleteSQL = "delete from jtrace.audit where personid = ?";
		
		PreparedStatement preparedStatement = null;
		Connection conn = null;
		
		try {

			conn = SimpleConnectionManager.getDBConnection();
			
			preparedStatement = conn.prepareStatement(deleteSQL);
			preparedStatement.setInt(1, personId);

			int affectedRows = preparedStatement.executeUpdate();
			logger.debug("Affected Rows:"+affectedRows);
					 
		} catch (SQLException e) {
			logger.error("Failure deleting Audit:",e);
			throw new MpiException("Audit delete failed");

		} finally {

			if (preparedStatement != null) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					logger.error("Failure closing Prepared Statement:",e);
					throw new MpiException("Audit delete failed");
				}
			}

		}

	}	
	
	public static List<Audit> findByPerson(int personId) throws MpiException {

		String findSQL = "select * from jtrace.audit where personid = ? ";
		
		PreparedStatement preparedStatement = null;
		Connection conn = null;
		ResultSet rs = null;
		
		List<Audit> audits = new ArrayList<Audit>();
		
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
				Audit audit = new Audit(type, pid, mid, desc);
				audit.setId(rs.getInt("id"));
				audit.setLastUpdated(rs.getTimestamp("lastUpdated"));
				audit.setUpdatedBy(rs.getString("updatedby"));
				
				audits.add(audit);
			}
			
		} catch (Exception e) {
			logger.error("Failure querying Audit.",e);
			throw new MpiException("Failure querying Audit. "+e.getMessage());
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
		
		return audits;

	}	
	
}
