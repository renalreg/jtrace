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
import com.agiloak.mpi.index.MasterRecord;

public class MasterRecordDAO {
	
	private final static Logger logger = LoggerFactory.getLogger(MasterRecordDAO.class);
	
	public static MasterRecord findMasterRecordByNationalId(String nationalId) {

		String findSQL = "select * from jtrace.masterrecord where nationalid = ? ";
		
		PreparedStatement preparedStatement = null;
		Connection conn = null;
		ResultSet rs = null;
		
		MasterRecord master = null;
		
		try {

			conn = SimpleConnectionManager.getDBConnection();
			
			preparedStatement = conn.prepareStatement(findSQL);
			preparedStatement.setString(1, nationalId);

			rs = preparedStatement.executeQuery();
			
			if (rs.next()){
				master = new MasterRecord();
				master.setId(rs.getInt("id"));
				master.setNationalId(rs.getString("nationalid"));
				master.setSurname(rs.getString("surname"));
				master.setGivenName(rs.getString("givenname"));
				master.setGender(rs.getString("gender"));
				master.setDateOfBirth(rs.getTimestamp("dateofbirth"));
				master.setLastUpdated(rs.getTimestamp("lastupdated"));
			}
			
		} catch (SQLException e) {
			logger.error("Failure querying MasterRecord.",e);
		} finally {

			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					logger.error("Failure closing resultset.",e);
				}
			}
			
			if (preparedStatement != null) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					logger.error("Failure closing prepared statement.",e);
				}
			}

		}
		
		return master;

	}


	public static void insert(MasterRecord master) throws MpiException {
		
		String insertSQL = "Insert into jtrace.masterrecord "+
				"(dateofbirth, gender, givenname, surname, lastupdated, nationalid)"+
				" values (?,?,?,?,?,?)";
		
		PreparedStatement preparedStatement = null;
		Connection conn = null;
		
		try {

			conn = SimpleConnectionManager.getDBConnection();
			
			preparedStatement = conn.prepareStatement(insertSQL, Statement.RETURN_GENERATED_KEYS);
			preparedStatement.setTimestamp(1, new Timestamp(master.getDateOfBirth().getTime()));
			preparedStatement.setString(2, master.getGender());
			preparedStatement.setString(3, master.getGivenName());
			preparedStatement.setString(4, master.getSurname());
			preparedStatement.setTimestamp(5,new Timestamp(master.getLastUpdated().getTime()));
			preparedStatement.setString(6, master.getNationalId());

			int affectedRows = preparedStatement.executeUpdate();
			logger.debug("Affected Rows:"+affectedRows);
			
		    try (ResultSet generatedKeys = preparedStatement.getGeneratedKeys()) {
	            if (generatedKeys.next()) {
	            	master.setId(generatedKeys.getInt(1));
	            	logger.debug("MASTERID:"+master.getId());
	            }
	            else {
	    			logger.error("Creating MasterRecord failed, no ID obtained.");
	                throw new SQLException("Creating MasterRecord failed, no ID obtained.");
	            }
		    }
		 
		} catch (SQLException e) {
			logger.error("Failure inserting MasterRecord:",e);
			throw new MpiException("MasterRecord insert failed");

		} finally {

			if (preparedStatement != null) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					logger.error("Failure closing Prepared Statement:",e);
					throw new MpiException("MasterRecord insert failed");
				}
			}

		}

	}
	public static void DeleteByNationalId(String nationalId) throws MpiException {
		
		String deleteSQL = "delete from jtrace.masterrecord where nationalid = ?";
		
		PreparedStatement preparedStatement = null;
		Connection conn = null;
		
		try {

			conn = SimpleConnectionManager.getDBConnection();
			
			preparedStatement = conn.prepareStatement(deleteSQL);
			conn = SimpleConnectionManager.getDBConnection();
			preparedStatement.setString(1, nationalId);
			int affectedRows = preparedStatement.executeUpdate();
			logger.debug("Affected Rows:"+affectedRows);
			
		} catch (SQLException e) {
			logger.error("Failure deleting MasterRecord:",e);
			throw new MpiException("MasterRecord delete failed");
		} finally {

			if (preparedStatement != null) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					logger.error("Failure closing Prepared Statement:",e);
					throw new MpiException("MasterRecord delete failed");
				}
			}

		}

	}
	
	private static java.sql.Date getSqlDate(java.util.Date date){
		if (date==null) return null;
	    java.sql.Date sqlDate = new java.sql.Date(date.getTime());
		return sqlDate;
		
	}

	
}
