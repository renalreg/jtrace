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
import com.agiloak.mpi.index.MasterRecord;
import com.agiloak.mpi.index.Person;

public class MasterRecordDAO {
	
	private final static Logger logger = LoggerFactory.getLogger(MasterRecordDAO.class);
	
	public static MasterRecord get(int id) throws MpiException {

		String getSQL = "select * from jtrace.masterrecord where id = ? ";
		
		PreparedStatement preparedStatement = null;
		Connection conn = null;
		ResultSet rs = null;
		
		MasterRecord master = null;
		
		try {

			conn = SimpleConnectionManager.getDBConnection();
			
			preparedStatement = conn.prepareStatement(getSQL);
			preparedStatement.setInt(1, id);

			rs = preparedStatement.executeQuery();
			
			if (rs.next()){
				master = new MasterRecord();
				master.setId(rs.getInt("id"));
				master.setNationalId(rs.getString("nationalid"));
				master.setNationalIdType(rs.getString("nationalidtype"));
				master.setSurname(rs.getString("surname"));
				master.setGivenName(rs.getString("givenname"));
				master.setGender(rs.getString("gender"));
				master.setDateOfBirth(rs.getDate("dateofbirth"));
				master.setLastUpdated(rs.getTimestamp("lastupdated"));
			}
			
		} catch (Exception e) {
			logger.error("Failure getting MasterRecord.",e);
			throw new MpiException("Failure getting MasterRecord. "+e.getMessage());
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
		
		return master;

	}

	public static MasterRecord findByNationalId(String nationalId, String nationalIdType) throws MpiException {

		String findSQL = "select * from jtrace.masterrecord where nationalid = ? and nationalidtype = ? ";
		
		PreparedStatement preparedStatement = null;
		Connection conn = null;
		ResultSet rs = null;
		
		MasterRecord master = null;
		
		try {

			conn = SimpleConnectionManager.getDBConnection();
			
			preparedStatement = conn.prepareStatement(findSQL);
			preparedStatement.setString(1, nationalId);
			preparedStatement.setString(2, nationalIdType);

			rs = preparedStatement.executeQuery();
			
			if (rs.next()){
				master = new MasterRecord();
				master.setId(rs.getInt("id"));
				master.setNationalId(rs.getString("nationalid"));
				master.setNationalIdType(rs.getString("nationalidtype"));
				master.setSurname(rs.getString("surname"));
				master.setGivenName(rs.getString("givenname"));
				master.setGender(rs.getString("gender"));
				master.setDateOfBirth(rs.getDate("dateofbirth"));
				master.setLastUpdated(rs.getTimestamp("lastupdated"));
			}
			
		} catch (Exception e) {
			logger.error("Failure querying MasterRecord.",e);
			throw new MpiException("Failure querying MasterRecord. "+e.getMessage());
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
		
		return master;

	}

	/*
	 * Specific implementation for UKRDC, using GN, SN, DOB
	 */
	public static List<MasterRecord> findByDemographics(Person person) throws MpiException {

		String findSQL = "select * from jtrace.masterrecord where givenname = ? and surname= ? and dateofbirth = ? ";
		
		PreparedStatement preparedStatement = null;
		Connection conn = null;
		ResultSet rs = null;
		
		List<MasterRecord> masterRecords = new ArrayList<MasterRecord>();
		
		try {

			conn = SimpleConnectionManager.getDBConnection();
			
			preparedStatement = conn.prepareStatement(findSQL);
			preparedStatement.setString(1, person.getGivenName());
			preparedStatement.setString(2, person.getSurname());
			preparedStatement.setDate(3, new java.sql.Date(person.getDateOfBirth().getTime()) );

			rs = preparedStatement.executeQuery();
			
			while (rs.next()){
				MasterRecord master = new MasterRecord();
				master.setId(rs.getInt("id"));
				master.setNationalId(rs.getString("nationalid"));
				master.setNationalIdType(rs.getString("nationalidtype"));
				master.setSurname(rs.getString("surname"));
				master.setGivenName(rs.getString("givenname"));
				master.setGender(rs.getString("gender"));
				master.setDateOfBirth(rs.getDate("dateofbirth"));
				master.setLastUpdated(rs.getTimestamp("lastupdated"));
				
				masterRecords.add(master);
			}
			
		} catch (Exception e) {
			logger.error("Failure querying MasterRecord.",e);
			throw new MpiException("Failure querying MasterRecord. "+e.getMessage());
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
		
		return masterRecords;

	}

	public static void create(MasterRecord master) throws MpiException {
		
		String insertSQL = "Insert into jtrace.masterrecord "+
				"(dateofbirth, gender, givenname, surname, lastupdated, nationalid, nationalidtype)"+
				" values (?,?,?,?,?,?,?)";
		
		PreparedStatement preparedStatement = null;
		Connection conn = null;
		
		try {

			conn = SimpleConnectionManager.getDBConnection();
			
			preparedStatement = conn.prepareStatement(insertSQL, Statement.RETURN_GENERATED_KEYS);
			preparedStatement.setDate(1, new java.sql.Date(master.getDateOfBirth().getTime()));
			preparedStatement.setString(2, master.getGender());
			preparedStatement.setString(3, master.getGivenName());
			preparedStatement.setString(4, master.getSurname());
			preparedStatement.setTimestamp(5,new Timestamp(master.getLastUpdated().getTime()));
			preparedStatement.setString(6, master.getNationalId());
			preparedStatement.setString(7, master.getNationalIdType());

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
		 
		} catch (Exception e) {
			logger.error("Failure inserting MasterRecord:",e);
			throw new MpiException("MasterRecord insert failed. "+e.getMessage());

		} finally {

			if (preparedStatement != null) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					logger.error("Failure closing Prepared Statement:",e);
					throw new MpiException("MasterRecord insert failed. "+e.getMessage());
				}
			}

		}

	}

	public static void update(MasterRecord master) throws MpiException {
		
		String updateSQL = "Update jtrace.masterrecord "+
				"set dateofbirth=?, gender=?, givenname=?, surname=?, lastupdated=? where id =? ";
		
		PreparedStatement preparedStatement = null;
		Connection conn = null;
		
		try {

			conn = SimpleConnectionManager.getDBConnection();
			
			preparedStatement = conn.prepareStatement(updateSQL);
			preparedStatement.setDate(1, new java.sql.Date(master.getDateOfBirth().getTime()));
			preparedStatement.setString(2, master.getGender());
			preparedStatement.setString(3, master.getGivenName());
			preparedStatement.setString(4, master.getSurname());
			preparedStatement.setTimestamp(5,new Timestamp(master.getLastUpdated().getTime()));
			preparedStatement.setInt(6, master.getId());

			int affectedRows = preparedStatement.executeUpdate();
			logger.debug("Affected Rows:"+affectedRows);
					 
		} catch (Exception e) {
			logger.error("Failure update MasterRecord:",e);
			throw new MpiException("MasterRecord update failed. "+e.getMessage());

		} finally {

			if (preparedStatement != null) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					logger.error("Failure closing Prepared Statement:",e);
					throw new MpiException("MasterRecord update failed. "+e.getMessage());
				}
			}

		}

	}

	public static void deleteByNationalId(String nationalId, String nationalIdType) throws MpiException {
		
		String deleteSQL = "delete from jtrace.masterrecord where nationalid = ? and nationalidtype = ? ";
		
		PreparedStatement preparedStatement = null;
		Connection conn = null;
		
		try {

			conn = SimpleConnectionManager.getDBConnection();
			
			preparedStatement = conn.prepareStatement(deleteSQL);
			conn = SimpleConnectionManager.getDBConnection();
			preparedStatement.setString(1, nationalId);
			preparedStatement.setString(2, nationalIdType);
			int affectedRows = preparedStatement.executeUpdate();
			logger.debug("Affected Rows:"+affectedRows);
			
		} catch (Exception e) {
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
	
}
