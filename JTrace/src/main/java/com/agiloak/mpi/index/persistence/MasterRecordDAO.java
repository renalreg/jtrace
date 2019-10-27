package com.agiloak.mpi.index.persistence;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.index.MasterRecord;
import com.agiloak.mpi.index.Person;

public class MasterRecordDAO {
	
	private final static Logger logger = LoggerFactory.getLogger(MasterRecordDAO.class);
	
	public static MasterRecord get(Connection conn, int id) throws MpiException {

		logger.debug("Starting");

		String getSQL = "select * from masterrecord where id = ? ";
		
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		
		MasterRecord master = null;
		
		try {

			preparedStatement = conn.prepareStatement(getSQL);
			preparedStatement.setInt(1, id);

			rs = preparedStatement.executeQuery();
			
			if (rs.next()){
				master = buildMasterRecord(rs);
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

	public static MasterRecord findByNationalId(Connection conn, String nationalId, String nationalIdType) throws MpiException {

		logger.debug("Starting");

		String findSQL = "select * from masterrecord where nationalid = ? and nationalidtype = ? ";
		
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		
		MasterRecord master = null;
		
		try {

			preparedStatement = conn.prepareStatement(findSQL);
			preparedStatement.setString(1, nationalId);
			preparedStatement.setString(2, nationalIdType);

			rs = preparedStatement.executeQuery();
			
			if (rs.next()){
				master = buildMasterRecord(rs);
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
	public static List<MasterRecord> findByDemographics(Connection conn, Person person) throws MpiException {

		logger.debug("Starting");

		String findSQL = "select * from masterrecord where givenname = ? and surname= ? and dateofbirth = ? ";
		
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		
		List<MasterRecord> masterRecords = new ArrayList<MasterRecord>();
		
		try {

			preparedStatement = conn.prepareStatement(findSQL);
			preparedStatement.setString(1, person.getGivenName());
			preparedStatement.setString(2, person.getSurname());
			preparedStatement.setDate(3, new java.sql.Date(person.getDateOfBirth().getTime()) );

			rs = preparedStatement.executeQuery();
			
			while (rs.next()){
				MasterRecord master = buildMasterRecord(rs);
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

	public static void create(Connection conn, MasterRecord master) throws MpiException {

		logger.debug("Starting");

		String insertSQL = "Insert into masterrecord "+
				"(dateofbirth, gender, givenname, surname, nationalid, nationalidtype, status, effectivedate)"+
				" values (?,?,?,?,?,?,?,?)";
		
		PreparedStatement preparedStatement = null;
		
		try {

			preparedStatement = conn.prepareStatement(insertSQL, Statement.RETURN_GENERATED_KEYS);
			populateStatement(preparedStatement, master);

			int affectedRows = preparedStatement.executeUpdate();
			logger.debug("Affected Rows:"+affectedRows);
			
		    try (ResultSet generatedKeys = preparedStatement.getGeneratedKeys()) {
	            if (generatedKeys.next()) {
	            	master.setId(generatedKeys.getInt("id"));
	            	master.setCreationDate(generatedKeys.getTimestamp("creationdate"));
	            	master.setLastUpdated(generatedKeys.getTimestamp("lastupdated"));
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

	public static void update(Connection conn, MasterRecord master) throws MpiException {

		logger.debug("Starting");

		master.setLastUpdated(new Timestamp(System.currentTimeMillis()));
		String updateSQL = "Update masterrecord "+
				"set dateofbirth=?, gender=?, givenname=?, surname=?, "+
				   " nationalid=?, nationalidtype=?, status=?, effectivedate=?, lastupdated=? "+
				   " where id =? ";
		
		PreparedStatement preparedStatement = null;
		
		try {

			preparedStatement = conn.prepareStatement(updateSQL);
			populateStatement(preparedStatement, master);
			preparedStatement.setTimestamp(9, master.getLastUpdated());
			preparedStatement.setInt(10, master.getId());

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

	public static void delete(Connection conn, MasterRecord master) throws MpiException {
		delete(conn, master.getId());
	}
	
	public static void delete(Connection conn, int masterId) throws MpiException {

		logger.debug("Starting");

		String deleteSQL = "delete from masterrecord where id = ? ";
		
		PreparedStatement preparedStatement = null;
		
		try {

			preparedStatement = conn.prepareStatement(deleteSQL);
			preparedStatement.setInt(1, masterId);
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
	public static void deleteByNationalId(Connection conn, String nationalId, String nationalIdType) throws MpiException {
		
		logger.debug("Starting");

		String deleteSQL = "delete from masterrecord where nationalid = ? and nationalidtype = ? ";
		
		PreparedStatement preparedStatement = null;
		
		try {

			preparedStatement = conn.prepareStatement(deleteSQL);
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
	
	public static String allocate(Connection conn) throws MpiException {

		logger.debug("Starting");

		String nextValSQL = "select nextval('ukrdc_id')";
		
		String  ukrdcId = "";

		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		
		try {

			preparedStatement = conn.prepareStatement(nextValSQL);

			rs = preparedStatement.executeQuery();
			
			if (rs.next()){
				ukrdcId = new Integer(rs.getInt("nextval")).toString();
			}
			
		} catch (Exception e) {
			logger.error("Failure querying UKRDC Sequence.",e);
			throw new MpiException("Failure querying UKRDC Sequence. "+e.getMessage());
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
		return ukrdcId;
		
	}
	
	private static MasterRecord buildMasterRecord(ResultSet rs) throws MpiException {

		logger.debug("Starting");

		MasterRecord master = null;
		try {

			master = new MasterRecord();
			master.setId(rs.getInt("id"));
			master.setNationalId(rs.getString("nationalid"));
			master.setNationalIdType(rs.getString("nationalidtype"));
			master.setSurname(rs.getString("surname"));
			master.setGivenName(rs.getString("givenname"));
			master.setGender(rs.getString("gender"));
			master.setDateOfBirth(rs.getDate("dateofbirth"));
			master.setLastUpdated(rs.getTimestamp("lastupdated"));
			master.setCreationDate(rs.getTimestamp("creationdate"));
			master.setStatus(rs.getInt("status"));
			master.setEffectiveDate(rs.getTimestamp("effectivedate"));

		} catch (Exception e) {
			logger.error("Failure querying MasterRecord.",e);
			throw new MpiException("Failure querying MasterRecord. "+e.getMessage());
		} 
		
		return master;

	}
	
	private static void populateStatement(PreparedStatement ps, MasterRecord master) throws SQLException {
		
		logger.debug("Starting");

		ps.setDate(1, new java.sql.Date(master.getDateOfBirth().getTime()));
		ps.setString(2, master.getGender());
		ps.setString(3, master.getGivenName());
		ps.setString(4, master.getSurname());
		ps.setString(5, master.getNationalId());
		ps.setString(6, master.getNationalIdType());
		ps.setInt(7, master.getStatus());
		ps.setTimestamp(8,new Timestamp(master.getEffectiveDate().getTime()));
	}
}
