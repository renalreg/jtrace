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
import com.agiloak.mpi.index.Person;

public class PersonDAO {
	
	private final static Logger logger = LoggerFactory.getLogger(PersonDAO.class);

	public static Person findByLocalId(Connection conn, String localIdType, String localId, String originator) throws MpiException {

		logger.debug("Starting");

		String findSQL = "select * from person where localidtype = ? and localid = ? and originator = ? ";
		
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		
		Person person = null;
		
		try {

			preparedStatement = conn.prepareStatement(findSQL);
			preparedStatement.setString(1, localIdType);
			preparedStatement.setString(2, localId);
			preparedStatement.setString(3, originator);

			rs = preparedStatement.executeQuery();
			
			if (rs.next()){
				person = new Person();
				person.setId(rs.getInt("id"));
				person.setOriginator(rs.getString("originator"));
				person.setLocalId(rs.getString("localid"));
				person.setLocalIdType(rs.getString("localidtype"));

				person.setPrimaryId(rs.getString("nationalid"));
				person.setPrimaryIdType(rs.getString("nationalidtype"));

				person.setDateOfBirth(rs.getTimestamp("dateofbirth"));
				person.setGender(rs.getString("gender"));
				person.setDateOfDeath(rs.getTimestamp("dateofdeath"));

				person.setGivenName(rs.getString("givenname"));
				person.setOtherGivenNames(rs.getString("othergivennames"));
				person.setSurname(rs.getString("surname"));
				person.setPrevSurname(rs.getString("prevsurname"));
				person.setTitle(rs.getString("title"));

				person.setPostcode(rs.getString("postcode"));
				person.setStreet(rs.getString("street"));

				person.setStdSurname(rs.getString("stdsurname"));
				person.setStdGivenName(rs.getString("stdgivenname"));
				person.setStdPrevSurname(rs.getString("stdprevsurname"));
				person.setStdPostcode(rs.getString("stdpostcode"));
			
				person.setSkipDuplicateCheck(rs.getBoolean("skipduplicatecheck"));

				person.setLastUpdated(rs.getTimestamp("lastupdated"));
				person.setCreationDate(rs.getTimestamp("creationdate"));

			}
			
		} catch (Exception e) {
			logger.error("Failure querying Person.",e);
			throw new MpiException("Person read failed. "+e.getMessage());
		} finally {

			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					logger.error("Failure closing resultset.",e);
					throw new MpiException("Person read failed. "+e.getMessage());
				}
			}
			
			if (preparedStatement != null) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					logger.error("Failure closing prepared statement.",e);
					throw new MpiException("Person read failed. "+e.getMessage());
				}
			}
		}
		
		return person;

	}

	public static List<Person> findByMasterId(Connection conn, int masterId) throws MpiException {

		logger.debug("Starting");

		String findSQL = "select * from person p, linkrecord lr where lr.masterid = ? and p.id = lr.personid ";
		
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		
		List<Person> personList = new ArrayList<Person>();
		
		try {

			preparedStatement = conn.prepareStatement(findSQL);
			preparedStatement.setInt(1, masterId);

			rs = preparedStatement.executeQuery();
			
			while  (rs.next()){
				Person person = new Person();
				person.setId(rs.getInt("id"));
				person.setOriginator(rs.getString("originator"));
				person.setLocalId(rs.getString("localid"));
				person.setLocalIdType(rs.getString("localidtype"));

				person.setPrimaryId(rs.getString("nationalid"));
				person.setPrimaryIdType(rs.getString("nationalidtype"));

				person.setDateOfBirth(rs.getTimestamp("dateofbirth"));
				person.setGender(rs.getString("gender"));
				person.setDateOfDeath(rs.getTimestamp("dateofdeath"));

				person.setGivenName(rs.getString("givenname"));
				person.setOtherGivenNames(rs.getString("othergivennames"));
				person.setSurname(rs.getString("surname"));
				person.setPrevSurname(rs.getString("prevsurname"));
				person.setTitle(rs.getString("title"));

				person.setPostcode(rs.getString("postcode"));
				person.setStreet(rs.getString("street"));

				person.setStdSurname(rs.getString("stdsurname"));
				person.setStdGivenName(rs.getString("stdgivenname"));
				person.setStdPrevSurname(rs.getString("stdprevsurname"));
				person.setStdPostcode(rs.getString("stdpostcode"));
				
				person.setSkipDuplicateCheck(rs.getBoolean("skipduplicatecheck"));

				person.setLastUpdated(rs.getTimestamp("lastupdated"));
				person.setCreationDate(rs.getTimestamp("creationdate"));
				
				personList.add(person);
				
			}
			
		} catch (Exception e) {
			logger.error("Failure querying Person.",e);
			throw new MpiException("Person read failed. "+e.getMessage());
		} finally {

			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					logger.error("Failure closing resultset.",e);
					throw new MpiException("Person read failed. "+e.getMessage());
				}
			}
			
			if (preparedStatement != null) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					logger.error("Failure closing prepared statement.",e);
					throw new MpiException("Person read failed. "+e.getMessage());
				}
			}

		}
		
		return personList;

	}

	public static void create(Connection conn, Person person) throws MpiException {

		logger.debug("Starting");

		// id is set by at database sequence
		// prevsurname is NULL for an insert
		// stdprevsurname is NULL for an insert
		
		String insertSQL = "Insert into person "+
				"(dateofbirth, gender, dateofdeath,"+ 
	            "givenname, surname, othergivennames, title, postcode, street, "+ 
	            "stdsurname, stdgivenname, stdpostcode,"+
	            "prevsurname, stdprevsurname, nationalid, nationalidtype, skipduplicatecheck, "+
	            "localid, localidtype, originator )"+
				" values (?,?,?,"+
	            " ?,?,?,?,?,?,"+
	            " ?,?,?,"+
	            " NULL, NULL,?,?,?,"+
				" ?,?,?)";
		
		PreparedStatement preparedStatement = null;
		
		try {

			preparedStatement = conn.prepareStatement(insertSQL, Statement.RETURN_GENERATED_KEYS);
			populatePersonStatement(preparedStatement, person);
			preparedStatement.setString(16, person.getLocalId());
			preparedStatement.setString(17, person.getLocalIdType());
			preparedStatement.setString(18, person.getOriginator());

			int affectedRows = preparedStatement.executeUpdate();
			logger.debug("Affected Rows:"+affectedRows);
			
		    try (ResultSet generatedKeys = preparedStatement.getGeneratedKeys()) {
	            if (generatedKeys.next()) {
	            	person.setId(generatedKeys.getInt("id"));
	            	person.setCreationDate(generatedKeys.getTimestamp("creationdate"));
	            	person.setLastUpdated(generatedKeys.getTimestamp("lastupdated"));
	            	logger.debug("PERSONID:"+person.getId());
	            }
	            else {
	    			logger.error("Creating person failed, no ID obtained.");
	                throw new SQLException("Creating person failed, no ID obtained.");
	            }
		    }
		 
		} catch (Exception e) {
			logger.error("Failure creating Person:",e);
			throw new MpiException("Person create failed. "+e.getMessage());
		} finally {

			if (preparedStatement != null) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					logger.error("Failure closing Prepared Statement:",e);
					throw new MpiException("Person create failed. "+e.getMessage());
				}
			}

		}

	}

	public static void update(Connection conn, Person person) throws MpiException {

		logger.debug("Starting");

		if (person.getId()==0) {
			logger.error("Person has no ID - cannot update.");
			throw new MpiException("Person has no ID - cannot update.");
		}

		person.setLastUpdated(new Timestamp(System.currentTimeMillis()));
		String updateSQL = "Update person SET "+
			   "dateofbirth=?, gender=?,"+ 
		       "dateofdeath=?, givenname=?, surname=?, othergivennames=?,"+ 
		       "title=?, postcode=?, street=?, stdsurname=?,"+ 
		       "stdgivenname=?, stdpostcode=?, nationalid=?, nationalidtype=?, skipduplicatecheck = ?, "+
		       "prevsurname=?, stdprevsurname=?, lastupdated=? "+
		       "WHERE id = ? ";
		
		PreparedStatement preparedStatement = null;
		
		try {

			preparedStatement = conn.prepareStatement(updateSQL);
			
			populatePersonStatement(preparedStatement, person);
			preparedStatement.setString(16, person.getPrevSurname());
			preparedStatement.setString(17, person.getStdPrevSurname());
			preparedStatement.setTimestamp(18, person.getLastUpdated());
			preparedStatement.setInt(19, person.getId());

			int affectedRows = preparedStatement.executeUpdate();
			logger.debug("Affected Rows:"+affectedRows);
			
		} catch (Exception e) {
			logger.error("Failure updating Person.",e);
			throw new MpiException("Person update failed. "+e.getMessage());
		} finally {

			if (preparedStatement != null) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					logger.error("Failure closing Prepared Statement:",e);
					throw new MpiException("Person update failed. "+e.getMessage());
				}
			}

		}

	}

	public static void delete(Connection conn, Person person) throws MpiException {
		
		logger.debug("Starting");

		String deleteSQL = "delete from person where id = ?";
		
		PreparedStatement preparedStatement = null;
		
		try {

			preparedStatement = conn.prepareStatement(deleteSQL);
			preparedStatement.setInt(1, person.getId());
			int affectedRows = preparedStatement.executeUpdate();
			logger.debug("Affected Rows:"+affectedRows);
			
		} catch (Exception e) {
			logger.error("Failure deleting Person:",e);
			throw new MpiException("Person delete failed. "+e.getMessage());
		} finally {

			if (preparedStatement != null) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					logger.error("Failure closing Prepared Statement:",e);
					throw new MpiException("Person delete failed. "+e.getMessage());
				}
			}
		}
	}
			
	private static void populatePersonStatement(PreparedStatement preparedStatement, Person person) throws SQLException {

		logger.debug("Starting");

		if (person.getDateOfBirth() != null ) {
			preparedStatement.setTimestamp(1, new Timestamp(person.getDateOfBirth().getTime()));
			preparedStatement.setNull(3,java.sql.Types.TIMESTAMP);
		}
		
		preparedStatement.setString(2, person.getGender());
		
		if (person.getDateOfDeath() != null ) {
			preparedStatement.setTimestamp(3, new Timestamp(person.getDateOfDeath().getTime()));
		} else {
			preparedStatement.setNull(3,java.sql.Types.TIMESTAMP);
		}
	
		preparedStatement.setString(4, person.getGivenName());
		preparedStatement.setString(5, person.getSurname());
		preparedStatement.setString(6, person.getOtherGivenNames());
		preparedStatement.setString(7, person.getTitle());
		
		preparedStatement.setString(8, person.getPostcode());
		preparedStatement.setString(9, person.getStreet());
		
		preparedStatement.setString(10, person.getStdSurname());
		preparedStatement.setString(11, person.getStdGivenName());
		preparedStatement.setString(12, person.getStdPostcode());
		
		preparedStatement.setString(13, person.getPrimaryId());
		preparedStatement.setString(14, person.getPrimaryIdType());

		preparedStatement.setBoolean(15, person.isSkipDuplicateCheck());
		
	}
	
}
