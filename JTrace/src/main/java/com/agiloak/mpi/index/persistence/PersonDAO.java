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
import com.agiloak.mpi.index.Person;

public class PersonDAO {
	
	private final static Logger logger = LoggerFactory.getLogger(PersonDAO.class);

	public static Person findByLocalId(String localIdType, String localId, String originator) throws MpiException {

		String findSQL = "select * from jtrace.person where localidtype = ? and localid = ? and originator = ? ";
		
		PreparedStatement preparedStatement = null;
		Connection conn = null;
		ResultSet rs = null;
		
		Person person = null;
		
		try {

			conn = SimpleConnectionManager.getDBConnection();
			
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

				person.setNationalId(rs.getString("nationalid"));
				person.setNationalIdType(rs.getString("nationalidtype"));

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
}
			
		} catch (SQLException e) {
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

	public static List<Person> findByMasterId(int masterId) throws MpiException {

		String findSQL = "select * from jtrace.person p, jtrace.linkrecord lr where lr.masterid = ? and p.id = lr.personid ";
		
		PreparedStatement preparedStatement = null;
		Connection conn = null;
		ResultSet rs = null;
		
		List<Person> personList = new ArrayList<Person>();
		
		try {

			conn = SimpleConnectionManager.getDBConnection();
			
			preparedStatement = conn.prepareStatement(findSQL);
			preparedStatement.setInt(1, masterId);

			rs = preparedStatement.executeQuery();
			
			while  (rs.next()){
				Person person = new Person();
				person.setId(rs.getInt("id"));
				person.setOriginator(rs.getString("originator"));
				person.setLocalId(rs.getString("localid"));
				person.setLocalIdType(rs.getString("localidtype"));

				person.setNationalId(rs.getString("nationalid"));
				person.setNationalIdType(rs.getString("nationalidtype"));

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
				
				personList.add(person);
				
			}
			
		} catch (SQLException e) {
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

	public static void create(Person person) throws MpiException {

		// id is set by at database sequence
		// prevsurname is NULL for an insert
		// stdprevsurname is NULL for an insert
		
		String insertSQL = "Insert into jtrace.person "+
				"(dateofbirth, gender, dateofdeath,"+ 
	            "givenname, surname, othergivennames, title, postcode, street, "+ 
	            "stdsurname, stdgivenname, stdpostcode,"+
	            "prevsurname, stdprevsurname, nationalId, nationalIdType,"+
	            "localid, localidtype, originator)"+
				" values (?,?,?,"+
	            " ?,?,?,?,?,?,"+
	            " ?,?,?,"+
	            " NULL, NULL,?,?,"+
				" ?,?,?)";
		
		PreparedStatement preparedStatement = null;
		Connection conn = null;
		
		try {

			conn = SimpleConnectionManager.getDBConnection();
			
			preparedStatement = conn.prepareStatement(insertSQL, Statement.RETURN_GENERATED_KEYS);
			populatePersonStatement(preparedStatement, person);
			preparedStatement.setString(15, person.getLocalId());
			preparedStatement.setString(16, person.getLocalIdType());
			preparedStatement.setString(17, person.getOriginator());

			int affectedRows = preparedStatement.executeUpdate();
			logger.debug("Affected Rows:"+affectedRows);
			
		    try (ResultSet generatedKeys = preparedStatement.getGeneratedKeys()) {
	            if (generatedKeys.next()) {
	            	person.setId(generatedKeys.getInt(1));
	            	logger.debug("PERSONID:"+person.getId());
	            }
	            else {
	    			logger.error("Creating person failed, no ID obtained.");
	                throw new SQLException("Creating person failed, no ID obtained.");
	            }
		    }
		 
		} catch (SQLException e) {
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

	public static void update(Person person) throws MpiException {

		String updateSQL = "Update jtrace.person SET "+
			   "dateofbirth=?, gender=?,"+ 
		       "dateofdeath=?, givenname=?, surname=?, othergivennames=?,"+ 
		       "title=?, postcode=?, street=?, stdsurname=?,"+ 
		       "stdgivenname=?, stdpostcode=?, nationalid=?, nationalidtype=?,"+
		       "prevsurname=?, stdprevsurname=? "+
		       "WHERE id = ? ";
		
		PreparedStatement preparedStatement = null;
		Connection conn = null;
		
		try {

			conn = SimpleConnectionManager.getDBConnection();
			
			preparedStatement = conn.prepareStatement(updateSQL);
			
			populatePersonStatement(preparedStatement, person);
			preparedStatement.setString(15, person.getPrevSurname());
			preparedStatement.setString(16, person.getStdPrevSurname());
			preparedStatement.setInt(17, person.getId());

			int affectedRows = preparedStatement.executeUpdate();
			logger.debug("Affected Rows:"+affectedRows);
			
		} catch (SQLException e) {
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

	public static void delete(Person person) throws MpiException {
		
		String deleteSQL = "delete from jtrace.person where id = ?";
		
		PreparedStatement preparedStatement = null;
		Connection conn = null;
		
		try {

			conn = SimpleConnectionManager.getDBConnection();
			
			preparedStatement = conn.prepareStatement(deleteSQL);
			conn = SimpleConnectionManager.getDBConnection();
			preparedStatement.setInt(1, person.getId());
			int affectedRows = preparedStatement.executeUpdate();
			logger.debug("Affected Rows:"+affectedRows);
			
		} catch (SQLException e) {
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
			
	private static void populatePersonStatement(PreparedStatement preparedStatement, Person person) throws SQLException{

		preparedStatement.setTimestamp(1, new Timestamp(person.getDateOfBirth().getTime()));
		preparedStatement.setString(2, person.getGender());
		preparedStatement.setTimestamp(3, new Timestamp(person.getDateOfDeath().getTime()));
	
		preparedStatement.setString(4, person.getGivenName());
		preparedStatement.setString(5, person.getSurname());
		preparedStatement.setString(6, person.getOtherGivenNames());
		preparedStatement.setString(7, person.getTitle());
		
		preparedStatement.setString(8, person.getPostcode());
		preparedStatement.setString(9, person.getStreet());
		
		preparedStatement.setString(10, person.getStdSurname());
		preparedStatement.setString(11, person.getStdGivenName());
		preparedStatement.setString(12, person.getStdPostcode());
		
		preparedStatement.setString(13, person.getNationalId());
		preparedStatement.setString(14, person.getNationalIdType());
		
	}
	
}
