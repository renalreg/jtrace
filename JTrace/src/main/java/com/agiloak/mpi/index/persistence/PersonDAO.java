package com.agiloak.mpi.index.persistence;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.agiloak.mpi.index.AlternativeReference;
import com.agiloak.mpi.index.Person;

public class PersonDAO {
	
	private final static Logger logger = LoggerFactory.getLogger(PersonDAO.class);

	/*
	 * Basic impl - just gets masterid and surname to support update mechanics
	 * TODO: Extend to full dataset
	 */
	public static Person findPerson(String localIdType, String localId, String originator) {

		String findSQL = "select masterid, surname from jtrace.person where localidtype = ? and localid = ? and localidoriginator = ? ";
		
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
				person.setMasterId(rs.getLong("masterid"));
				person.setSurname(rs.getString("surname"));
			}
			
		} catch (SQLException e) {
			logger.error("Failure querying Person.",e);
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
		
		return person;

	}

	public static void insert(Person person) {

		// MasterId is set by at database sequence
		// prevsurname is NULL for an insert
		// stdprevsurname is NULL for an insert
		
		String insertSQL = "Insert into jtrace.person "+
				"(dateofbirth, gender, dateofdeath,"+ 
	            "givenname, surname, othergivennames, title, postcode, street, "+ 
	            "stdsurname, stdgivenname, stdpostcode,"+
	            "prevsurname, stdprevsurname,"+
	            "masterid, localid, localidtype, localidoriginator)"+
				" values (?,?,?,"+
	            " ?,?,?,?,?,?,"+
	            " ?,?,?,"+
	            " NULL, NULL,"+
				" DEFAULT, ?,?,?)";
		
		PreparedStatement preparedStatement = null;
		Connection conn = null;
		
		try {

			conn = SimpleConnectionManager.getDBConnection();
			
			preparedStatement = conn.prepareStatement(insertSQL, Statement.RETURN_GENERATED_KEYS);
			populatePersonStatement(preparedStatement, person);
			preparedStatement.setString(13, person.getLocalId());
			preparedStatement.setString(14, person.getLocalIdType());
			preparedStatement.setString(15, person.getLocalIdOriginator());

			int affectedRows = preparedStatement.executeUpdate();
			logger.debug("Affected Rows:"+affectedRows);
			
		    try (ResultSet generatedKeys = preparedStatement.getGeneratedKeys()) {
	            if (generatedKeys.next()) {
	            	person.setMasterId(generatedKeys.getLong(1));
	            	logger.debug("MASTERID:"+person.getMasterId());
	            }
	            else {
	    			logger.error("Creating person failed, no ID obtained.");
	                throw new SQLException("Creating person failed, no ID obtained.");
	            }
		    }
		 
		    insertMR(person);
		    insertAR(person);
		    
		} catch (SQLException e) {
			logger.error("Failure inserting Person:",e);

		} finally {

			if (preparedStatement != null) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					logger.error("Failure closing Prepared Statement:",e);
				}
			}

		}

	}

	public static void update(Person person) {

		String updateSQL = "Update jtrace.person SET "+
			   "dateofbirth=?, gender=?,"+ 
		       "dateofdeath=?, givenname=?, surname=?, othergivennames=?,"+ 
		       "title=?, postcode=?, street=?, stdsurname=?,"+ 
		       "stdgivenname=?, stdpostcode=?, prevsurname=?, stdprevsurname=? "+
		       "WHERE masterid = ? ";
		
		PreparedStatement preparedStatement = null;
		Connection conn = null;
		
		try {

			conn = SimpleConnectionManager.getDBConnection();
			
			preparedStatement = conn.prepareStatement(updateSQL);
			
			populatePersonStatement(preparedStatement, person);
			preparedStatement.setString(13, person.getPrevSurname());
			preparedStatement.setString(14, person.getStdPrevSurname());
			preparedStatement.setLong(15, person.getMasterId());

			int affectedRows = preparedStatement.executeUpdate();
			logger.debug("Affected Rows:"+affectedRows);
			
		    // Update won't update the primary reference (localid) but will update all secondary references
		    updateAR(person);
		    
		} catch (SQLException e) {
			logger.error("Failure updating Person.",e);

		} finally {

			if (preparedStatement != null) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					logger.error("Failure closing Prepared Statement:",e);
				}
			}

		}

	}

	private static void populatePersonStatement(PreparedStatement preparedStatement, Person person) throws SQLException{
		preparedStatement.setDate(1,getSqlDate(person.getDateOfBirth()));
		preparedStatement.setString(2, person.getGender());
		preparedStatement.setDate(3,getSqlDate(person.getDateOfDeath()));

		preparedStatement.setString(4, person.getGivenName());
		preparedStatement.setString(5, person.getSurname());
		preparedStatement.setString(6, person.getOtherGivenNames());
		preparedStatement.setString(7, person.getTitle());
		
		preparedStatement.setString(8, person.getPostcode());
		preparedStatement.setString(9, person.getStreet());
		
		preparedStatement.setString(10, person.getStdSurname());
		preparedStatement.setString(11, person.getStdGivenName());
		preparedStatement.setString(12, person.getStdPostcode());
		
	}
	
	private static void insertMR(Person person) {

		// supercedeby is NULL for an insert
		
		String insertSQL = "Insert into jtrace.masterreference "+
				"(masterid, localid, localidtype, originator, supercededby)"+
				" values (?,?,?,?,0)";
		
		PreparedStatement preparedStatement = null;
		Connection conn = null;
		
		try {

			conn = SimpleConnectionManager.getDBConnection();
			
			preparedStatement = conn.prepareStatement(insertSQL);
			preparedStatement.setLong(1, person.getMasterId());
			preparedStatement.setString(2, person.getLocalId());
			preparedStatement.setString(3, person.getLocalIdType());
			preparedStatement.setString(4, person.getLocalIdOriginator());

			int affectedRows = preparedStatement.executeUpdate();
			logger.debug("Affected Rows:"+affectedRows);
			
		} catch (SQLException e) {
			logger.error("Failure inserting MR:"+e.getErrorCode()+":"+e.getMessage(),e);

		} finally {

			if (preparedStatement != null) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					logger.error("Failure closing Prepared Statement:",e);
				}
			}

		}

	}

	private static void insertAR(Person person) {

		if (person.getSecondaryId()==null || person.getSecondaryIdType()==null){
			return;
		}
		
		// supercedeby is NULL for an insert
		
		String insertSQL = "Insert into jtrace.alternativereference "+
				"(masterid, altid, altidtype, originator, supercededby)"+
				" values (?,?,?,?,0)";
		
		PreparedStatement preparedStatement = null;
		Connection conn = null;
		
		try {

			conn = SimpleConnectionManager.getDBConnection();
			
			preparedStatement = conn.prepareStatement(insertSQL);
			preparedStatement.setLong(1, person.getMasterId());
			preparedStatement.setString(2, person.getSecondaryId());
			preparedStatement.setString(3, person.getSecondaryIdType());
			preparedStatement.setString(4, person.getSecondaryIdOriginator());

			int affectedRows = preparedStatement.executeUpdate();
			logger.debug("Affected Rows:"+affectedRows);
			
		} catch (SQLException e) {
			logger.error("Failure inserting AR:"+e.getErrorCode()+":"+e.getMessage(),e);

		} finally {

			if (preparedStatement != null) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					logger.error("Failure closing Prepared Statement:",e);
				}
			}

		}

	}

	/*
	 * TODO : this should work for a list of references - update logic may get tricky!
	 */
	private static void updateAR(Person person) {

		AlternativeReference alt = readAR(person);

		if (person.getSecondaryId()==null || person.getSecondaryIdType()==null){

			if (alt==null){
				// no alt record on database on new record - do nothing
				return;
			} else {
				// Alt record on database but not on incoming - logical delete of alt on record
				deleteAR(alt);
			}
			
		} else {
			
			if (alt==null){
				// no previous AR but there is now one - just insert
				insertAR(person);
				
			} else {
				
				if (alt.getId().equals(person.getSecondaryId()) && 
					alt.getType().equals(person.getSecondaryIdType()) &&
					alt.getOriginator().equals(person.getSecondaryIdOriginator())) {
					// if not changed - do nothing
					return;
					
				} else {
					// It has changed - delete and update
					deleteAR(alt);
					insertAR(person);
				}
			}
		}
		
	}

	private static AlternativeReference readAR(Person person) {

		String querySQL = "select * from jtrace.alternativereference "+
				"where masterid = ? and supercededby = 0;";
		
		AlternativeReference alt = null;
		PreparedStatement preparedStatement = null;
		Connection conn = null;
		ResultSet rs = null;
		
		try {

			conn = SimpleConnectionManager.getDBConnection();
			
			preparedStatement = conn.prepareStatement(querySQL);
			preparedStatement.setLong(1, person.getMasterId());

			rs = preparedStatement.executeQuery();
			
			if (rs.next()){
				alt = new AlternativeReference();
				alt.setArid(rs.getLong("arid"));
				alt.setMasterId(rs.getLong("masterid"));
				alt.setId(rs.getString("altid"));
				alt.setType(rs.getString("altidtype"));
				alt.setOriginator(rs.getString("originator"));
				alt.setSupercededBy(rs.getString("supercededby"));
			}
			
		} catch (SQLException e) {
			logger.error("Failure reading AR:"+e.getErrorCode()+":"+e.getMessage(),e);

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
					logger.error("Failure closing Prepared Statement:",e);
				}
			}

		}
		
		return alt;

	}

	private static void deleteAR(AlternativeReference alt) {

		String updateSQL = "update jtrace.alternativereference "+
						   "set supercededby = -1 where arid = ?";
		
		PreparedStatement preparedStatement = null;
		Connection conn = null;
		
		try {

			conn = SimpleConnectionManager.getDBConnection();
			
			preparedStatement = conn.prepareStatement(updateSQL);
			preparedStatement.setLong(1, alt.getArid());

			int affectedRows = preparedStatement.executeUpdate();
			logger.debug("Affected Rows:"+affectedRows);

			
		} catch (SQLException e) {
			logger.error("Failure updating AR:"+e.getErrorCode()+":"+e.getMessage());

		} finally {

			if (preparedStatement != null) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					logger.error("Failure closing Prepared Statement:",e);
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
