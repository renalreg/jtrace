package com.agiloak.mpi.index.persistence;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.index.Person;
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
	
	public static PidXREF get(Connection conn, int id) throws MpiException {

		logger.debug("Starting");

		String getSQL = "select * from pidxref where id = ? ";
		
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		
		PidXREF pidxref = null;
		
		try {

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
		}
		
		return pidxref;

	}


	public static void create(Connection conn, PidXREF xref) throws MpiException {

		logger.debug("Starting");
		
		if (xref.getPid()==null) {
			String pid = allocate(conn);
			xref.setPid(pid);
		}

		String insertSQL = "Insert into pidxref "+
				"(pid, sendingFacility, sendingExtract, localId)"+
				" values (?,?,?,?)";
		
		PreparedStatement preparedStatement = null;
		
		try {

			preparedStatement = conn.prepareStatement(insertSQL, Statement.RETURN_GENERATED_KEYS);
			preparedStatement.setString(1, xref.getPid());
			preparedStatement.setString(2, xref.getSendingFacility());
			preparedStatement.setString(3, xref.getSendingExtract());
			preparedStatement.setString(4, xref.getLocalId());

			int affectedRows = preparedStatement.executeUpdate();
			logger.debug("Affected Rows:"+affectedRows);
			
		    try (ResultSet generatedKeys = preparedStatement.getGeneratedKeys()) {
	            if (generatedKeys.next()) {
	            	xref.setId(generatedKeys.getInt("id"));
	            	xref.setCreationDate(generatedKeys.getTimestamp("creationdate"));
	            	xref.setLastUpdated(generatedKeys.getTimestamp("lastupdated"));
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
		}

	}

	public static PidXREF findByLocalId(Connection conn, String sendingFacility, String sendingExtract, String localId) throws MpiException {
		
		logger.debug("Starting");

		String findSQL = "select * from pidxref where sendingFacility = ? and sendingExtract = ? and localId = ? ";
		
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		
		PidXREF xref = null;
		
		try {

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
		}
		
		return xref;

	}
	
	public static List<Person> FindByNationalIdAndFacility(Connection conn, String sendingFacility, String sendingExtract, String nationalIdType, String nationalId) throws MpiException {

		logger.debug("Starting");

		String findSQL = "select p.* from  pidxref px, person p, linkrecord lr, masterrecord mr "+
					     "where px.sendingFacility = ? and px.sendingExtract = ? and px.pid = p.localId and p.id = lr.personid "+
					     "and lr.masterId = mr.id and mr.nationalIdType = ? and mr.nationalId = ? ";

		
		PreparedStatement preparedStatement = null;
		ResultSet rs = null;
		
		List<Person> personList = new ArrayList<Person>();
		
		try {

			preparedStatement = conn.prepareStatement(findSQL);
			preparedStatement.setString(1, sendingFacility);
			preparedStatement.setString(2, sendingExtract);
			preparedStatement.setString(3, nationalIdType);
			preparedStatement.setString(4, nationalId);

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

	
	public static String allocate(Connection conn) throws MpiException {

		logger.debug("Starting");
		String patientId = allocateSequence(conn, "patient_id");
		logger.debug("Complete");
		return patientId;
		
	}
	
	public static void deleteByLocalId(Connection conn, String sendingFacility, String sendingExtract, String localId) throws MpiException {
		
		logger.debug("Starting");

		String deleteSQL = "delete from pidxref where sendingFacility = ? and sendingExtract = ? and localId = ?";
		
		PreparedStatement preparedStatement = null;
		
		try {

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
			xref.setLastUpdated(rs.getTimestamp("lastupdated"));
			xref.setCreationDate(rs.getTimestamp("creationdate"));

		} catch (Exception e) {
			logger.error("Failure querying PidXREF.",e);
			throw new MpiException("Failure querying PidXREF. "+e.getMessage());
		} 
		
		return xref;

	}
	

}
