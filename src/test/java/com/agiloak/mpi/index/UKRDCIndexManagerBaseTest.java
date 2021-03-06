package com.agiloak.mpi.index;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.index.persistence.LinkRecordDAO;
import com.agiloak.mpi.index.persistence.MasterRecordDAO;
import com.agiloak.mpi.index.persistence.PersonDAO;
import com.agiloak.mpi.trace.persistence.TraceDAO;
import com.agiloak.mpi.workitem.persistence.WorkItemDAO;
import java.util.Date;

public class UKRDCIndexManagerBaseTest {

	private final static Logger logger = LoggerFactory.getLogger(UKRDCIndexManagerBaseTest.class);
	public static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
	public static Connection conn = null;

	protected final static String MR = "MR";

	protected static java.util.Date getDate(String sDate) {
		
		java.util.Date uDate = null;
	    try {
		   uDate = formatter.parse(sDate);
		} catch (ParseException e) {
			e.printStackTrace();
			assert(false);
		}	
	    return uDate;
	    
	}

	protected static void clearAll(Connection conn) throws MpiException {
		deleteAll(conn, "person");
		deleteAll(conn, "masterrecord");
		deleteAll(conn, "linkrecord");
		deleteAll(conn, "workitem");
		deleteAll(conn, "pidxref");
		deleteAll(conn, "audit");
		miscTestSQL(conn, "ALTER SEQUENCE jtrace.ukrdc_id RESTART WITH 500000000;");
		miscTestSQL(conn, "ALTER SEQUENCE jtrace.patient_id RESTART WITH 1000000000;");
	}

	public static Person createPerson(String local, String originator, Date dob, String surname, String given, String gender, NationalIdentity nhs) throws MpiException {
		Person person = new Person().setDateOfBirth(dob).setSurname(surname).setGivenName(given).setGender(gender);
		person.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		person.setLocalId(local).setLocalIdType("MR").setOriginator(originator);
		if (nhs != null) {
			person.addNationalId(nhs);
		}
		return person;
	}
	
	// Get the singular link of a person to a national id type 
	public static NationalIdentity getNationalIdentity(Connection conn, Person person, String nationaltype) throws MpiException {
		LinkRecord link = LinkRecordDAO.findByPersonAndType(conn, person.getId(), nationaltype);
		if (link==null) return new NationalIdentity("",""); // return an invalid entry rather than null - easier for the assertion in the calling test
		
		MasterRecord master = MasterRecordDAO.get(conn, link.getMasterId());
		if (master == null) return null;
		return master.getNationalIdentity();
	}
	
	// Check link of a person to a master record. Asking the same question as getNationalIdentity in a different way to verify integrity
	public static boolean isPersonLinkedToMaster(Connection conn, Person person, NationalIdentity natId) throws MpiException {
		MasterRecord master = MasterRecordDAO.findByNationalId(conn, natId.getId(), natId.getType());
		if (master==null) return false;

		List<LinkRecord> links = LinkRecordDAO.findByMaster(conn, master.getId());
		for (LinkRecord link : links) {
			if (link.getPersonId() == person.getId()) return true;
		}
		return false;
	}

	public static void deleteAll(Connection conn, String tableName) throws MpiException {
		
		logger.debug("Starting - Delete all:"+tableName);

		String deleteSQL = "delete from "+tableName;
		
		PreparedStatement preparedStatement = null;
		
		try {

			preparedStatement = conn.prepareStatement(deleteSQL);
			int affectedRows = preparedStatement.executeUpdate();
			logger.debug("Affected Rows:"+affectedRows);
			
		} catch (Exception e) {
			logger.error("Failure deleting:"+tableName,e);
			throw new MpiException("Failure deleting:"+tableName+e.getMessage());
		} finally {

			if (preparedStatement != null) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					logger.error("Failure closing prepared statement:"+tableName,e);
					throw new MpiException("Failure closing prepared statement:"+tableName+", "+e.getMessage());
				}
			}
		}
	}
	protected static void miscTestSQL(Connection conn, String sql) throws MpiException {
		
		logger.debug("Running:"+sql);

		PreparedStatement preparedStatement = null;
		
		try {

			preparedStatement = conn.prepareStatement(sql);
			int affectedRows = preparedStatement.executeUpdate();
			logger.debug("Affected Rows:"+affectedRows);
			
		} catch (Exception e) {
			logger.error("Failure running:"+sql,e);
			throw new MpiException("Failure running:"+sql+", "+e.getMessage());
		} finally {

			if (preparedStatement != null) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					logger.error("Failure closing prepared statement:"+sql,e);
					throw new MpiException("Failure closing prepared statement:"+sql+", "+e.getMessage());
				}
			}
		}
	}
	
	protected static void clear(Connection conn, String type, String localId, String originator) throws MpiException {
		
		Person person = PersonDAO.findByLocalId(conn, type, localId, originator);
		if (person != null) {
			List<LinkRecord> links = LinkRecordDAO.findByPerson(conn, person.getId());
			for (LinkRecord link : links) {
				MasterRecordDAO.delete(conn, link.getMasterId());
			}
			LinkRecordDAO.deleteByPerson(conn, person.getId());
			WorkItemDAO.deleteByPerson(conn, person.getId());
			PersonDAO.delete(conn, person);
			String traceId = TraceDAO.getTraceId(conn, localId, "MR", originator, "AUTO");
			if (traceId != null) {
				TraceDAO.clearByTraceId(conn, traceId);
			}
		}
	
	}
	public UKRDCIndexManagerBaseTest() {
		super();
	}

	protected NationalIdentity store(Person person) {
		UKRDCIndexManager im = new UKRDCIndexManager();
		UKRDCIndexManagerResponse resp = im.store(person);
		NationalIdentity natId = resp.getNationalIdentity();
		return natId;
	}

}