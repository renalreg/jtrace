package com.agiloak.mpi.index;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.BeforeClass;
import org.junit.Test;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.index.persistence.LinkRecordDAO;
import com.agiloak.mpi.index.persistence.MasterRecordDAO;
import com.agiloak.mpi.index.persistence.PersonDAO;

public class UKRDCIndexManagerTest {
	
	private Date d1 = getDate("1962-08-31");
	private Date d2 = getDate("1962-08-30");
	private Date d3 = getDate("1962-07-31");
	private Date d4 = getDate("1961-08-31");
	private Date d5 = getDate("1961-07-30");

	public static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
	
	@BeforeClass
	public static void setup()  throws MpiException {
		
		int masterId1 = getAndDeleteMasterId("NHS0000001","NHS");
		clear( "TST1000001", "TST1", masterId1);
		clear( "TST2000001", "TST2", masterId1);
		clear( "TST3000001", "TST3", masterId1);
		clear( "TST4000001", "TST4", masterId1);

		int masterId2 = getAndDeleteMasterId("NHS0000010","NHS");
		clear( "TST5000001", "TST5", masterId2);
		
		int masterId3 = getAndDeleteMasterId("NHS0000010","NHS");
		clear( "UPD1000001", "UPD1", masterId3);
		int masterId4 = getAndDeleteMasterId("NHS0000011","NHS");
		clear( "UPD1000002", "UPD2", masterId4);
		
	}

	public static int getAndDeleteMasterId(String id, String type) throws MpiException {
		int masterId = 0;
		MasterRecord mr = MasterRecordDAO.findByNationalId(id,type);
		if (mr !=null) {
			masterId = mr.getId();
			MasterRecordDAO.deleteByNationalId(id, type);
		}
		return masterId;
		
	}
	
	public static void clear(String localId, String localIdType, int masterId)  throws MpiException {
		
		Person person = PersonDAO.findByLocalId("MR", localId, localIdType);
		if (person != null) {
			PersonDAO.delete(person);
			if (masterId > 0) {
				LinkRecord lr = LinkRecordDAO.find(masterId, person.getId());
				if (lr != null) {
					LinkRecordDAO.delete(lr);
				}
			}
		}

	}

	@Test
	public void testIndexManagerUpdate() throws MpiException {

		UKRDCIndexManager im = new UKRDCIndexManager();
		
		// Prepare Person 1
		Person p1 = new Person().setDateOfBirth(d1).setSurname("UPDATE").setGivenName("PAT").setPrimaryIdType("NHS").setPrimaryId("NHS0000010").setGender("1");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId("UPD1000001").setLocalIdType("MR").setOriginator("UPD1");
		im.createOrUpdate(p1);
		assert(true);
		
		// Update the record - no change
		im.createOrUpdate(p1);
		assert(true);

		// Prepare p2 - no national Id. Same demographics so should raise a Work item
		Person p2 = new Person().setDateOfBirth(d1).setSurname("UPDATE").setGivenName("PAT").setGender("1");
		p2.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p2.setLocalId("UPD1000002").setLocalIdType("MR").setOriginator("UPD2");
		im.createOrUpdate(p2);
		assert(true);

		// Add national id to a record which previously had none
		p2.setPrimaryIdType("NHS").setPrimaryId("NHS0000011");
		im.createOrUpdate(p2);
		assert(true);

		// Add remove national id again
		p2.setPrimaryIdType(null).setPrimaryId(null);
		im.createOrUpdate(p2);
		assert(true);

	}
	
	@Test
	public void testIndexManagerNew() throws MpiException {
		
		// Basic test - add a new record
		Person p1 = new Person().setDateOfBirth(d1).setSurname("JONES").setGivenName("NICK").setPrimaryIdType("NHS").setPrimaryId("NHS0000001").setGender("1");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId("TST1000001").setLocalIdType("MR").setOriginator("TST1");
		UKRDCIndexManager im = new UKRDCIndexManager();
		im.createOrUpdate(p1);
		assert(true);
		
		// Basic test - link a 2nd record
		Person p2 = new Person().setDateOfBirth(d1).setSurname("JONES").setGivenName("NICK").setPrimaryIdType("NHS").setPrimaryId("NHS0000001").setGender("1");
		p2.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p2.setLocalId("TST2000001").setLocalIdType("MR").setOriginator("TST2");
		im = new UKRDCIndexManager();
		im.createOrUpdate(p2);
		assert(true);

		// Basic test - link a 3rd record - partial DOB match - should still link
		Person p3 = new Person().setDateOfBirth(d2).setSurname("JONES").setGivenName("NICK").setPrimaryIdType("NHS").setPrimaryId("NHS0000001").setGender("1");
		p3.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p3.setLocalId("TST3000001").setLocalIdType("MR").setOriginator("TST3");
		im = new UKRDCIndexManager();
		im.createOrUpdate(p3);
		assert(true);

		// Basic test - add a 4th record - same demogs but no national id. Should not link but add a work item
		Person p4 = new Person().setDateOfBirth(d1).setSurname("JONES").setGivenName("NICK").setGender("1");
		p4.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p4.setLocalId("TST4000001").setLocalIdType("MR").setOriginator("TST4");
		im = new UKRDCIndexManager();
		im.createOrUpdate(p4);
		assert(true);
		
		// Basic test - add a 5th record - same demogs but different national id. Should create new master, link to it but add a work item about the potential match
		Person p5 = new Person().setDateOfBirth(d1).setSurname("JONES").setGivenName("NICK").setGender("1");
		p5.setPrimaryId("CHI0000001").setPrimaryIdType("CHI");
		p5.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p5.setLocalId("TST5000001").setLocalIdType("MR").setOriginator("TST5");
		im = new UKRDCIndexManager();
		im.createOrUpdate(p5);
		assert(true);
	}
	
	private static java.util.Date getDate(String sDate){
		
		java.util.Date uDate = null;
	    try {
		   uDate = formatter.parse(sDate);
		} catch (ParseException e) {
			e.printStackTrace();
			assert(false);
		}	
	    return uDate;
	    
	}
	
}
