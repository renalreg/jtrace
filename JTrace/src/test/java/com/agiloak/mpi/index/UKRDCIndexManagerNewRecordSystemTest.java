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

public class UKRDCIndexManagerNewRecordSystemTest {
	
	private Date d1 = getDate("1962-08-31");
	private Date d2 = getDate("1962-08-30");
	private Date d3 = getDate("1962-07-31");
	private Date d4 = getDate("1961-08-31");
	private Date d5 = getDate("1961-07-30");

	public static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
	
	@BeforeClass
	public static void setup()  throws MpiException {
		
		int masterId = getAndDeleteMasterId("NHS0000001","NHS");
		clear( "PRE1000001", "PRE1", masterId);
		clear( "NSYS100001", "NSYS1", masterId);
		clear( "NSYS100002", "NSYS1", masterId);
		
		masterId = getAndDeleteMasterId("NHS2000001","NHS");
		clear( "PRE2000001", "PRE2", masterId);
		masterId = getAndDeleteMasterId("NHS2000002","NHS");
		clear( "NSYS200002", "NSYS2", masterId);
		masterId = getAndDeleteMasterId("NHS2000003","NHS");
		clear( "NSYS200001", "NSYS2", masterId);

		masterId = getAndDeleteMasterId("NHS3000001","NHS");
		clear( "PRE3000001", "PRE3", masterId);
		clear( "NSYS300001", "NSYS3", 0);
		clear( "NSYS300002", "NSYS3", 0);
		
		//TODO Clear work items

	}


	@Test
	public void testNewWithExistingNationalId() throws MpiException {

		UKRDCIndexManager im = new UKRDCIndexManager();
		
		// P1S - Set up NationalId for P1. New Person, New MAster and new link to the master
		Person p1s = new Person().setDateOfBirth(d1).setSurname("JONES").setGivenName("NICK").setNationalIdType("NHS").setNationalId("NHS0000001").setGender("1");
		p1s.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1s.setLocalId("PRE1000001").setLocalIdType("MR").setOriginator("PRE1");
		im.createOrUpdate(p1s);

		// P1 - New + NationalId + NationalId exists + Matches. New Person & Link to existing Master
		Person p1 = new Person().setDateOfBirth(d1).setSurname("JONES").setGivenName("NICK").setNationalIdType("NHS").setNationalId("NHS0000001").setGender("1");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId("NSYS100001").setLocalIdType("MR").setOriginator("NSYS1");
		im.createOrUpdate(p1);
		// P2 - New + NationalId + NationalId exists + No match (DOB 2 part and given name mismatch). New Person & work item about mismatched Master
		Person p2 = new Person().setDateOfBirth(d2).setSurname("JONES").setGivenName("MATTY").setNationalIdType("NHS").setNationalId("NHS0000001").setGender("1");
		p2.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p2.setLocalId("NSYS100002").setLocalIdType("MR").setOriginator("NSYS1");
		im.createOrUpdate(p2);

		assert(true);
		
	}
	
	@Test
	public void testNewWithNonexistantNationalId() throws MpiException {

		UKRDCIndexManager im = new UKRDCIndexManager();
		
		// P1S - Set up NationalId for P2. New Person, New Master and Link to new master
		Person p1s = new Person().setDateOfBirth(d1).setSurname("EVANS").setGivenName("BOB").setNationalIdType("NHS").setNationalId("NHS2000001").setGender("1");
		p1s.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1s.setLocalId("PRE2000001").setLocalIdType("MR").setOriginator("PRE2");
		im.createOrUpdate(p1s);

		// P1 - New + NationalId + NationalId which does not exist. New Master, New Person & Link to new Master
		Person p1 = new Person().setDateOfBirth(d3).setSurname("SMITH").setGivenName("BILL").setNationalIdType("NHS").setNationalId("NHS2000002").setGender("1");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId("NSYS200001").setLocalIdType("MR").setOriginator("NSYS2");
		im.createOrUpdate(p1);
		// P2 - New + NationalId + NationalId which does not exist but demogs match another record. New Master, New Person & Link to new Master + WARN to other master
		Person p2 = new Person().setDateOfBirth(d1).setSurname("EVANS").setGivenName("BOB").setNationalIdType("NHS").setNationalId("NHS2000003").setGender("1");
		p2.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p2.setLocalId("NSYS200002").setLocalIdType("MR").setOriginator("NSYS2");
		im.createOrUpdate(p2);

		assert(true);
		
	}

	@Test
	public void testNewWithNoNationalId() throws MpiException {

		UKRDCIndexManager im = new UKRDCIndexManager();
		
		// P1S - Set up NationalId for P2. New Person, New Master and new link to the master
		Person p1s = new Person().setDateOfBirth(d1).setSurname("WILLIAMS").setGivenName("JIM").setNationalIdType("NHS").setNationalId("NHS3000001").setGender("1");
		p1s.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1s.setLocalId("PRE3000001").setLocalIdType("MR").setOriginator("PRE3");
		im.createOrUpdate(p1s);

		// P1 - New + No NationalId No Matches to any master. New Person only
		Person p1 = new Person().setDateOfBirth(d1).setSurname("LORIMER").setGivenName("PETER").setGender("1");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId("NSYS300001").setLocalIdType("MR").setOriginator("NSYS3");
		im.createOrUpdate(p1);
		
		// P2 - New + No NationalId + Matches to existing master. New Person and work
		Person p2 = new Person().setDateOfBirth(d1).setSurname("WILLIAMS").setGivenName("JIM").setGender("1");
		p2.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p2.setLocalId("NSYS300002").setLocalIdType("MR").setOriginator("NSYS3");
		im.createOrUpdate(p2);

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
}
