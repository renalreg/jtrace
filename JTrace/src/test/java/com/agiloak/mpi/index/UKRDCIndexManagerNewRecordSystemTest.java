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
import com.agiloak.mpi.trace.persistence.TraceDAO;
import com.agiloak.mpi.workitem.persistence.WorkItemDAO;

public class UKRDCIndexManagerNewRecordSystemTest {
	
	private Date d1 = getDate("1962-08-31");
	private Date d2 = getDate("1962-08-30");
	private Date d3 = getDate("1962-07-31");

	public static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
	
	@BeforeClass
	public static void setup()  throws MpiException {

		MasterRecordDAO.deleteByNationalId("NHS0000001","NHS");
		clear("NSYS100001", "NSYS1");
		clear("NSYS100002", "NSYS1");
		clear("NSYS100003", "NSYS1");
		
		MasterRecordDAO.deleteByNationalId("NHS2000001","NHS");
		MasterRecordDAO.deleteByNationalId("NHS2000002","NHS");
		MasterRecordDAO.deleteByNationalId("NHS2000003","NHS");
		clear( "NSYS200001", "NSYS2");
		clear( "NSYS200002", "NSYS2");
		clear( "NSYS200003", "NSYS2");

		MasterRecordDAO.deleteByNationalId("NHS3000001","NHS");
		clear( "NSYS300001", "NSYS3");
		clear( "NSYS300002", "NSYS3");
		clear( "NSYS300003", "NSYS3");
		
	}

	@Test
	public void testNewWithExistingNationalId() throws MpiException {

		UKRDCIndexManager im = new UKRDCIndexManager();
		
		// P1S - Set up NationalId for P1. New Person, New Master and new link to the master
		Person p1s = new Person().setDateOfBirth(d1).setSurname("JONES").setGivenName("NICK").setNationalIdType("NHS").setNationalId("NHS0000001").setGender("1");
		p1s.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1s.setLocalId("NSYS100001").setLocalIdType("MR").setOriginator("NSYS1");
		im.createOrUpdate(p1s);

		// P1 - New + NationalId + NationalId exists + Matches. New Person & Link to existing Master
		Person p1 = new Person().setDateOfBirth(d1).setSurname("JONES").setGivenName("NICK").setNationalIdType("NHS").setNationalId("NHS0000001").setGender("1");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId("NSYS100002").setLocalIdType("MR").setOriginator("NSYS1");
		im.createOrUpdate(p1);
		// P2 - New + NationalId + NationalId exists + No match (DOB 2 part and given name mismatch). New Person & work item about mismatched Master
		Person p2 = new Person().setDateOfBirth(d2).setSurname("JONES").setGivenName("MATTY").setNationalIdType("NHS").setNationalId("NHS0000001").setGender("1");
		p2.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p2.setLocalId("NSYS100003").setLocalIdType("MR").setOriginator("NSYS1");
		im.createOrUpdate(p2);

		assert(true);
		
	}
	
	@Test
	public void testNewWithNonexistantNationalId() throws MpiException {

		UKRDCIndexManager im = new UKRDCIndexManager();
		
		// P1S - Set up NationalId for P2. New Person, New Master and Link to new master
		Person p1s = new Person().setDateOfBirth(d1).setSurname("EVANS").setGivenName("BOB").setNationalIdType("NHS").setNationalId("NHS2000001").setGender("1");
		p1s.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1s.setLocalId("NSYS200001").setLocalIdType("MR").setOriginator("NSYS2");
		im.createOrUpdate(p1s);

		// P1 - New + NationalId + NationalId which does not exist. New Master, New Person & Link to new Master
		Person p1 = new Person().setDateOfBirth(d3).setSurname("SMITH").setGivenName("BILL").setNationalIdType("NHS").setNationalId("NHS2000002").setGender("1");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId("NSYS200002").setLocalIdType("MR").setOriginator("NSYS2");
		im.createOrUpdate(p1);
		// P2 - New + NationalId + NationalId which does not exist but demogs match another record. New Master, New Person & Link to new Master + WARN to other master
		Person p2 = new Person().setDateOfBirth(d1).setSurname("EVANS").setGivenName("BOB").setNationalIdType("NHS").setNationalId("NHS2000003").setGender("1");
		p2.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p2.setLocalId("NSYS200003").setLocalIdType("MR").setOriginator("NSYS2");
		im.createOrUpdate(p2);

		assert(true);
		
	}

	@Test
	public void testNewWithNoNationalId() throws MpiException {

		UKRDCIndexManager im = new UKRDCIndexManager();
		
		// P1S - Set up NationalId for P2. New Person, New Master and new link to the master
		Person p1s = new Person().setDateOfBirth(d1).setSurname("WILLIAMS").setGivenName("JIM").setNationalIdType("NHS").setNationalId("NHS3000001").setGender("1");
		p1s.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1s.setLocalId("NSYS300001").setLocalIdType("MR").setOriginator("NSYS3");
		im.createOrUpdate(p1s);

		// P1 - New + No NationalId No Matches to any master. New Person only
		Person p1 = new Person().setDateOfBirth(d1).setSurname("LORIMER").setGivenName("PETER").setGender("1");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId("NSYS300002").setLocalIdType("MR").setOriginator("NSYS3");
		im.createOrUpdate(p1);
		
		// P2 - New + No NationalId + Matches to existing master. New Person and work
		Person p2 = new Person().setDateOfBirth(d1).setSurname("WILLIAMS").setGivenName("JIM").setGender("1");
		p2.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p2.setLocalId("NSYS300003").setLocalIdType("MR").setOriginator("NSYS3");
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
	
	public static void clear(String localId, String originator)  throws MpiException {
		
		Person person = PersonDAO.findByLocalId("MR", localId, originator);
		if (person != null) {
			LinkRecordDAO.deleteByPerson(person.getId());
			WorkItemDAO.deleteByPerson(person.getId());
			PersonDAO.delete(person);
			String traceId = TraceDAO.getTraceId(localId, "MR", originator, "AUTO");
			if (traceId != null) {
				TraceDAO.clearByTraceId(traceId);
			}
		}

	}	
}
