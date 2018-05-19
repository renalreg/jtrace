package com.agiloak.mpi.index;

import java.util.Date;
import java.util.List;

import org.junit.BeforeClass;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.SimpleConnectionManager;
import com.agiloak.mpi.index.persistence.LinkRecordDAO;
import com.agiloak.mpi.index.persistence.PersonDAO;
import com.agiloak.mpi.workitem.WorkItem;
import com.agiloak.mpi.workitem.persistence.WorkItemDAO;

public class UKRDCIndexManagerTraceSystemTest extends UKRDCIndexManagerBaseTest {
	
	private Date d1 = getDate("1990-03-31");

	@BeforeClass
	public static void setup()  throws MpiException {
		SimpleConnectionManager.configure("postgres", "postgres","localhost", "5432", "JTRACE");
		
		clear( "TUP1000001", "TRC");
		clear( "TUP1000002", "TRC");

	}
	
	// @Test - Trace function currently switched off
	public void testSimpleMatching() throws MpiException {

		// P1 - Setup a person
		Person p1 = new Person().setDateOfBirth(d1).setSurname("TRACER").setGivenName("TERRY").setGender("1");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId("TUP1000001").setLocalIdType("MR").setOriginator("TRC");
		store(p1);
		
		// P1 - update - should not match to itself
		p1.setId(0); // reset the id - incoming record won't have this.
		store(p1);

		// P2 - Setup another person with similar details but not good enough to trigger a match
		Person p2 = new Person().setDateOfBirth(d1).setSurname("TRACER").setGivenName("TERRY").setGender("1");
		p2.setPostcode("WK7 1AZ").setStreet("Townfield Lane");
		p2.setLocalId("TUP1000002").setLocalIdType("MR").setOriginator("TRC");
		store(p2);
		List<WorkItem> workItems = WorkItemDAO.findByPerson(p2.getId());
		assert(workItems.size()==0);
		List<LinkRecord> linkRecords = LinkRecordDAO.findByPerson(p2.getId());
		assert(linkRecords.size()==0);
		
		// P2 - change the details to force a match
		p2 = new Person().setDateOfBirth(d1).setSurname("TRACER").setGivenName("TERRY").setGender("1");
		p2.setPostcode("CH1 5AB").setStreet("Townfield Lane");
		p2.setLocalId("TUP1000002").setLocalIdType("MR").setOriginator("TRC");
		store(p2);
		List<WorkItem> workItems2 = WorkItemDAO.findByPerson(p2.getId());
		assert(workItems2.size()==1);
		List<LinkRecord> linkRecords2 = LinkRecordDAO.findByPerson(p2.getId());
		assert(linkRecords2.size()==0);
		
	}
	
	

	public static void clear(String localId, String localIdType)  throws MpiException {
		
		Person person = PersonDAO.findByLocalId("MR", localId, localIdType);
		if (person != null) {
			LinkRecordDAO.deleteByPerson(person.getId());
			WorkItemDAO.deleteByPerson(person.getId());
			PersonDAO.delete(person);
		}

	}	
}
