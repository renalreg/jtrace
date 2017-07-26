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

public class UKRDCIndexManagerUpdateRecordSystemTest {
	
	private Date d1 = getDate("1970-01-29");
	private Date d2 = getDate("1970-01-28");

	private Date d3 = getDate("1980-06-01");
	private Date d4 = getDate("1980-06-06");

	public static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
	
	@BeforeClass
	public static void setup()  throws MpiException {
		
		MasterRecordDAO.deleteByNationalId("UHS1000001","NHS");
		clear( "PUP1000001", "PUP1");
		clear( "PUP1000002", "PUP1");
		clear( "PUP1000003", "PUP1");

		MasterRecordDAO.deleteByNationalId("UHS2000001","NHS");
		MasterRecordDAO.deleteByNationalId("UHS2000002","NHS");
		MasterRecordDAO.deleteByNationalId("UHS2000003","NHS");
		MasterRecordDAO.deleteByNationalId("UHS2000004","NHS");
		clear( "PUP2000001", "PUP2");
		clear( "PUP2000002", "PUP2");
		clear( "PUP2000003", "PUP2");

		MasterRecordDAO.deleteByNationalId("UHS3000001","NHS");
		MasterRecordDAO.deleteByNationalId("UHS3000002","NHS");
		clear( "PUP3000001", "PUP3");
		clear( "PUP3000002", "PUP3");
		clear( "PUP3000003", "PUP3");
		clear( "PUP3000004", "PUP3");

		MasterRecordDAO.deleteByNationalId("UHS4000001","NHS");
		clear( "PUP4000001", "PUP4");
		clear( "PUP4000002", "PUP4");

		MasterRecordDAO.deleteByNationalId("UHS5000001","NHS");
		clear( "PUP5000001", "PUP5");

	}
	
	@Test
	public void testUpdateNatIdOnPreviousNotCurrent() throws MpiException {

		UKRDCIndexManager im = new UKRDCIndexManager();
		
		// P1 - Setup a person with a national id
		Person p1 = new Person().setDateOfBirth(d1).setSurname("ERIKSON").setGivenName("ERICA").setGender("2");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId("PUP5000001").setLocalIdType("MR").setOriginator("PUP5");
		p1.setNationalIdType("NHS").setNationalId("UHS5000001");
		im.createOrUpdate(p1);
		
		// P1 - update to remove the nationalId. Will DELINK and delete the master
		p1.setNationalIdType(null).setNationalId(null);
		im.createOrUpdate(p1);

		// P2 - Setup a person with a national id and another linked person. Removing the NationalId from first patient should leave the other patient linked
		Person p2 = new Person().setDateOfBirth(d1).setSurname("FLINTOFF").setGivenName("FRED").setGender("1");
		p2.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p2.setLocalId("PUP5000002").setLocalIdType("MR").setOriginator("PUP5");
		p2.setNationalIdType("NHS").setNationalId("UHS5000002");
		im.createOrUpdate(p2);
		
		// P3 - Link to National Id
		Person p3 = new Person().setDateOfBirth(d1).setSurname("FLINTOFF").setGivenName("FRED").setGender("1");
		p3.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p3.setLocalId("PUP5000003").setLocalIdType("MR").setOriginator("PUP5A");
		p3.setNationalIdType("NHS").setNationalId("UHS5000002");
		im.createOrUpdate(p3);
		
		// P2 - update to remove the nationalId. Will DELINK P2 but leave the master linked to P3. Raise a WORK because of the demog match with P3
		p2.setNationalIdType(null).setNationalId(null);
		im.createOrUpdate(p2);
		
	}
	
	@Test
	public void testUpdateNatIdNotOnCurrentOrPrevious() throws MpiException {

		UKRDCIndexManager im = new UKRDCIndexManager();
		// P1 - Update with a master id that does not exist and has no demographic match
		// P1 - Set up person with no national id
		Person p1 = new Person().setDateOfBirth(d1).setSurname("DAVIES").setGivenName("DEREK").setGender("1");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId("PUP4000001").setLocalIdType("MR").setOriginator("PUP4");
		im.createOrUpdate(p1);
		
		// P1 - update no change and no DEMOG matches - no master/links/work created
		im.createOrUpdate(p1);
		
		// P2 - set up a person with master and same demographics to create a demogs match and WORK ITEM
		Person p2 = new Person().setDateOfBirth(d1).setSurname("DAVIES").setGivenName("DEREK").setGender("1");
		p2.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p2.setLocalId("PUP4000002").setLocalIdType("MR").setOriginator("PUP4");
		p2.setNationalIdType("NHS").setNationalId("UHS4000001");
		im.createOrUpdate(p2);

		// P1 - now update again and create demog match - work created
		im.createOrUpdate(p1);

	}

	@Test
	public void testUpdateNatIdOnCurrentNotPrevious() throws MpiException {

		UKRDCIndexManager im = new UKRDCIndexManager();
		
		// P1 - Update with a master id that does not exist and has no demographic match
		// P1 - Set up person with no national id
		Person p1 = new Person().setDateOfBirth(d3).setSurname("CLOWN").setGivenName("CAROL").setGender("2");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId("PUP3000001").setLocalIdType("MR").setOriginator("PUP3");
		im.createOrUpdate(p1);
		
		// P1 - add a National Id which doesn't exist. Add a master and a link. No similar demogs so no WORK
		p1.setNationalIdType("NHS").setNationalId("UHS3000001");
		im.createOrUpdate(p1);

		// P2 - Update with a master id that does not exist but has a demographic match
		// P2 - Set up person with no national id - no master but raises a WORK due to demographic match
		Person p2 = new Person().setDateOfBirth(d3).setSurname("CLOWN").setGivenName("CAROL").setGender("2");
		p2.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p2.setLocalId("PUP3000002").setLocalIdType("MR").setOriginator("PUP3");
		im.createOrUpdate(p2);
		
		// P2 - add a National Id which doesn't exist. Add a master and a link. Similar demogs exist so another WORK created
		p2.setNationalIdType("NHS").setNationalId("UHS3000002");
		im.createOrUpdate(p2);

		// P3 - Update with a master id that does exist and verifies
		// P3 - Set up person with no national id - no master but raises 2 WORK items due to demographic match with UHS3000001 and UHS3000002
		Person p3 = new Person().setDateOfBirth(d3).setSurname("CLOWN").setGivenName("CAROL").setGender("2");
		p3.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p3.setLocalId("PUP3000003").setLocalIdType("MR").setOriginator("PUP3");
		im.createOrUpdate(p3);
		
		// P3 - add a National Id which does exist and verifies. Links to that master. No WORK created
		p3.setNationalIdType("NHS").setNationalId("UHS3000002");
		im.createOrUpdate(p3);
		
		// P4 - Update with a master id that does exist and does not verifies
		// P4 - Set up person with no national id - no master and no WORK as demographics are not an exact match
		Person p4 = new Person().setDateOfBirth(d4).setSurname("CLOWN").setGivenName("KAROL").setGender("2");
		p4.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p4.setLocalId("PUP3000004").setLocalIdType("MR").setOriginator("PUP3");
		im.createOrUpdate(p4);
		
		// P4 - add a National Id which does exist but does not verify. No link but a WORK item created
		p4.setNationalIdType("NHS").setNationalId("UHS3000002");
		im.createOrUpdate(p4);
		
		assert(true);
	
	}

	@Test
	public void testUpdateNatIdOnCurrentAndPrevious() throws MpiException {

		UKRDCIndexManager im = new UKRDCIndexManager();
		
		// P1 - Set up NationalId for P1. New Person, New MAster and new link to the master
		Person p1 = new Person().setDateOfBirth(d1).setSurname("JONES").setGivenName("KAREN").setNationalIdType("NHS").setNationalId("UHS1000001").setGender("2");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId("PUP1000001").setLocalIdType("MR").setOriginator("PUP1");
		im.createOrUpdate(p1);
		
		// P1S2 - Set up another record linked to the NationalId for P1. This should be delinked by later demographic update as only a partial DOB match and given name different
		Person p1s2 = new Person().setDateOfBirth(d1).setSurname("JONES").setGivenName("KAREN").setNationalIdType("NHS").setNationalId("UHS1000001").setGender("2");
		p1s2.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1s2.setLocalId("PUP1000002").setLocalIdType("MR").setOriginator("PUP1");
		im.createOrUpdate(p1s2);

		// P1S3 - Set up another record linked to the NationalId for P1. This should NOT be delinked by later demographic update as the full DOB matches the update which will arrive in P1A
		Person p1s3 = new Person().setDateOfBirth(d2).setSurname("JONES").setGivenName("KAREN").setNationalIdType("NHS").setNationalId("UHS1000001").setGender("2");
		p1s3.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1s3.setLocalId("PUP1000003").setLocalIdType("MR").setOriginator("PUP1");
		im.createOrUpdate(p1s3);

		// P1 - Update - National Id matches existing record. Demographics have not changed. Simple update to the record
		im.createOrUpdate(p1);
		
		// P1A - Update - National Id matches existing record. Demographics have changed. Update the record and the master record. SHOULD DELINK PUP1000001 but not PUP1000002
		p1.setDateOfBirth(d2).setGivenName("CAROL");
		im.createOrUpdate(p1);

		assert(true);
	
	}
	
	@Test
	public void testUpdateNatIdChange() throws MpiException {

		UKRDCIndexManager im = new UKRDCIndexManager();
		
		// P1S1 - Setup Person and Master for update with no other links
		Person p1s1 = new Person().setDateOfBirth(d1).setSurname("ANDERSON").setGivenName("ALAN").setNationalIdType("NHS").setNationalId("UHS2000001").setGender("2");
		p1s1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1s1.setLocalId("PUP2000001").setLocalIdType("MR").setOriginator("PUP2");
		im.createOrUpdate(p1s1);

		// P1S2 - Setup Person and Master for update which will have other links - master and other link should remain when this patient changes NHS Number
		Person p1s2 = new Person().setDateOfBirth(d2).setSurname("BEVAN").setGivenName("BOB").setNationalIdType("NHS").setNationalId("UHS2000002").setGender("2");
		p1s2.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1s2.setLocalId("PUP2000002").setLocalIdType("MR").setOriginator("PUP2");
		im.createOrUpdate(p1s2);

		// P1S2A - Setup 2nd Person to link to P1S2
		Person p1s2a = new Person().setDateOfBirth(d2).setSurname("BEVAN").setGivenName("BOB").setNationalIdType("NHS").setNationalId("UHS2000002").setGender("2");
		p1s2a.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1s2a.setLocalId("PUP2000003").setLocalIdType("MR").setOriginator("PUP2");
		im.createOrUpdate(p1s2a);

		// P1S1 - Change NHS Number. Should delink this person from the existing master and delete that master. Then add and link to a new master
		p1s1.setNationalIdType("NHS").setNationalId("UHS2000003");
		im.createOrUpdate(p1s1);

		// P1S2 - Change NHS Number. Should delink this person from the existing master but not delete that master because of PUP2000003. Then add and link to a new master. Raise WORK due to same demographics
		p1s2.setNationalIdType("NHS").setNationalId("UHS2000004");
		im.createOrUpdate(p1s2);
		
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
