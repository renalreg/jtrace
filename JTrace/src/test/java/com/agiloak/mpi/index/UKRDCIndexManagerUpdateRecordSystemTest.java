package com.agiloak.mpi.index;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.index.persistence.LinkRecordDAO;
import com.agiloak.mpi.index.persistence.MasterRecordDAO;
import com.agiloak.mpi.index.persistence.PersonDAO;
import com.agiloak.mpi.trace.persistence.TraceDAO;
import com.agiloak.mpi.workitem.WorkItem;
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
		clear( "PUP5000002", "PUP5");
		clear( "PUP5000003", "PUP5");

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
		// VERIFY SETUP
		Person person = PersonDAO.findByLocalId(p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		MasterRecord master = MasterRecordDAO.findByNationalId("UHS5000001", "NHS");
		assert(master!=null);
		List<LinkRecord> links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master.getId());
		
		// TEST1 - update to remove the nationalId. Will DELINK and delete the master
		p1.setId(0); // reset the id - incoming record won't have this.
		p1.setNationalIdType(null).setNationalId(null);
		im.createOrUpdate(p1);
		// VERIFY
		person = PersonDAO.findByLocalId(p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		master = MasterRecordDAO.findByNationalId("UHS5000001", "NHS");
		assert(master==null);
		links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==0);

		// P2 - Setup a person with a national id and another linked person. Removing the NationalId from first patient should leave the other patient linked
		Person p2 = new Person().setDateOfBirth(d1).setSurname("FLINTOFF").setGivenName("FRED").setGender("1");
		p2.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p2.setLocalId("PUP5000002").setLocalIdType("MR").setOriginator("PUP5");
		p2.setNationalIdType("NHS").setNationalId("UHS5000002");
		im.createOrUpdate(p2);
		// VERIFY SETUP
		person = PersonDAO.findByLocalId(p2.getLocalIdType(), p2.getLocalId(), p2.getOriginator());
		assert(person!=null);
		master = MasterRecordDAO.findByNationalId("UHS5000002", "NHS");
		assert(master!=null);
		links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==1);
		assert(links.get(0).getPersonId()==p2.getId()); 
		List<WorkItem> items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==1);
		assert(items.get(0).getType()==WorkItem.TYPE_DEMOGS_NEAR_MATCH);
		
		// P3 - Link to National Id
		Person p3 = new Person().setDateOfBirth(d1).setSurname("FLINTOFF").setGivenName("FRED").setGender("1");
		p3.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p3.setLocalId("PUP5000003").setLocalIdType("MR").setOriginator("PUP5A");
		p3.setNationalIdType("NHS").setNationalId("UHS5000002");
		im.createOrUpdate(p3);
		
		// TEST2 - update to remove the nationalId. Will DELINK P2 but leave the master linked to P3. Raise a WORK because of the demog match with P3
		p2.setId(0); // reset the id - incoming record won't have this.
		p2.setNationalIdType(null).setNationalId(null);
		im.createOrUpdate(p2);
		// VERIFY
		person = PersonDAO.findByLocalId(p2.getLocalIdType(), p2.getLocalId(), p2.getOriginator());
		assert(person!=null);
		master = MasterRecordDAO.findByNationalId("UHS5000002", "NHS");
		assert(master!=null);
		links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==0);
		links = LinkRecordDAO.findByMaster(master.getId());
		assert(links.size()==1);
		assert(links.get(0).getPersonId()==p3.getId()); // STILL LINKED TO P3
		items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==3);
		assert(items.get(0).getType()==WorkItem.TYPE_DEMOGS_NEAR_MATCH); // FROM P2 SETUP
		assert(items.get(1).getType()==WorkItem.TYPE_DEMOGS_MATCH_OTHER_NATIONAL_ID);
		assert(items.get(2).getType()==WorkItem.TYPE_DEMOGS_NEAR_MATCH);
		
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
		
		// TEST1 - update no change and no DEMOG matches - no master/links/work created
		p1.setId(0); // reset the id - incoming record won't have this.
		im.createOrUpdate(p1);
		// VERIFY
		Person person = PersonDAO.findByLocalId(p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		List<LinkRecord> links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==0);
		List<WorkItem> items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==0);
		
		// P2 - set up a person with master and same demographics to create a demogs match and WORK ITEM
		Person p2 = new Person().setDateOfBirth(d1).setSurname("DAVIES").setGivenName("DEREK").setGender("1");
		p2.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p2.setLocalId("PUP4000002").setLocalIdType("MR").setOriginator("PUP4");
		p2.setNationalIdType("NHS").setNationalId("UHS4000001");
		im.createOrUpdate(p2);

		// TEST2 - now update p1 again and "find" demog match - work created
		p1.setId(0); // reset the id - incoming record won't have this.
		im.createOrUpdate(p1);
		// VERIFY
		person = PersonDAO.findByLocalId(p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		MasterRecord master = MasterRecordDAO.findByNationalId("UHS4000001", "NHS");
		assert(master!=null);
		links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==0);
		items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==2);
		assert(items.get(0).getType()==WorkItem.TYPE_DEMOGS_MATCH_OTHER_NATIONAL_ID);
		assert(items.get(1).getType()==WorkItem.TYPE_DEMOGS_NEAR_MATCH);

	}

	@Test
	public void testUpdateNatIdOnCurrentNotPrevious() throws MpiException {

		UKRDCIndexManager im = new UKRDCIndexManager();
		
		// P1 - Update with a master id that does not exist and has no demographic match
		// SETUP person with no national id
		Person p1 = new Person().setDateOfBirth(d3).setSurname("CLOWN").setGivenName("CAROL").setGender("2");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId("PUP3000001").setLocalIdType("MR").setOriginator("PUP3");
		im.createOrUpdate(p1);
		
		// TEST1 - add a National Id which doesn't exist. Add a master and a link. No similar demogs so no WORK
		p1.setId(0); // reset the id - incoming record won't have this.
		p1.setNationalIdType("NHS").setNationalId("UHS3000001");
		im.createOrUpdate(p1);
		// VERIFY
		Person person = PersonDAO.findByLocalId(p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		MasterRecord master = MasterRecordDAO.findByNationalId("UHS3000001", "NHS");
		assert(master!=null);
		List<LinkRecord> links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==1);
		List<WorkItem> items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==0);

		// P2 - Update with a master id that does not exist but has a demographic match
		// SETUP person with no national id - no master but raises a WORK due to demographic match
		Person p2 = new Person().setDateOfBirth(d3).setSurname("CLOWN").setGivenName("CAROL").setGender("2");
		p2.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p2.setLocalId("PUP3000002").setLocalIdType("MR").setOriginator("PUP3");
		im.createOrUpdate(p2);
		// VERIFY SETUP
		person = PersonDAO.findByLocalId(p2.getLocalIdType(), p2.getLocalId(), p2.getOriginator());
		assert(person!=null);
		links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==0);
		items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==2);
		assert(items.get(0).getType()==WorkItem.TYPE_DEMOGS_MATCH_OTHER_NATIONAL_ID);
		assert(items.get(1).getType()==WorkItem.TYPE_DEMOGS_NEAR_MATCH);
		
		// TEST2 - add a National Id which doesn't exist. Add a master and a link. Similar demogs exist so another WORK created
		p2.setId(0); // reset the id - incoming record won't have this.
		p2.setNationalIdType("NHS").setNationalId("UHS3000002");
		im.createOrUpdate(p2);
		// VERIFY
		person = PersonDAO.findByLocalId(p2.getLocalIdType(), p2.getLocalId(), p2.getOriginator());
		assert(person!=null);
		master = MasterRecordDAO.findByNationalId("UHS3000002", "NHS");
		assert(master!=null);
		links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==1);
		items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==4); // TODO: STOP DUPLICATE WORK ITEMS
		assert(items.get(0).getType()==WorkItem.TYPE_DEMOGS_MATCH_OTHER_NATIONAL_ID);
		assert(items.get(1).getType()==WorkItem.TYPE_DEMOGS_NEAR_MATCH);
		assert(items.get(2).getType()==WorkItem.TYPE_DEMOGS_MATCH_OTHER_NATIONAL_ID);
		assert(items.get(3).getType()==WorkItem.TYPE_DEMOGS_NEAR_MATCH);

		// P3 - Update with a master id that does exist and verifies
		// SETUP person with no national id - no master but raises 3 WORK items due to demographic match with UHS3000001 & UHS3000002 and Algorithmic Match
		Person p3 = new Person().setDateOfBirth(d3).setSurname("CLOWN").setGivenName("CAROL").setGender("2");
		p3.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p3.setLocalId("PUP3000003").setLocalIdType("MR").setOriginator("PUP3");
		im.createOrUpdate(p3);
		// VERIFY SETUP
		person = PersonDAO.findByLocalId(p3.getLocalIdType(), p3.getLocalId(), p3.getOriginator());
		assert(person!=null);
		master = MasterRecordDAO.findByNationalId("UHS3000002", "NHS");
		assert(master!=null);
		links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==0);
		items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==3);
		assert(items.get(0).getType()==WorkItem.TYPE_DEMOGS_MATCH_OTHER_NATIONAL_ID);  // TODO - Suppress duplicates
		assert(items.get(1).getType()==WorkItem.TYPE_DEMOGS_MATCH_OTHER_NATIONAL_ID);
		assert(items.get(2).getType()==WorkItem.TYPE_DEMOGS_NEAR_MATCH);
		
		// TEST3 add a National Id which does exist and verifies. Links to that master. Algorithmic WORK created and Demog Match WORK created to UHS3000001
		p3.setId(0); // reset the id - incoming record won't have this.
		p3.setNationalIdType("NHS").setNationalId("UHS3000002");
		im.createOrUpdate(p3);
		// VERIFY
		person = PersonDAO.findByLocalId(p3.getLocalIdType(), p3.getLocalId(), p3.getOriginator());
		assert(person!=null);
		master = MasterRecordDAO.findByNationalId("UHS3000002", "NHS");
		assert(master!=null);
		links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==1);
		items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==5);
		assert(items.get(0).getType()==WorkItem.TYPE_DEMOGS_MATCH_OTHER_NATIONAL_ID); // TODO - Suppress duplicates
		assert(items.get(1).getType()==WorkItem.TYPE_DEMOGS_MATCH_OTHER_NATIONAL_ID);
		assert(items.get(2).getType()==WorkItem.TYPE_DEMOGS_NEAR_MATCH);
		assert(items.get(3).getType()==WorkItem.TYPE_DEMOGS_MATCH_OTHER_NATIONAL_ID);
		assert(items.get(4).getType()==WorkItem.TYPE_DEMOGS_NEAR_MATCH);  
		
		// P4 - Update with a master id that does exist and does not verifies
		// SETUP person with no national id - no master and no MATCH WORK items, but a ALGO WORK ITEM as demographics are not an exact match
		Person p4 = new Person().setDateOfBirth(d4).setSurname("CLOWN").setGivenName("KAROL").setGender("2");
		p4.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p4.setLocalId("PUP3000004").setLocalIdType("MR").setOriginator("PUP3");
		im.createOrUpdate(p4);
		// VERIFY SETUP
		person = PersonDAO.findByLocalId(p4.getLocalIdType(), p4.getLocalId(), p4.getOriginator());
		assert(person!=null);
		links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==0);
		items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==1);
		assert(items.get(0).getType()==WorkItem.TYPE_DEMOGS_NEAR_MATCH);
		
		// TEST4 - add a National Id which does exist but does not verify. No link but a WORK item created for fail to match and another for the algorithmic match
		p4.setId(0); // reset the id - incoming record won't have this.
		p4.setNationalIdType("NHS").setNationalId("UHS3000002");
		im.createOrUpdate(p4);
		// VERIFY
		person = PersonDAO.findByLocalId(p4.getLocalIdType(), p4.getLocalId(), p4.getOriginator());
		assert(person!=null);
		links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==0);
		items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==3);
		assert(items.get(0).getType()==WorkItem.TYPE_DEMOGS_NEAR_MATCH);
		assert(items.get(1).getType()==WorkItem.TYPE_NOLINK_DEMOG_NOT_VERIFIED);
		assert(items.get(2).getType()==WorkItem.TYPE_DEMOGS_NEAR_MATCH); // TODO - Suppress duplicates
		
		assert(true);
	
	}

	@Test
	public void testUpdateNatIdOnCurrentAndPrevious() throws MpiException {

		UKRDCIndexManager im = new UKRDCIndexManager();
		
		//SETUP person with NationalId. New Person, New MAster and new link to the master
		Person p1 = new Person().setDateOfBirth(d1).setSurname("JONES").setGivenName("KAREN").setNationalIdType("NHS").setNationalId("UHS1000001").setGender("2");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId("PUP1000001").setLocalIdType("MR").setOriginator("PUP1");
		im.createOrUpdate(p1);
		
		// SETUP another record linked to the NationalId for P1. This should be delinked by later demographic update as only a partial DOB match and given name different. WORK for ALGO match
		Person p2 = new Person().setDateOfBirth(d1).setSurname("JONES").setGivenName("KAREN").setNationalIdType("NHS").setNationalId("UHS1000001").setGender("2");
		p2.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p2.setLocalId("PUP1000002").setLocalIdType("MR").setOriginator("PUP1");
		im.createOrUpdate(p2);
		// VERIFY SETUP
		MasterRecord master = MasterRecordDAO.findByNationalId("UHS1000001", "NHS");
		assert(master!=null);
		List<LinkRecord> links = LinkRecordDAO.findByPerson(p2.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master.getId());
		List<WorkItem> items = WorkItemDAO.findByPerson(p2.getId());
		assert(items.size()==1);
		assert(items.get(0).getType()==WorkItem.TYPE_DEMOGS_NEAR_MATCH); // TODO - Suppress as this is triggered by a linked record

		// SETUP another record linked to the NationalId for P1. This should NOT be delinked by later demographic update as the full DOB matches the update which will arrive in P1A
		Person p3 = new Person().setDateOfBirth(d2).setSurname("JONES").setGivenName("KAREN").setNationalIdType("NHS").setNationalId("UHS1000001").setGender("2");
		p3.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p3.setLocalId("PUP1000003").setLocalIdType("MR").setOriginator("PUP1");
		im.createOrUpdate(p3);
		// VERIFY SETUP
		master = MasterRecordDAO.findByNationalId("UHS1000001", "NHS");
		assert(master!=null);
		links = LinkRecordDAO.findByPerson(p3.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master.getId());
		items = WorkItemDAO.findByPerson(p3.getId());
		assert(items.size()==1);
		assert(items.get(0).getType()==WorkItem.TYPE_DEMOGS_NEAR_MATCH); // TODO - Suppress as this is triggered by a linked record

		// TEST1 - Update - National Id matches existing record. Demographics have not changed so DQ checks not executed. Simple update to the record.
		p1.setId(0); // reset the id - incoming record won't have this.
		im.createOrUpdate(p1);
		// VERIFY
		master = MasterRecordDAO.findByNationalId("UHS1000001", "NHS");
		assert(master!=null);
		links = LinkRecordDAO.findByPerson(p1.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master.getId());
		links = LinkRecordDAO.findByPerson(p2.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master.getId()); // P2 still linked
		links = LinkRecordDAO.findByPerson(p3.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master.getId()); // P3 still linked
		items = WorkItemDAO.findByPerson(p1.getId());
		assert(items.size()==0);
		
		// TEST2 - Update - National Id matches existing record. Demographics have changed. Update the record and the master record. SHOULD DELINK PUP1000002 but not PUP1000003
		//                  No WORK as Algorithmic match below threshold
		p1.setId(0); // reset the id - incoming record won't have this.
		p1.setDateOfBirth(d2).setGivenName("CAROL");
		im.createOrUpdate(p1);
		// VERIFY
		master = MasterRecordDAO.findByNationalId("UHS1000001", "NHS");
		assert(master!=null);
		links = LinkRecordDAO.findByPerson(p1.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master.getId());
		links = LinkRecordDAO.findByPerson(p2.getId());
		assert(links.size()==0); 							// P2 no longer linked
		links = LinkRecordDAO.findByPerson(p3.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master.getId()); // P3 still linked
		items = WorkItemDAO.findByPerson(p1.getId());
		assert(items.size()==0);
		
	}
	
	@Test
	public void testUpdateNatIdChange() throws MpiException {

		UKRDCIndexManager im = new UKRDCIndexManager();
		
		// SETUP Person and Master for update with no other links
		Person p1 = new Person().setDateOfBirth(d1).setSurname("ANDERSON").setGivenName("ALAN").setNationalIdType("NHS").setNationalId("UHS2000001").setGender("2");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId("PUP2000001").setLocalIdType("MR").setOriginator("PUP2");
		im.createOrUpdate(p1);
		// VERIFY SETUP
		MasterRecord master = MasterRecordDAO.findByNationalId("UHS2000001", "NHS");
		assert(master!=null);
		List<LinkRecord> links = LinkRecordDAO.findByPerson(p1.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master.getId());
		List<WorkItem> items = WorkItemDAO.findByPerson(p1.getId());
		assert(items.size()==0);

		// SETUP Person and Master for update which will have other links - master and other link should remain when this patient changes NHS Number
		Person p2 = new Person().setDateOfBirth(d2).setSurname("BEVAN").setGivenName("BOB").setNationalIdType("NHS").setNationalId("UHS2000002").setGender("2");
		p2.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p2.setLocalId("PUP2000002").setLocalIdType("MR").setOriginator("PUP2");
		im.createOrUpdate(p2);
		// VERIFY SETUP
		master = MasterRecordDAO.findByNationalId("UHS2000002", "NHS");
		assert(master!=null);
		links = LinkRecordDAO.findByPerson(p2.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master.getId());
		items = WorkItemDAO.findByPerson(p2.getId());
		assert(items.size()==0);

		// SETUP - 2nd Person to link to P1
		Person p3 = new Person().setDateOfBirth(d2).setSurname("BEVAN").setGivenName("BOB").setNationalIdType("NHS").setNationalId("UHS2000002").setGender("2");
		p3.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p3.setLocalId("PUP2000003").setLocalIdType("MR").setOriginator("PUP2");
		im.createOrUpdate(p3);
		// VERIFY SETUP
		master = MasterRecordDAO.findByNationalId("UHS2000002", "NHS");
		assert(master!=null);
		links = LinkRecordDAO.findByPerson(p3.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master.getId());
		items = WorkItemDAO.findByPerson(p3.getId());
		assert(items.size()==1);
		assert(items.get(0).getType()==WorkItem.TYPE_DEMOGS_NEAR_MATCH); // TODO - Suppress as this is triggered by a linked record

		// TEST1 - Change NHS Number. Should delink this person from the existing master and delete that master. Then add and link to a new master
		p1.setNationalIdType("NHS").setNationalId("UHS2000003");
		im.createOrUpdate(p1);
		// VERIFY
		master = MasterRecordDAO.findByNationalId("UHS2000001", "NHS");
		assert(master==null);
		master = MasterRecordDAO.findByNationalId("UHS2000003", "NHS");
		assert(master!=null);
		links = LinkRecordDAO.findByPerson(p1.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master.getId());
		items = WorkItemDAO.findByPerson(p1.getId());
		assert(items.size()==0);

		// TEST2 - Change NHS Number. Should delink this person from the existing master but not delete that master because of PUP2000003. Then add and link to a new master. Raise WORK due to same demographics
		p2.setNationalIdType("NHS").setNationalId("UHS2000004");
		im.createOrUpdate(p2);
		// VERIFY
		master = MasterRecordDAO.findByNationalId("UHS2000002", "NHS");
		assert(master!=null);
		master = MasterRecordDAO.findByNationalId("UHS2000004", "NHS");
		assert(master!=null);
		links = LinkRecordDAO.findByPerson(p2.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master.getId());
		items = WorkItemDAO.findByPerson(p2.getId());
		assert(items.size()==2);
		assert(items.get(0).getType()==WorkItem.TYPE_DEMOGS_MATCH_OTHER_NATIONAL_ID); 
		assert(items.get(1).getType()==WorkItem.TYPE_DEMOGS_NEAR_MATCH); // TODO - Suppress as this is triggered by a linked record
		
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
