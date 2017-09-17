package com.agiloak.mpi.index;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.index.persistence.LinkRecordDAO;
import com.agiloak.mpi.index.persistence.MasterRecordDAO;
import com.agiloak.mpi.index.persistence.PersonDAO;
import com.agiloak.mpi.trace.persistence.TraceDAO;
import com.agiloak.mpi.workitem.WorkItem;
import com.agiloak.mpi.workitem.persistence.WorkItemDAO;

public class UKRDCIndexManagerUpdateRecordSystemTest {

	private final static Logger logger = LoggerFactory.getLogger(UKRDCIndexManagerUpdateRecordSystemTest.class);
	
	private Date d1 = getDate("1970-01-29");
	private Date d2 = getDate("1970-01-28");

	private Date d3 = getDate("1980-06-01");
	private Date d4 = getDate("1980-06-06");

	public static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
	
	@BeforeClass
	public static void setup()  throws MpiException {
		
		//  Revised
		clear( "UPDT10001", "UPDT1");
		MasterRecordDAO.deleteByNationalId("UPDT2000R1",NationalIdentity.UKRR_TYPE);
		clear( "UPDT20001", "UPDT2");
		MasterRecordDAO.deleteByNationalId("UPDT3000R1",NationalIdentity.UKRR_TYPE);
		MasterRecordDAO.deleteByNationalId("UPDT3000R2",NationalIdentity.UKRR_TYPE);
		MasterRecordDAO.deleteByNationalId("UPDT3000R3",NationalIdentity.UKRR_TYPE);
		clear( "UPDT30001", "UPDT3");
		clear( "UPDT3A001", "UPDT3A");
		MasterRecordDAO.deleteByNationalId("UPDT4000R1",NationalIdentity.UKRR_TYPE);
		MasterRecordDAO.deleteByNationalId("UPDT4000N1",NationalIdentity.NHS_TYPE);
		MasterRecordDAO.deleteByNationalId("UPDT4000C1",NationalIdentity.CHI_TYPE);
		clear( "UPDT40001", "UPDT4");

	}

	@Test
	public void test1SimpleUpdateNoNationalIds() throws MpiException {
		
		logger.debug("************* test1SimpleUpdateNoNationalIds ****************");

		String originator = "UPDT1";
		String idBase = originator+"000";
		UKRDCIndexManager im = new UKRDCIndexManager();
		
		// P1 - Setup a person with no national ids
		Person p1 = new Person().setDateOfBirth(d1).setSurname("ANT").setGivenName("ADAM").setGender("1");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId(idBase+"1").setLocalIdType("MR").setOriginator(originator);
		im.createOrUpdate(p1);
		// VERIFY SETUP
		Person person = PersonDAO.findByLocalId(p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		List<LinkRecord> links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==0);

		logger.debug("************* test1SimpleUpdateNoNationalIds ********UT1-1********");
		// UT1-1 - Simple Update - change surname - still no national ids
		p1.setSurname("ANT2").setPostcode("CH1 6LB").setStreet("Townfield Lane");
		im.createOrUpdate(p1);
		// VERIFY SETUP
		person = PersonDAO.findByLocalId(p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==0);
		assert(person.getSurname().equals("ANT2"));
		assert(person.getPrevSurname().equals("ANT"));

	}

	@Test
	public void test2UpdateAddingUKRDCId() throws MpiException {

		logger.debug("************* test2UpdateAddingUKRDCId ****************");

		String originator = "UPDT2";
		String idBase = originator+"000";
		UKRDCIndexManager im = new UKRDCIndexManager();
		// P1 - Setup a person with no national ids
		Person p1 = new Person().setDateOfBirth(d1).setSurname("BOLD").setGivenName("BRENDA").setGender("2");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId(idBase+"1").setLocalIdType("MR").setOriginator(originator);
		im.createOrUpdate(p1);
		
		logger.debug("************* test2UpdateAddingUKRDCId ********UT2-1********");
		// UT2-1 - Add a UKRDC id
		p1.setId(0); // reset the id - incoming record won't have this.
		p1.setPrimaryIdType(NationalIdentity.UKRR_TYPE).setPrimaryId(idBase+"R1");
		im.createOrUpdate(p1);
		// VERIFY
		Person person = PersonDAO.findByLocalId(p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		MasterRecord master = MasterRecordDAO.findByNationalId(idBase+"R1",NationalIdentity.UKRR_TYPE);
		assert(master!=null);
		List<LinkRecord> links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master.getId());
		List<WorkItem> items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==0);

		logger.debug("************* test2UpdateAddingUKRDCId ********UT2-2********");
		// UT2-2 - No UKRDC on inbound, but already on record - just leave it there - this may be a common occurrence
		p1.setId(0); // reset the id - incoming record won't have this.
		p1.setPrimaryIdType(null).setPrimaryId(null);
		im.createOrUpdate(p1);
		// VERIFY
		person = PersonDAO.findByLocalId(p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		master = MasterRecordDAO.findByNationalId(idBase+"R1",NationalIdentity.UKRR_TYPE);
		assert(master!=null);
		links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master.getId());
		items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==0);

		logger.debug("************* test2UpdateAddingUKRDCId ********UT2-2A********");
		// UT2-2 - UKRDC on inbound matching that on on record 
		p1.setId(0); // reset the id - incoming record won't have this.
		p1.setPrimaryIdType(NationalIdentity.UKRR_TYPE).setPrimaryId(idBase+"R1");
		im.createOrUpdate(p1);
		// VERIFY
		person = PersonDAO.findByLocalId(p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		master = MasterRecordDAO.findByNationalId(idBase+"R1",NationalIdentity.UKRR_TYPE);
		assert(master!=null);
		links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master.getId());
		items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==0);

		logger.debug("************* test2UpdateAddingUKRDCId ********UT2-3********");
		// UT2-3 - As 2 but change demographics - later effective date (not set, so defaulted) should cause master update
		p1.setId(0); // reset the id - incoming record won't have this.
		p1.setPrimaryIdType(null).setPrimaryId(null);
		p1.setGivenName("BELINDA");
		im.createOrUpdate(p1);
		// VERIFY
		person = PersonDAO.findByLocalId(p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		master = MasterRecordDAO.findByNationalId(idBase+"R1",NationalIdentity.UKRR_TYPE);
		assert(master!=null);
		assert(master.getGivenName().equals("BELINDA"));
		assert(master.getEffectiveDate().compareTo(p1.getEffectiveDate())==0);
		links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master.getId());
		items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==0);

		logger.debug("************* test2UpdateAddingUKRDCId ********UT2-4********");
		// UT2-4 - As 2 but change demographics - earlier effective date - no update of Master. Match still verifies
		p1.setId(0); // reset the id - incoming record won't have this.
		p1.setPrimaryIdType(null).setPrimaryId(null);
		p1.setGivenName("BELINDER");
		p1.setEffectiveDate(getDate("2017-08-01"));
		im.createOrUpdate(p1);
		// VERIFY
		person = PersonDAO.findByLocalId(p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		master = MasterRecordDAO.findByNationalId(idBase+"R1",NationalIdentity.UKRR_TYPE);
		assert(master!=null);
		//Chcek that the master has not been updated
		assert(master.getGivenName().equals("BELINDA"));
		assert(master.getDateOfBirth().compareTo(d1)==0);
		links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master.getId());
		items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==0);

		logger.debug("************* test2UpdateAddingUKRDCId ********UT2-5********");
		// UT2-5 - As 4 change demographics - earlier effective date - no update of Master. But no longer verifies so work item raised and master marked for investigation
		p1.setId(0); // reset the id - incoming record won't have this.
		p1.setPrimaryIdType(null).setPrimaryId(null);
		p1.setGivenName("BELINDER");
		p1.setDateOfBirth(d4);
		p1.setEffectiveDate(getDate("2017-08-01"));
		im.createOrUpdate(p1);
		// VERIFY
		person = PersonDAO.findByLocalId(p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		master = MasterRecordDAO.findByNationalId(idBase+"R1",NationalIdentity.UKRR_TYPE);
		assert(master!=null);
		//Chcek that the master has not been updated
		assert(master.getGivenName().equals("BELINDA"));
		assert(master.getDateOfBirth().compareTo(d1)==0);
		links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master.getId());
		items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==1);
		assert(items.get(0).getType()==(WorkItem.TYPE_STALE_DEMOGS_NOT_VERIFIED_PRIMARY));

	}
	
	@Test
	public void test3UpdateChangingUKRDCId() throws MpiException {

		logger.debug("************* test3UpdateChangingUKRDCId ****************");

		String originator = "UPDT3";
		String originator2 = "UPDT3A";
		String idBase = originator+"000";
		String idBase2 = originator2+"00";
		UKRDCIndexManager im = new UKRDCIndexManager();
		// P1 - Setup a person with a UKRDC
		logger.debug("************* test3UpdateChangingUKRDCId ********SETUP-1********");
		Person p1 = new Person().setDateOfBirth(d1).setSurname("COLLINS").setGivenName("CHRIS").setGender("1");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId(idBase+"1").setLocalIdType("MR").setOriginator(originator);
		p1.setPrimaryIdType(NationalIdentity.UKRR_TYPE).setPrimaryId(idBase+"R1");
		im.createOrUpdate(p1);
		
		logger.debug("************* test3UpdateChangingUKRDCId ********UT3-1********");
		// UT3-1 - Change the UKRDC ID. Should delete the link to the previous and the original master whilst creating new links
		p1.setId(0); // reset the id - incoming record won't have this.
		p1.setPrimaryIdType(NationalIdentity.UKRR_TYPE).setPrimaryId(idBase+"R2");
		im.createOrUpdate(p1);
		// VERIFY
		Person person = PersonDAO.findByLocalId(p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		MasterRecord master = MasterRecordDAO.findByNationalId(idBase+"R1",NationalIdentity.UKRR_TYPE);
		assert(master==null);
		MasterRecord master2 = MasterRecordDAO.findByNationalId(idBase+"R2",NationalIdentity.UKRR_TYPE);
		assert(master2!=null);
		List<LinkRecord> links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master2.getId());
		List<WorkItem> items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==0);
		
		logger.debug("************* test3UpdateChangingUKRDCId ********SETUP-2********");
		// P2 - Setup another person linked to the same UKRDC
		Person p2 = new Person().setDateOfBirth(d1).setSurname("COLLINS").setGivenName("CHRIS").setGender("1");
		p2.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p2.setLocalId(idBase2+"1").setLocalIdType("MR").setOriginator(originator2);
		p2.setPrimaryIdType(NationalIdentity.UKRR_TYPE).setPrimaryId(idBase+"R2");
		im.createOrUpdate(p2);

		logger.debug("************* test3UpdateChangingUKRDCId ********UT3-2********");
		// UT3-2 - Change the UKRDC ID. Should delete the link to the previous. Original master stays in place though because p2 is linked to it
		p1.setId(0); // reset the id - incoming record won't have this.
		p1.setPrimaryIdType(NationalIdentity.UKRR_TYPE).setPrimaryId(idBase+"R3");
		im.createOrUpdate(p1);
		// VERIFY
		person = PersonDAO.findByLocalId(p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		master = MasterRecordDAO.findByNationalId(idBase+"R2",NationalIdentity.UKRR_TYPE);
		assert(master!=null);
		master2 = MasterRecordDAO.findByNationalId(idBase+"R3",NationalIdentity.UKRR_TYPE);
		assert(master2!=null);
		links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==1);
		assert(links.get(0).getMasterId()==master2.getId());
		items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==0);
	}
	
	@Test
	public void test4AddingOtherNationalId() throws MpiException {

		logger.debug("************* test4AddingOtherNationalId ****************");

		String originator = "UPDT4";
		String idBase = originator+"000";

		UKRDCIndexManager im = new UKRDCIndexManager();
		// P1 - Setup a person with a UKRDC
		logger.debug("************* test4AddingOtherNationalId ********SETUP-1********");
		Person p1 = new Person().setDateOfBirth(d1).setSurname("DERBY").setGivenName("DORIS").setGender("2");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId(idBase+"1").setLocalIdType("MR").setOriginator(originator);
		p1.setPrimaryIdType(NationalIdentity.UKRR_TYPE).setPrimaryId(idBase+"R1");
		im.createOrUpdate(p1);
		
		logger.debug("************* test4AddingOtherNationalId ********UT4-1********");
		// UT4-1 - Add NHS Number and CHI Number
		p1.setId(0); // reset the id - incoming record won't have this.
		p1.addNationalId(new NationalIdentity(NationalIdentity.NHS_TYPE,idBase+"N1"));
		p1.addNationalId(new NationalIdentity(NationalIdentity.CHI_TYPE,idBase+"C1"));
		im.createOrUpdate(p1);
		// VERIFY
		Person person = PersonDAO.findByLocalId(p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		MasterRecord masterUkrdc = MasterRecordDAO.findByNationalId(idBase+"R1",NationalIdentity.UKRR_TYPE);
		assert(masterUkrdc!=null);
		MasterRecord masterNhs = MasterRecordDAO.findByNationalId(idBase+"N1",NationalIdentity.NHS_TYPE);
		assert(masterNhs!=null);
		MasterRecord masterChi = MasterRecordDAO.findByNationalId(idBase+"C1",NationalIdentity.CHI_TYPE);
		assert(masterChi!=null);

		List<LinkRecord> links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==3);
		assert(links.get(0).getMasterId()==masterUkrdc.getId());
		assert(links.get(1).getMasterId()==masterNhs.getId());
		assert(links.get(2).getMasterId()==masterChi.getId());
		List<WorkItem> items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==0);
		
		logger.debug("************* test4AddingOtherNationalId ********UT4-2********");
		// UT4-2 - Remove the CHI and add a new HSI
		p1.setId(0); // reset the id - incoming record won't have this.
		p1.setNationalIds(new ArrayList<NationalIdentity>());
		p1.addNationalId(new NationalIdentity(NationalIdentity.NHS_TYPE,idBase+"N1"));
		p1.addNationalId(new NationalIdentity(NationalIdentity.HSC_TYPE,idBase+"H1"));
		im.createOrUpdate(p1);
		// VERIFY
		person = PersonDAO.findByLocalId(p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		masterUkrdc = MasterRecordDAO.findByNationalId(idBase+"R1",NationalIdentity.UKRR_TYPE);
		assert(masterUkrdc!=null);
		masterNhs = MasterRecordDAO.findByNationalId(idBase+"N1",NationalIdentity.NHS_TYPE);
		assert(masterNhs!=null);
		masterChi = MasterRecordDAO.findByNationalId(idBase+"C1",NationalIdentity.CHI_TYPE);
		assert(masterChi==null);
		MasterRecord masterHsc = MasterRecordDAO.findByNationalId(idBase+"H1",NationalIdentity.HSC_TYPE);
		assert(masterHsc!=null);

		links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==3);
		assert(links.get(0).getMasterId()==masterUkrdc.getId());
		assert(links.get(1).getMasterId()==masterNhs.getId());
		assert(links.get(2).getMasterId()==masterHsc.getId());
		items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==0);

		logger.debug("************* test4AddingOtherNationalId ********UT4-3********");
		// UT4-3 - Remove the CHI and add a new HSI
		p1.setId(0); // reset the id - incoming record won't have this.
		p1.setSurname("DARBY");
		im.createOrUpdate(p1);
		// VERIFY
		person = PersonDAO.findByLocalId(p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		assert(person.getSurname().equals("DARBY"));
		masterUkrdc = MasterRecordDAO.findByNationalId(idBase+"R1",NationalIdentity.UKRR_TYPE);
		assert(masterUkrdc!=null);
		assert(masterUkrdc.getSurname().equals("DARBY"));
		masterNhs = MasterRecordDAO.findByNationalId(idBase+"N1",NationalIdentity.NHS_TYPE);
		assert(masterNhs!=null);
		assert(masterNhs.getSurname().equals("DARBY"));
		masterChi = MasterRecordDAO.findByNationalId(idBase+"C1",NationalIdentity.CHI_TYPE);
		assert(masterChi==null);
		masterHsc = MasterRecordDAO.findByNationalId(idBase+"H1",NationalIdentity.HSC_TYPE);
		assert(masterHsc!=null);
		assert(masterHsc.getSurname().equals("DARBY"));

		links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==3);
		assert(links.get(0).getMasterId()==masterUkrdc.getId());
		assert(links.get(1).getMasterId()==masterNhs.getId());
		assert(links.get(2).getMasterId()==masterHsc.getId());
		items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==0);		

		logger.debug("************* test4AddingOtherNationalId ********UT4-4********");
		// UT4-4 - Update the surname - but set an old effective date. Master records should not be updated. Work Items not created as data still verifies
		p1.setId(0); // reset the id - incoming record won't have this.
		p1.setSurname("DARBEY");
		p1.setEffectiveDate(getDate("2017-08-01"));
		im.createOrUpdate(p1);
		// VERIFY
		person = PersonDAO.findByLocalId(p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person!=null);
		assert(person.getSurname().equals("DARBEY"));
		masterUkrdc = MasterRecordDAO.findByNationalId(idBase+"R1",NationalIdentity.UKRR_TYPE);
		assert(masterUkrdc!=null);
		assert(masterUkrdc.getSurname().equals("DARBY"));
		masterNhs = MasterRecordDAO.findByNationalId(idBase+"N1",NationalIdentity.NHS_TYPE);
		assert(masterNhs!=null);
		assert(masterNhs.getSurname().equals("DARBY"));
		masterChi = MasterRecordDAO.findByNationalId(idBase+"C1",NationalIdentity.CHI_TYPE);
		assert(masterChi==null);
		masterHsc = MasterRecordDAO.findByNationalId(idBase+"H1",NationalIdentity.HSC_TYPE);
		assert(masterHsc!=null);
		assert(masterHsc.getSurname().equals("DARBY"));

		links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==3);
		assert(links.get(0).getMasterId()==masterUkrdc.getId());
		assert(links.get(1).getMasterId()==masterNhs.getId());
		assert(links.get(2).getMasterId()==masterHsc.getId());
		items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==0);		

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
