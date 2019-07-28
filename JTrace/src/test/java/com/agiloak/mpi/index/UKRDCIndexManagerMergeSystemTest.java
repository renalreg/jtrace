package com.agiloak.mpi.index;

import java.util.Date;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.SimpleConnectionManager;
import com.agiloak.mpi.audit.Audit;
import com.agiloak.mpi.audit.AuditUtility;
import com.agiloak.mpi.audit.persistence.AuditDAO;
import com.agiloak.mpi.index.persistence.LinkRecordDAO;
import com.agiloak.mpi.index.persistence.MasterRecordDAO;
import com.agiloak.mpi.index.persistence.PersonDAO;
import com.agiloak.mpi.workitem.WorkItem;
import com.agiloak.mpi.workitem.persistence.WorkItemDAO;

public class UKRDCIndexManagerMergeSystemTest extends UKRDCIndexManagerBaseTest {
	
	private final static Logger logger = LoggerFactory.getLogger(UKRDCIndexManagerMergeSystemTest.class);

	private Date d1 = getDate("1962-08-31");
	private Date d2 = getDate("1962-08-30");


	@BeforeClass
	public static void setup()  throws MpiException {
		logger.debug("***************START OF SETUP****************");
		SimpleConnectionManager.configure("postgres", "postgres","localhost", "5432", "JTRACE");
		conn = SimpleConnectionManager.getDBConnection();
		// RESET audit.updatedby to the correct value before starting (just in case)
		AuditUtility.makeUpdatedByMandatory(conn, false);
		
		clear(conn, "LOCALHOSP", "MSYS100001", "MSYS1");
		clear(conn, "RADAR",     "MSYS100002", "MSYS1");
		clear(conn, "LOCALHOSP", "MSYS200001", "MSYS1");
		clear(conn, "RADAR",     "MSYS200002", "MSYS1");
		logger.debug("***************END OF SETUP****************");
	}

	@AfterClass
	public static void cleanup()  throws MpiException {
		// RESET audit.updatedby to the correct value after (just in case)
		AuditUtility.makeUpdatedByMandatory(conn, false);
	}

	@Test
	public void testMerge() throws MpiException {
		logger.debug("***************START TEST****************");

		String orig = "MSYS1";
		
		// Setup-1 
		Person p1 = new Person().setDateOfBirth(d1).setSurname("JONES").setGivenName("NICHOLAS").setGender("1");
		p1.setLocalId("MSYS100001").setLocalIdType("LOCALHOSP").setOriginator(orig);
		p1.addNationalId(new NationalIdentity(NationalIdentity.NHS_TYPE,"NHSM100001"));
		NationalIdentity natId1 = store(p1);
		
		// Setup-2 - similar record with same NHS Number but different
		Person p2 = new Person().setDateOfBirth(d2).setSurname("NICHOLAS").setGivenName("JONES").setGender("1");
		p2.setLocalId("MSYS100002").setLocalIdType("RADAR").setOriginator(orig);
		p2.addNationalId(new NationalIdentity(NationalIdentity.NHS_TYPE,"NHSM100001"));
		NationalIdentity natId2 = store(p2);

		// VERIFY
		assert(!natId1.getId().equals(natId2.getId()));
		assert(natId1.getType()==NationalIdentity.UKRDC_TYPE);
		assert(natId2.getType()==NationalIdentity.UKRDC_TYPE);

		Person person1 = PersonDAO.findByLocalId(conn, p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person1!=null);
		List<WorkItem> items1 = WorkItemDAO.findByPerson(conn, person1.getId());
		assert(items1.size()==0);
		MasterRecord mr1 = MasterRecordDAO.findByNationalId(conn, natId1.getId(), NationalIdentity.UKRDC_TYPE);
		assert(mr1!=null);

		Person person2 = PersonDAO.findByLocalId(conn, p2.getLocalIdType(), p2.getLocalId(), p2.getOriginator());
		assert(person2!=null);
		List<WorkItem> items2 = WorkItemDAO.findByPerson(conn, person2.getId());
		assert(items2.size()==2);
		MasterRecord mr2 = MasterRecordDAO.findByNationalId(conn, natId2.getId(), NationalIdentity.UKRDC_TYPE);
		assert(mr2!=null);

		UKRDCIndexManager im = new UKRDCIndexManager();
		UKRDCIndexManagerResponse mergeResp = im.merge(mr1.getId(), mr2.getId());
		assert(mergeResp.status==UKRDCIndexManagerResponse.SUCCESS);
		
		// Superceeded Master has been deleted
		MasterRecord mr2a = MasterRecordDAO.findByNationalId(conn, natId2.getId(), NationalIdentity.UKRDC_TYPE);
		assert(mr2a==null);
		
		// No more links to the superceeded master
		List<LinkRecord> links2a = LinkRecordDAO.findByMaster(conn, mr2.getId());
		assert(links2a.size()==0);
		
		// Check that audit has been created
		List<Audit> audits = AuditDAO.findByPerson(conn, p2.getId());
		assert(audits.size()==2);
		for (Audit audit : audits) {
			assert((audit.getType()==Audit.UKRDC_MERGE) || (audit.getType()==Audit.NO_MATCH_ASSIGN_NEW)  || (audit.getType()==Audit.WORK_ITEM_CREATED));
		}
		
	}
	@Test
	public void testMergeRollback() throws MpiException {
		logger.debug("***************START TEST****************");

		String orig = "MSYS1";
		
		// Setup-1 
		Person p1 = new Person().setDateOfBirth(d1).setSurname("JONES").setGivenName("NICHOLAS").setGender("1");
		p1.setLocalId("MSYS200001").setLocalIdType("LOCALHOSP").setOriginator(orig);
		p1.addNationalId(new NationalIdentity(NationalIdentity.NHS_TYPE,"NHSM200001"));
		NationalIdentity natId1 = store(p1);
		
		// Setup-2 - similar record with same NHS Number but different
		Person p2 = new Person().setDateOfBirth(d2).setSurname("NICHOLAS").setGivenName("JONES").setGender("1");
		p2.setLocalId("MSYS200002").setLocalIdType("RADAR").setOriginator(orig);
		p2.addNationalId(new NationalIdentity(NationalIdentity.NHS_TYPE,"NHSM200001"));
		NationalIdentity natId2 = store(p2);

		// VERIFY
		assert(!natId1.getId().equals(natId2.getId()));
		assert(natId1.getType()==NationalIdentity.UKRDC_TYPE);
		assert(natId2.getType()==NationalIdentity.UKRDC_TYPE);

		Person person1 = PersonDAO.findByLocalId(conn, p1.getLocalIdType(), p1.getLocalId(), p1.getOriginator());
		assert(person1!=null);
		List<WorkItem> items1 = WorkItemDAO.findByPerson(conn, person1.getId());
		assert(items1.size()==0);
		MasterRecord mr1 = MasterRecordDAO.findByNationalId(conn, natId1.getId(), NationalIdentity.UKRDC_TYPE);
		assert(mr1!=null);

		Person person2 = PersonDAO.findByLocalId(conn, p2.getLocalIdType(), p2.getLocalId(), p2.getOriginator());
		assert(person2!=null);
		List<WorkItem> items2 = WorkItemDAO.findByPerson(conn, person2.getId());
		assert(items2.size()==2);
		MasterRecord mr2 = MasterRecordDAO.findByNationalId(conn, natId2.getId(), NationalIdentity.UKRDC_TYPE);
		assert(mr2!=null);

		// Get audits pre-test
		List<Audit> auditsPre = AuditDAO.findByPerson(conn, p2.getId());

		UKRDCIndexManager im = new UKRDCIndexManager();
		AuditUtility.makeUpdatedByMandatory(conn, true);
		UKRDCIndexManagerResponse mergeResp = im.merge(mr1.getId(), mr2.getId());
		AuditUtility.makeUpdatedByMandatory(conn, false);
		assert(mergeResp.status==UKRDCIndexManagerResponse.FAIL);
		
		// Superceeded Master has been deleted
		MasterRecord mr2a = MasterRecordDAO.findByNationalId(conn, natId2.getId(), NationalIdentity.UKRDC_TYPE);
		assert(mr2a!=null);
		
		// No more links to the superceeded master
		List<LinkRecord> links2a = LinkRecordDAO.findByMaster(conn, mr2.getId());
		assert(links2a.size()!=0);
		
		// Check that audit has been created
		List<Audit> audits = AuditDAO.findByPerson(conn, p2.getId());
		assert(audits.size()==auditsPre.size());
		
	}

}
