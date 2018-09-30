package com.agiloak.mpi.index;

import java.util.Date;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.SimpleConnectionManager;
import com.agiloak.mpi.audit.Audit;
import com.agiloak.mpi.audit.persistence.AuditDAO;
import com.agiloak.mpi.index.persistence.LinkRecordDAO;
import com.agiloak.mpi.index.persistence.MasterRecordDAO;
import com.agiloak.mpi.index.persistence.PersonDAO;
import com.agiloak.mpi.workitem.WorkItem;
import com.agiloak.mpi.workitem.persistence.WorkItemDAO;

public class UKRDCIndexManagerMainNationalIdSystemTest extends UKRDCIndexManagerBaseTest {
	
	private final static Logger logger = LoggerFactory.getLogger(UKRDCIndexManagerMainNationalIdSystemTest.class);

	private Date d1 = getDate("1962-08-31");
	private Date d2 = getDate("1962-08-30");

	@BeforeClass
	public static void setup()  throws MpiException {
		logger.debug("***************START OF SETUP****************");
		SimpleConnectionManager.configure("postgres", "postgres","localhost", "5432", "JTRACE");
		clear("LOCALHOSP", "ASYS100001", "ASYS1");
		clear("LOCALHOSP", "ASYS100002", "ASYS1");
		clear("LOCALHOSP", "ASYS100003", "ASYS1");
		clear("LOCALHOSP", "ASYS100004", "ASYS1");
		clear("LOCALHOSP", "ASYS100005", "ASYS1");
		clear("LOCALHOSP", "ASYS100006", "ASYS1");
		clear("LOCALHOSP", "ASYS100007", "ASYS1");
		clear("LOCALHOSP", "ASYS100008", "ASYS1");
		logger.debug("***************END OF SETUP****************");
	}

	@Test
	public void testAuditNHSId() throws MpiException {
		logger.debug("***************START TEST****************");

		String orig = "ASYS1";
		
		// Test-1 
		Person p1 = new Person().setDateOfBirth(d1).setSurname("JONES").setGivenName("NICHOLAS").setGender("1");
		p1.setLocalId("ASYS100001").setLocalIdType("LOCALHOSP").setOriginator(orig);
		p1.addNationalId(new NationalIdentity(NationalIdentity.NHS_TYPE,"NHSA100001"));
		NationalIdentity natId = store(p1);
		// Check that audit has been created with the NHS Number
		List<Audit> audits = AuditDAO.findByPerson(p1.getId());
		assert(audits.size()==1);
		for (Audit audit : audits) {
			assert(audit.getType()==Audit.NO_MATCH_ASSIGN_NEW);
			assert(audit.getMainNationalIdentity().getType().trim().equals(NationalIdentity.NHS_TYPE));
			assert(audit.getMainNationalIdentity().getId().equals("NHSA100001"));
		}
		
	}

	@Test
	public void testAuditCHIId() throws MpiException {
		logger.debug("***************START TEST****************");

		String orig = "ASYS1";
		
		Person p1 = new Person().setDateOfBirth(d2).setSurname("NICHOLAS").setGivenName("JONES").setGender("1");
		p1.setLocalId("ASYS100002").setLocalIdType("LOCALHOSP").setOriginator(orig);
		p1.addNationalId(new NationalIdentity(NationalIdentity.CHI_TYPE,"CHIA100002"));
		NationalIdentity natId = store(p1);
		// Check that audit has been created with the NHS Number
		List<Audit> audits = AuditDAO.findByPerson(p1.getId());
		assert(audits.size()==1);
		for (Audit audit : audits) {
			assert(audit.getType()==Audit.NO_MATCH_ASSIGN_NEW);
			assert(audit.getMainNationalIdentity().getType().trim().equals(NationalIdentity.CHI_TYPE));
			assert(audit.getMainNationalIdentity().getId().equals("CHIA100002"));
		}

	}
	
	@Test
	public void testAuditHSCId() throws MpiException {
		logger.debug("***************START TEST****************");

		String orig = "ASYS1";
		
		Person p1 = new Person().setDateOfBirth(d2).setSurname("NICHOLAS").setGivenName("JONES").setGender("1");
		p1.setLocalId("ASYS100003").setLocalIdType("LOCALHOSP").setOriginator(orig);
		p1.addNationalId(new NationalIdentity(NationalIdentity.HSC_TYPE,"HSCA100003"));
		NationalIdentity natId = store(p1);
		// Check that audit has been created with the NHS Number
		List<Audit> audits = AuditDAO.findByPerson(p1.getId());
		assert(audits.size()==1);
		for (Audit audit : audits) {
			assert(audit.getType()==Audit.NO_MATCH_ASSIGN_NEW);
			assert(audit.getMainNationalIdentity().getType().trim().equals(NationalIdentity.HSC_TYPE));
			assert(audit.getMainNationalIdentity().getId().equals("HSCA100003"));
		}
	}
	@Test
	public void testAuditRADARId() throws MpiException {
		logger.debug("***************START TEST****************");

		String orig = "ASYS1";
		
		Person p1 = new Person().setDateOfBirth(d2).setSurname("NICHOLAS").setGivenName("JONES").setGender("1");
		p1.setLocalId("ASYS100004").setLocalIdType("LOCALHOSP").setOriginator(orig);
		p1.addNationalId(new NationalIdentity(NationalIdentity.RADAR_TYPE,"RADA100004"));
		NationalIdentity natId = store(p1);
		// Check that audit has been created with the NHS Number
		List<Audit> audits = AuditDAO.findByPerson(p1.getId());
		assert(audits.size()==1);
		for (Audit audit : audits) {
			assert(audit.getType()==Audit.NO_MATCH_ASSIGN_NEW);
			assert(audit.getMainNationalIdentity().getType().trim().equals(NationalIdentity.RADAR_TYPE));
			assert(audit.getMainNationalIdentity().getId().equals("RADA100004"));
		}
	}
	@Test
	public void testAuditMultipleIds1() throws MpiException {
		logger.debug("***************START TEST****************");

		String orig = "ASYS1";
		
		Person p1 = new Person().setDateOfBirth(d2).setSurname("NICHOLAS").setGivenName("JONES").setGender("1");
		p1.setLocalId("ASYS100005").setLocalIdType("LOCALHOSP").setOriginator(orig);
		p1.addNationalId(new NationalIdentity(NationalIdentity.NHS_TYPE,"NHSA100005"));
		p1.addNationalId(new NationalIdentity(NationalIdentity.CHI_TYPE,"CHIA100005"));
		NationalIdentity natId = store(p1);
		// Check that audit has been created with the NHS Number
		List<Audit> audits = AuditDAO.findByPerson(p1.getId());
		assert(audits.size()==1);
		for (Audit audit : audits) {
			assert(audit.getType()==Audit.NO_MATCH_ASSIGN_NEW);
			assert(audit.getMainNationalIdentity().getType().trim().equals(NationalIdentity.NHS_TYPE));
			assert(audit.getMainNationalIdentity().getId().equals("NHSA100005"));
		}
	}
	@Test
	public void testAuditMultipleIds2() throws MpiException {
		logger.debug("***************START TEST****************");

		String orig = "ASYS1";
		
		Person p1 = new Person().setDateOfBirth(d2).setSurname("NICHOLAS").setGivenName("JONES").setGender("1");
		p1.setLocalId("ASYS100006").setLocalIdType("LOCALHOSP").setOriginator(orig);
		p1.addNationalId(new NationalIdentity(NationalIdentity.CHI_TYPE,"CHIA100006"));
		p1.addNationalId(new NationalIdentity(NationalIdentity.HSC_TYPE,"HSCA100006"));
		NationalIdentity natId = store(p1);
		// Check that audit has been created with the NHS Number
		List<Audit> audits = AuditDAO.findByPerson(p1.getId());
		assert(audits.size()==1);
		for (Audit audit : audits) {
			assert(audit.getType()==Audit.NO_MATCH_ASSIGN_NEW);
			assert(audit.getMainNationalIdentity().getType().trim().equals(NationalIdentity.CHI_TYPE));
			assert(audit.getMainNationalIdentity().getId().equals("CHIA100006"));
		}
	}
	@Test
	public void testAuditMultipleIds3() throws MpiException {
		logger.debug("***************START TEST****************");

		String orig = "ASYS1";
		
		Person p1 = new Person().setDateOfBirth(d2).setSurname("NICHOLAS").setGivenName("JONES").setGender("1");
		p1.setLocalId("ASYS100007").setLocalIdType("LOCALHOSP").setOriginator(orig);
		p1.addNationalId(new NationalIdentity(NationalIdentity.HSC_TYPE,"HSCA100007"));
		p1.addNationalId(new NationalIdentity(NationalIdentity.RADAR_TYPE,"RADA100007"));
		NationalIdentity natId = store(p1);
		// Check that audit has been created with the NHS Number
		List<Audit> audits = AuditDAO.findByPerson(p1.getId());
		assert(audits.size()==1);
		for (Audit audit : audits) {
			assert(audit.getType()==Audit.NO_MATCH_ASSIGN_NEW);
			assert(audit.getMainNationalIdentity().getType().trim().equals(NationalIdentity.HSC_TYPE));
			assert(audit.getMainNationalIdentity().getId().equals("HSCA100007"));
		}
	}
	@Test
	public void testAuditNoNationalId() throws MpiException {
		logger.debug("***************START TEST****************");

		String orig = "ASYS1";
		
		Person p1 = new Person().setDateOfBirth(d2).setSurname("NICHOLAS").setGivenName("JONES").setGender("1");
		p1.setLocalId("ASYS100008").setLocalIdType("LOCALHOSP").setOriginator(orig);
		NationalIdentity natId = store(p1);
		// Check that audit has been created with the NHS Number
		List<Audit> audits = AuditDAO.findByPerson(p1.getId());
		assert(audits.size()==1);
		for (Audit audit : audits) {
			assert(audit.getType()==Audit.NO_MATCH_ASSIGN_NEW);
			assert(audit.getMainNationalIdentity()==null);
		}
	}

}
