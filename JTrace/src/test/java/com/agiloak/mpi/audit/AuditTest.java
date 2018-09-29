package com.agiloak.mpi.audit;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.SimpleConnectionManager;
import com.agiloak.mpi.audit.persistence.AuditDAO;
import com.agiloak.mpi.index.NationalIdentity;
import com.agiloak.mpi.index.Person;
import com.agiloak.mpi.index.UKRDCIndexManager;
import com.agiloak.mpi.index.UKRDCIndexManagerBaseTest;
import com.agiloak.mpi.index.UKRDCIndexManagerResponse;

public class AuditTest {

	@Rule
	public final ExpectedException exception = ExpectedException.none();
	
	@BeforeClass
	public static void setup()  throws MpiException {
		SimpleConnectionManager.configure("postgres", "postgres","localhost", "5432", "JTRACE");
		AuditDAO.deleteByPerson(100001);
		AuditDAO.deleteByPerson(100002);
		AuditDAO.deleteByPerson(100003);
		AuditDAO.deleteByPerson(100004);
		AuditDAO.deleteByPerson(100005);
	}

	@Test
	public void testCreateAndRead1() throws MpiException {
		AuditManager am = new AuditManager();
				
		Audit audit = am.create(Audit.NO_MATCH_ASSIGN_NEW, 100001, 100001, "test");
		List<Audit> audits = AuditDAO.findByPerson(100001);
		assert(audits.size()==1);
		
		Audit audit2 = audits.get(0);
		assert(audit2.getPersonId()==audit.getPersonId());
		assert(audit2.getMasterId()==audit.getMasterId());
		assert(audit2.getType()==audit.getType());
		assert(audit2.getDescription().equals(audit.getDescription()));
		assert(audit2.getLastUpdated().compareTo(audit.getLastUpdated())==0);

	}
	@Test
	public void testCreateAndRead1WithAttributes() throws MpiException {
		AuditManager am = new AuditManager();
		Map<String,String> attr = new HashMap<String, String>();
		attr.put("TestK1", "TestV1");
		attr.put("TestK2", "TestV2");
		
		Audit audit = am.create(Audit.NO_MATCH_ASSIGN_NEW, 100002, 1, "test", attr);
		List<Audit> audits = AuditDAO.findByPerson(100002);
		assert(audits.size()==1);
		
		Audit audit2 = audits.get(0);
		assert(audit2.getPersonId()==audit.getPersonId());
		assert(audit2.getMasterId()==audit.getMasterId());
		assert(audit2.getType()==audit.getType());
		assert(audit2.getDescription().equals(audit.getDescription()));
		assert(audit2.getLastUpdated().compareTo(audit.getLastUpdated())==0);
		assert(audit2.getAttributes().size()==2);
		assert(audit2.getAttributes().get("TestK1").equals("TestV1"));
		assert(audit2.getAttributes().get("TestK2").equals("TestV2"));

	}

	@Test
	public void testCreateAndRead2() throws MpiException {
		AuditManager am = new AuditManager();
		
		Audit audit = am.create(Audit.NEW_MATCH_THROUGH_NATIONAL_ID, 100003, 1, "test");
		List<Audit> audits = AuditDAO.findByPerson(100003);
		assert(audits.size()==1);
		
		Audit audit2 = audits.get(0);
		assert(audit2.getPersonId()==audit.getPersonId());
		assert(audit2.getMasterId()==audit.getMasterId());
		assert(audit2.getType()==audit.getType());
		assert(audit2.getDescription().equals(audit.getDescription()));
		assert(audit2.getLastUpdated().compareTo(audit.getLastUpdated())==0);
	}

	@Test
	public void testCreateAndRead3() throws MpiException {
		AuditManager am = new AuditManager();
		
		Audit audit = am.create(Audit.UKRDC_MERGE, 100004, 1, "test");
		List<Audit> audits = AuditDAO.findByPerson(100004);
		assert(audits.size()==1);
		
		Audit audit2 = audits.get(0);
		assert(audit2.getPersonId()==audit.getPersonId());
		assert(audit2.getMasterId()==audit.getMasterId());
		assert(audit2.getType()==audit.getType());
		assert(audit2.getDescription().equals(audit.getDescription()));
		assert(audit2.getLastUpdated().compareTo(audit.getLastUpdated())==0);
	}

	@Test
	public void testCreateNoPerson() throws MpiException {
		AuditManager am = new AuditManager();
		exception.expect(MpiException.class);
		am.create(Audit.NO_MATCH_ASSIGN_NEW,0,1,"test");
	}

	@Test
	public void testCreateNoMaster() throws MpiException {
		AuditManager am = new AuditManager();
		exception.expect(MpiException.class);
		am.create(Audit.NO_MATCH_ASSIGN_NEW,1,0,"test");
	}
	@Test
	public void testCreateNullDesc() throws MpiException {
		AuditManager am = new AuditManager();
		exception.expect(MpiException.class);
		am.create(Audit.NO_MATCH_ASSIGN_NEW,1,2,null);
	}

	@Test
	public void testCreateEmptyDesc() throws MpiException {
		AuditManager am = new AuditManager();
		exception.expect(MpiException.class);
		am.create(Audit.NO_MATCH_ASSIGN_NEW,1,3,"");
	}
	
	@Test
	public void testDelete() throws MpiException {
		AuditManager am = new AuditManager();
		am.create(Audit.NEW_MATCH_THROUGH_NATIONAL_ID, 100005, 1, "test");
		am.create(Audit.NO_MATCH_ASSIGN_NEW, 100005, 2, "test2");
		List<Audit> audits = AuditDAO.findByPerson(100005);
		assert(audits.size()==2);
		
		AuditDAO.deleteByPerson(100005);
		audits = AuditDAO.findByPerson(100005);
		assert(audits.size()==0);
		
	}
	
}
