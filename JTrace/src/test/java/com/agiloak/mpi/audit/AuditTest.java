package com.agiloak.mpi.audit;

import java.util.List;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.SimpleConnectionManager;
import com.agiloak.mpi.audit.persistence.AuditDAO;

public class AuditTest {

	@Rule
	public final ExpectedException exception = ExpectedException.none();
	
	@BeforeClass
	public static void setup()  throws MpiException {
		SimpleConnectionManager.configure("postgres", "postgres","localhost", "5432", "JTRACE");
		AuditDAO.deleteByPerson(1);
		AuditDAO.deleteByPerson(2);
		AuditDAO.deleteByPerson(3);
		AuditDAO.deleteByPerson(4);
	}

	@Test
	public void testCreateAndRead1() throws MpiException {
		AuditManager am = new AuditManager();
		
		Audit audit = am.create(Audit.NO_MATCH_ASSIGN_NEW, 1, 1, "test");
		List<Audit> audits = AuditDAO.findByPerson(1);
		assert(audits.size()==1);
		
		Audit audit2 = audits.get(0);
		assert(audit2.getPersonId()==audit.getPersonId());
		assert(audit2.getMasterId()==audit.getMasterId());
		assert(audit2.getType()==audit.getType());
		assert(audit2.getDescription().equals(audit.getDescription()));
		assert(audit2.getLastUpdated().compareTo(audit.getLastUpdated())==0);

	}

	@Test
	public void testCreateAndRead2() throws MpiException {
		AuditManager am = new AuditManager();
		
		Audit audit = am.create(Audit.NEW_MATCH_THROUGH_NATIONAL_ID, 2, 1, "test");
		List<Audit> audits = AuditDAO.findByPerson(2);
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
		
		Audit audit = am.create(Audit.UKRDC_MERGE, 4, 1, "test");
		List<Audit> audits = AuditDAO.findByPerson(4);
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
		am.create(Audit.NEW_MATCH_THROUGH_NATIONAL_ID, 3, 1, "test");
		am.create(Audit.NO_MATCH_ASSIGN_NEW, 3, 2, "test2");
		List<Audit> audits = AuditDAO.findByPerson(3);
		assert(audits.size()==2);
		
		AuditDAO.deleteByPerson(3);
		audits = AuditDAO.findByPerson(3);
		assert(audits.size()==0);
		
	}
	
}
