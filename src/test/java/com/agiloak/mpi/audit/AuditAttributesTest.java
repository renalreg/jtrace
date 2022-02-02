package com.agiloak.mpi.audit;

import java.util.HashMap;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.agiloak.mpi.MpiException;

public class AuditAttributesTest {

	@Rule
	public final ExpectedException exception = ExpectedException.none();
	
	@BeforeClass
	public static void setup()  throws MpiException {
		//SimpleConnectionManager.configure("postgres", "postgres","localhost", "5432", "JTRACE");
		//AuditDAO.deleteByPerson(1);
	}

	@Test
	public void testSimpleJsonify() throws MpiException {
		
		Map<String,String> attr = new HashMap<String, String>();
		attr.put("TestK1", "TestV1");
		attr.put("TestK2", "TestV2");
		Audit audit = new Audit(Audit.NO_MATCH_ASSIGN_NEW, 1, 1, "test",attr);
		assert(audit.getAttributesJson().length()>0);
	}
	@Test
	public void testHydration() throws MpiException {
		
		//SETUP
		Map<String,String> attr = new HashMap<String, String>();
		attr.put("TestK1", "TestV1");
		attr.put("TestK2", "TestV2");
		Audit audit = new Audit(Audit.NO_MATCH_ASSIGN_NEW, 1, 1, "test",attr);

		//TEST
		Audit audit2 = new Audit(Audit.NO_MATCH_ASSIGN_NEW, 1, 1, "test", null,null,audit.getAttributesJson());
		assert(audit2.getAttributes().size()==2);
	}
	
	@Test
	public void testNoAttributes() throws MpiException {
		
		Audit audit = new Audit(Audit.NO_MATCH_ASSIGN_NEW, 1, 1, "test", null);
		assert(audit.getAttributesJson().length()>0);
		Audit audit2 = new Audit(Audit.NO_MATCH_ASSIGN_NEW, 1, 1, "test", null, null, audit.getAttributesJson());
		assert(audit2.getAttributes().size()==0);
	}
}
