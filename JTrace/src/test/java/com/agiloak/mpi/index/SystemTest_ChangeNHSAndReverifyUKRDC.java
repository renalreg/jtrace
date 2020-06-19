package com.agiloak.mpi.index;

import java.util.ArrayList;
import java.util.Date;

import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.SimpleConnectionManager;
import com.agiloak.mpi.index.persistence.PersonDAO;

public class SystemTest_ChangeNHSAndReverifyUKRDC extends UKRDCIndexManagerBaseTest {

	private final static String testSuite = "SAA";

	private final static Logger logger = LoggerFactory.getLogger(SystemTest_ChangeNHSAndReverifyUKRDC.class);
	
	private Date d1 = getDate("1970-01-29");
	@BeforeClass
	public static void setup()  throws MpiException {
		
		SimpleConnectionManager.configure("postgres", "postgres","localhost", "5432", "JTRACE");
		conn = SimpleConnectionManager.getDBConnection();
		clearAll(conn);

	}

	// P1 - Linked to NHS 1, will create UKRDC 1
	// P1 - Linked to NHS 2, will drop link to NHS 2. UKRDC not changed
	@Test
	public void test1ChangeNHS() throws MpiException {
		String testId = "1";
		
		logger.debug("************* test1ChangeNHSAndReverifyUKRDC ****************");

		String originator = testSuite+testId;
		String idBase = originator+"000";
		String nhsBase = "NH"+idBase;
		String LOCAL1 = idBase+"1";
		NationalIdentity NHS1 = new NationalIdentity(NationalIdentity.NHS_TYPE, nhsBase+"1");
		NationalIdentity NHS2 = new NationalIdentity(NationalIdentity.NHS_TYPE, nhsBase+"2");
		
		Person p1 = createPerson(LOCAL1, originator, d1, "ANT", "ADAM", "1", NHS1);
		NationalIdentity ukrdc1 = store(p1);

		// VERIFY SETUP
		assert(ukrdc1 != null);
		
		// UPDATE NHS Number
		p1.setNationalIds(new ArrayList<NationalIdentity>());
		p1.addNationalId(NHS2);
		NationalIdentity ukrdc1_A = store(p1);
		
		// VERIFY UPDATE - UKRDC
		assert(ukrdc1_A != null);
		assert(ukrdc1.equals(ukrdc1_A));

		// VERIFY WORKITEMS AND AUDIT? - TODO

		// VERIFY UPDATE - NHS
		Person person1 = PersonDAO.findByLocalId(conn, MR, LOCAL1, p1.getOriginator());
		assert(getNationalIdentity(conn, person1, NationalIdentity.NHS_TYPE).equals(NHS2));
		assert(!isPersonLinkedToMaster(conn, person1, NHS1));
		assert(isPersonLinkedToMaster(conn, person1, NHS2));

		// And reverify the UKRDC through read APIs
		assert(getNationalIdentity(conn, person1, NationalIdentity.UKRDC_TYPE).equals(ukrdc1));
		assert(isPersonLinkedToMaster(conn, person1, ukrdc1));

	}

	// P1 - Linked to NHS 1, will create UKRDC 1
	// P2 - Linked to NHS 2, will create UKRDC 2
	// P2 - Linked to NHS 1, will drop link to NHS 2, drop link to UKRDC 2 and link to UKRDC 1
	@Test
	public void test2ChangeNHSAndReverifyUKRDC() throws MpiException {
		
		String testId = "2";
		
		logger.debug("************* test2ChangeNHSAndReverifyUKRDC ****************");

		String originator = testSuite+testId;
		String idBase = originator+"000";
		String nhsBase = "NH"+idBase;
		String LOCAL1 = idBase+"1";
		String LOCAL2 = idBase+"2";
		NationalIdentity NHS1 = new NationalIdentity(NationalIdentity.NHS_TYPE, nhsBase+"1");
		NationalIdentity NHS2 = new NationalIdentity(NationalIdentity.NHS_TYPE, nhsBase+"2");
		
		Person p1 = createPerson(LOCAL1, originator, d1, "ANT", "ADAM", "1", NHS1);
		NationalIdentity ukrdc1 = store(p1);

		Person p2 = createPerson(LOCAL2, originator, d1, "ANT", "ADAM", "1", NHS2);
		NationalIdentity ukrdc2 = store(p2);

		// VERIFY SETUP
		assert(ukrdc1 != null);
		assert(ukrdc2 != null);
		assert(!ukrdc1.equals(ukrdc2));
		
		// UPDATE NHS Number
		p1.setNationalIds(new ArrayList<NationalIdentity>());
		p1.addNationalId(NHS2);
		NationalIdentity ukrdc1_A = store(p1);
		
		// VERIFY UPDATE - UKRDC
		assert(ukrdc1_A != null);
		assert(!ukrdc1.equals(ukrdc1_A));
		assert(ukrdc2.equals(ukrdc1_A));
		
		// VERIFY UPDATE - NHS
		Person person1 = PersonDAO.findByLocalId(conn, MR, LOCAL1, p1.getOriginator());
		assert(getNationalIdentity(conn, person1, NationalIdentity.NHS_TYPE).equals(NHS2));
		assert(!isPersonLinkedToMaster(conn, person1, NHS1));
		assert(isPersonLinkedToMaster(conn, person1, NHS2));

		// And reverify the UKRDC through read APIs
		assert(getNationalIdentity(conn, person1, NationalIdentity.UKRDC_TYPE).equals(ukrdc1_A));
		assert(!isPersonLinkedToMaster(conn, person1, ukrdc1));
		assert(isPersonLinkedToMaster(conn, person1, ukrdc1_A));

	}

	// P1 - Linked to NHS 1 & CHI 1, will create UKRDC 1
	// P1 - Linked to NHS 2, will drop link to NHS 2. UKRDC not changed, CHI not changed
	@Test
	public void test3ChangeNHSNotCHI() throws MpiException {
		String testId = "3";
		
		logger.debug("************* test3ChangeNHSNotCHI ****************");

		String originator = testSuite+testId;
		String idBase = originator+"000";
		String nhsBase = "NH"+idBase;
		String chiBase = "CH"+idBase;
		String LOCAL1 = idBase+"1";
		NationalIdentity NHS1 = new NationalIdentity(NationalIdentity.NHS_TYPE, nhsBase+"1");
		NationalIdentity NHS2 = new NationalIdentity(NationalIdentity.NHS_TYPE, nhsBase+"2");
		NationalIdentity CHI1 = new NationalIdentity(NationalIdentity.CHI_TYPE, chiBase+"1");
		
		Person p1 = createPerson(LOCAL1, originator, d1, "ANT", "ADAM", "1", NHS1);
		p1.addNationalId(CHI1);
		NationalIdentity ukrdc1 = store(p1);

		// VERIFY SETUP
		assert(ukrdc1 != null);
		Person person1 = PersonDAO.findByLocalId(conn, MR, LOCAL1, p1.getOriginator());
		assert(getNationalIdentity(conn, person1, NationalIdentity.NHS_TYPE).equals(NHS1));
		assert(isPersonLinkedToMaster(conn, person1, NHS1));
		assert(getNationalIdentity(conn, person1, NationalIdentity.CHI_TYPE).equals(CHI1));
		assert(isPersonLinkedToMaster(conn, person1, CHI1));
		
		// UPDATE NHS Number
		p1.setNationalIds(new ArrayList<NationalIdentity>());
		p1.addNationalId(CHI1);
		p1.addNationalId(NHS2);

		NationalIdentity ukrdc1_A = store(p1);
		
		// VERIFY UPDATE - UKRDC
		assert(ukrdc1_A != null);
		assert(ukrdc1.equals(ukrdc1_A));
		
		// VERIFY UPDATE - NHS
		person1 = PersonDAO.findByLocalId(conn, MR, LOCAL1, p1.getOriginator());
		assert(getNationalIdentity(conn, person1, NationalIdentity.NHS_TYPE).equals(NHS2));
		assert(!isPersonLinkedToMaster(conn, person1, NHS1));
		assert(isPersonLinkedToMaster(conn, person1, NHS2));

		// VERIFY CHI
		assert(getNationalIdentity(conn, person1, NationalIdentity.CHI_TYPE).equals(CHI1));
		assert(isPersonLinkedToMaster(conn, person1, CHI1));

		// And reverify the UKRDC through read APIs
		assert(getNationalIdentity(conn, person1, NationalIdentity.UKRDC_TYPE).equals(ukrdc1));
		assert(isPersonLinkedToMaster(conn, person1, ukrdc1));

	}

	// P1 - Linked to NHS1 will create UKRDC 1
	// P2 - Linked to CHI1 will create UKRDC 2
	// P3 - Linked to NHS1 and CHI1, will link to .... UKRDC1 and warn for 2
	@Test
	public void test4ConflictingLinks() throws MpiException {
		String testId = testSuite+"4";
		
		logger.debug("************* test4ConflictingLinks ****************");

		String originator1 = testId+"1";
		String originator2 = testId+"2";
		String originator3 = testId+"3";
		String LOCAL1 = originator1+"001";
		String LOCAL2 = originator2+"002";
		String LOCAL3 = originator3+"003";
		String nhsBase = "NH00"+testId;
		String chiBase = "CH00"+testId;
		
		NationalIdentity NHS1 = new NationalIdentity(NationalIdentity.NHS_TYPE, nhsBase+"1");
		NationalIdentity CHI1 = new NationalIdentity(NationalIdentity.CHI_TYPE, chiBase+"1");
		
		Person p1 = createPerson(LOCAL1, originator1, d1, "ANT", "ADAM", "1", NHS1);
		NationalIdentity ukrdc1 = store(p1);

		Person p2 = createPerson(LOCAL2, originator2, d1, "ANT", "ADAM", "1", CHI1);
		NationalIdentity ukrdc2 = store(p2);

		// VERIFY SETUP
		assert(ukrdc1 != null);
		Person person1 = PersonDAO.findByLocalId(conn, MR, LOCAL1, originator1);
		assert(getNationalIdentity(conn, person1, NationalIdentity.NHS_TYPE).equals(NHS1));
		assert(isPersonLinkedToMaster(conn, person1, NHS1));

		assert(ukrdc2 != null);
		Person person2 = PersonDAO.findByLocalId(conn, MR, LOCAL2, originator2);
		assert(getNationalIdentity(conn, person2, NationalIdentity.CHI_TYPE).equals(CHI1));
		assert(isPersonLinkedToMaster(conn, person2, CHI1));

		Person p3 = createPerson(LOCAL3, originator3, d1, "ANT", "ADAM", "1", NHS1);
		p3.addNationalId(CHI1);
		NationalIdentity ukrdc3 = store(p3);
		
		// VERIFY
		Person person3 = PersonDAO.findByLocalId(conn, MR, LOCAL3, originator3);
		assert(isPersonLinkedToMaster(conn, person3, CHI1));
		assert(isPersonLinkedToMaster(conn, person3, NHS1));
		assert(isPersonLinkedToMaster(conn, person3, ukrdc1));
		assert(!isPersonLinkedToMaster(conn, person3, ukrdc2));
		assert(isPersonLinkedToMaster(conn, person3, ukrdc3));
		assert(ukrdc3.equals(ukrdc1));
		
		// UPDATE - No Nat ID change
		p1.setGivenName("Adam");
		NationalIdentity ukrdc1_A = store(p1);
		assert(ukrdc1_A != null);
		assert(ukrdc1_A.equals(ukrdc1));
		
	}

}
