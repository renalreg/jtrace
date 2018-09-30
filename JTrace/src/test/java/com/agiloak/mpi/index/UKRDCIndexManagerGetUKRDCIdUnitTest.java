package com.agiloak.mpi.index;

import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.SimpleConnectionManager;
import com.agiloak.mpi.index.persistence.MasterRecordDAO;

public class UKRDCIndexManagerGetUKRDCIdUnitTest extends UKRDCIndexManagerBaseTest {
	
	@Rule
	public final ExpectedException exception = ExpectedException.none();
	
	public static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
	private Date d1 = getDate("1962-08-31");
	@BeforeClass
	public static void setup()  throws MpiException {
		SimpleConnectionManager.configure("postgres", "postgres","localhost", "5432", "JTRACE");
		clear("NSYS100001", "NSYS1");
		clear("NSYS100002", "NSYS1");
	}

	@Test
	public void testMissingMasterId() throws MpiException, SQLException {
		UKRDCIndexManager im = new UKRDCIndexManager();
		UKRDCIndexManagerResponse resp = im.getUKRDCId(0);
		assert(resp.getStatus()==UKRDCIndexManagerResponse.FAIL);
		assert(resp.getMessage().contains("does not exist"));
	}
	@Test
	public void testUnknownMasterId() throws MpiException, SQLException {
		UKRDCIndexManager im = new UKRDCIndexManager();
		UKRDCIndexManagerResponse resp = im.getUKRDCId(9999999);
		assert(resp.getStatus()==UKRDCIndexManagerResponse.FAIL);
		assert(resp.getMessage().contains("does not exist"));
	}
	
	@Test
	public void testNonUKRDCMasterId() throws MpiException, SQLException {
		String orig = "NSYS1";
		String ukrdcId = "RR1000001";
		
		// T1-1 NationalId for P1. New Person, New Master and new link to the master
		Person p1 = new Person().setDateOfBirth(d1).setSurname(" JONES").setGivenName("NICHOLAS ").setPrimaryIdType(NationalIdentity.UKRDC_TYPE).setPrimaryId(ukrdcId).setGender("1");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId("NSYS100001").setLocalIdType("MR").setOriginator(orig);
		p1.addNationalId(new NationalIdentity(NationalIdentity.NHS_TYPE,"NHS0100001"));
		NationalIdentity natId = store(p1);
		MasterRecord master1 = MasterRecordDAO.findByNationalId("NHS0100001", NationalIdentity.NHS_TYPE);
		assert(master1!=null);
		
		UKRDCIndexManager im = new UKRDCIndexManager();
		UKRDCIndexManagerResponse resp = im.getUKRDCId(master1.getId());
		assert(resp.getStatus()==UKRDCIndexManagerResponse.FAIL);
		assert(resp.getMessage().contains("UKRDC"));
	}
	
	@Test
	public void testRealMasterId() throws MpiException, SQLException {
		String orig = "NSYS1";
		String ukrdcId = "RR1000002";
		
		// T1-1 NationalId for P1. New Person, New Master and new link to the master
		Person p1 = new Person().setDateOfBirth(d1).setSurname(" JONES").setGivenName("NICHOLAS").setPrimaryIdType(NationalIdentity.UKRDC_TYPE).setPrimaryId(ukrdcId).setGender("1");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId("NSYS100002").setLocalIdType("MR").setOriginator(orig);
		p1.addNationalId(new NationalIdentity(NationalIdentity.NHS_TYPE,"NHS0100002"));
		NationalIdentity natId = store(p1);
		MasterRecord master1 = MasterRecordDAO.findByNationalId("NHS0100002", NationalIdentity.NHS_TYPE);
		assert(master1!=null);
		MasterRecord master2 = MasterRecordDAO.findByNationalId(ukrdcId, NationalIdentity.UKRDC_TYPE);
		assert(master2!=null);
		
		UKRDCIndexManager im = new UKRDCIndexManager();
		UKRDCIndexManagerResponse resp = im.getUKRDCId(master2.getId());
		assert(resp.getStatus()==UKRDCIndexManagerResponse.SUCCESS);
	}
		
}
