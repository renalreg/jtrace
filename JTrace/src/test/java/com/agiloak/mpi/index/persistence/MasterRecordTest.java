package com.agiloak.mpi.index.persistence;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.index.MasterRecord;
import com.agiloak.mpi.index.Person;

public class MasterRecordTest {
	
	public static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

	@Rule
	public final ExpectedException exception = ExpectedException.none();

	@BeforeClass
	public static void setup()  throws MpiException {
		// delete test data
		MasterRecordDAO.deleteByNationalId("NHS0000001","NHS");
		MasterRecordDAO.deleteByNationalId("NHS0000002","NHS");
		MasterRecordDAO.deleteByNationalId("NHS0000003","NHS");
		MasterRecordDAO.deleteByNationalId("NHS0000004","NHS");
		MasterRecordDAO.deleteByNationalId("NHS0000005","NHS");
		MasterRecordDAO.deleteByNationalId("NHS0000006","NHS");
		MasterRecordDAO.deleteByNationalId("NHS0000007","NHS");
		MasterRecordDAO.deleteByNationalId("NHS0000008","NHS");
		MasterRecordDAO.deleteByNationalId("NHS0000009","NHS");
		MasterRecordDAO.deleteByNationalId("NHS0000010","NHS");
		MasterRecordDAO.deleteByNationalId("NHS0000011","NHS");
		MasterRecordDAO.deleteByNationalId("NHS0000012","NHS");
	}
	
	@Test
	public void testGetSequence() throws MpiException {
		String ukrdcId = MasterRecordDAO.allocate();
		assert(ukrdcId!=null);
		assert(ukrdcId.length()==9);
	}

	@Test
	public void testDelete() throws MpiException {
		MasterRecord mr = new MasterRecord();
		mr.setDateOfBirth(getDate("1962-08-31")).setGender("M");
		mr.setGivenName("Nick").setSurname("Jones");
		mr.setNationalId("NHS0000011").setNationalIdType("NHS");
		mr.setEffectiveDate(getDate("2017-08-22"));
		MasterRecordDAO.create(mr);
		MasterRecord mr2 = MasterRecordDAO.findByNationalId("NHS0000011","NHS");
		assert(mr2!=null);
		MasterRecordDAO.delete(mr);
		MasterRecord mr3 = MasterRecordDAO.findByNationalId("NHS0000011","NHS");
		assert(mr3==null);
		MasterRecord mr4 = MasterRecordDAO.get(mr.getId());
		assert(mr4==null);
	}

	@Test
	public void testDeleteById() throws MpiException {
		MasterRecord mr = new MasterRecord();
		mr.setDateOfBirth(getDate("1962-08-31")).setGender("M");
		mr.setGivenName("Nick").setSurname("Jones");
		mr.setNationalId("NHS0000012").setNationalIdType("NHS");
		mr.setEffectiveDate(getDate("2017-08-22"));
		MasterRecordDAO.create(mr);
		MasterRecord mr2 = MasterRecordDAO.findByNationalId("NHS0000012","NHS");
		assert(mr2!=null);
		MasterRecordDAO.delete(mr.getId());
		MasterRecord mr3 = MasterRecordDAO.findByNationalId("NHS0000012","NHS");
		assert(mr3==null);
		MasterRecord mr4 = MasterRecordDAO.get(mr.getId());
		assert(mr4==null);
	}

	@Test
	public void testDeleteByNationalId() throws MpiException {
		MasterRecord mr = new MasterRecord();
		mr.setDateOfBirth(getDate("1962-08-31")).setGender("M");
		mr.setGivenName("Nick").setSurname("Jones");
		mr.setNationalId("NHS0000004").setNationalIdType("NHS");
		mr.setEffectiveDate(getDate("2017-08-22"));
		MasterRecordDAO.create(mr);
		assert(true);
		MasterRecord mr2 = MasterRecordDAO.findByNationalId("NHS0000004","NHS");
		assert(mr2.getId()==(mr.getId()));
		MasterRecordDAO.deleteByNationalId("NHS0000004","NHS");
		assert(true);
		MasterRecord mr3 = MasterRecordDAO.findByNationalId("NHS0000004","NHS");
		assert(mr3==null);
	}
	@Test
	public void testCreate() throws MpiException {
		MasterRecord mr = new MasterRecord();
		mr.setDateOfBirth(getDate("1962-08-31")).setGender("M");
		mr.setGivenName("Nick").setSurname("Jones");
		mr.setNationalId("NHS0000001").setNationalIdType("NHS");
		mr.setEffectiveDate(getDate("2017-08-22"));
		MasterRecordDAO.create(mr);
		assert(mr.getId()>0);
	}

	@Test
	public void testGet() throws MpiException {
		MasterRecord mr = new MasterRecord();
		mr.setDateOfBirth(getDate("1962-08-31")).setGender("M");
		mr.setGivenName("Nick").setSurname("Jones");
		mr.setNationalId("NHS0000009").setNationalIdType("NHS");
		mr.setEffectiveDate(getDate("2017-08-22"));
		MasterRecordDAO.create(mr);
		assert(true);
		
		MasterRecord mr2 = MasterRecordDAO.get(mr.getId());
		assert(mr2.getId()==(mr.getId()));
		assert(mr2.getDateOfBirth().compareTo(mr.getDateOfBirth())==0);
		assert(mr2.getGender().equals(mr.getGender()));
		assert(mr2.getGivenName().equals(mr.getGivenName()));
		assert(mr2.getSurname().equals(mr.getSurname()));
		assert(mr2.getNationalId().equals(mr.getNationalId()));
		assert(mr2.getNationalIdType().trim().equals(mr.getNationalIdType().trim()));
		assert(mr2.getLastUpdated().compareTo(mr.getLastUpdated())==0);
		assert(mr2.getEffectiveDate().compareTo(mr.getEffectiveDate())==0);
		assert(mr2.getStatus()==mr.getStatus());
		assert(mr2.getStatus()==MasterRecord.OK);
		
	}

	@Test
	public void testUpdate() throws MpiException {
		MasterRecord mr = new MasterRecord();
		mr.setDateOfBirth(getDate("1962-08-31")).setGender("M");
		mr.setGivenName("Nick").setSurname("Jones");
		mr.setNationalId("NHS0000005").setNationalIdType("NHS");
		mr.setEffectiveDate(getDate("2017-08-22"));
		MasterRecordDAO.create(mr);
		assert(true);
		
		mr.setGender("F");
		mr.setGivenName("Nicholas").setSurname("James");
		mr.setEffectiveDate(getDate("2017-08-24"));
		MasterRecordDAO.update(mr);
		assert(true);
		
		MasterRecord mr2 = MasterRecordDAO.findByNationalId("NHS0000005","NHS");
		assert(mr2.getId()==(mr.getId()));
		assert(mr2.getDateOfBirth().compareTo(mr.getDateOfBirth())==0);
		assert(mr2.getGender().equals(mr.getGender()));
		assert(mr2.getGivenName().equals(mr.getGivenName()));
		assert(mr2.getSurname().equals(mr.getSurname()));
		assert(mr2.getNationalId().equals(mr.getNationalId()));
		assert(mr2.getNationalIdType().trim().equals(mr.getNationalIdType().trim()));
		assert(mr2.getLastUpdated().compareTo(mr.getLastUpdated())==0);
		assert(mr2.getEffectiveDate().compareTo(mr.getEffectiveDate())==0);
		assert(mr2.getStatus()==mr.getStatus());
		assert(mr2.getStatus()==MasterRecord.OK);
	
	}

	@Test
	public void testUpdateInvestigate() throws MpiException {
		MasterRecord mr = new MasterRecord();
		mr.setDateOfBirth(getDate("1962-08-31")).setGender("M");
		mr.setGivenName("Nick").setSurname("Jones");
		mr.setNationalId("NHS0000010").setNationalIdType("NHS");
		mr.setEffectiveDate(getDate("2017-08-22"));
		MasterRecordDAO.create(mr);
		assert(true);
		
		mr.setStatus(MasterRecord.INVESTIGATE);
		MasterRecordDAO.update(mr);
		assert(true);
		
		MasterRecord mr2 = MasterRecordDAO.findByNationalId("NHS0000010","NHS");
		assert(mr2.getId()==(mr.getId()));
		assert(mr2.getDateOfBirth().compareTo(mr.getDateOfBirth())==0);
		assert(mr2.getGender().equals(mr.getGender()));
		assert(mr2.getGivenName().equals(mr.getGivenName()));
		assert(mr2.getSurname().equals(mr.getSurname()));
		assert(mr2.getNationalId().equals(mr.getNationalId()));
		assert(mr2.getNationalIdType().trim().equals(mr.getNationalIdType().trim()));
		assert(mr2.getLastUpdated().compareTo(mr.getLastUpdated())==0);
		assert(mr2.getEffectiveDate().compareTo(mr.getEffectiveDate())==0);
		assert(mr2.getStatus()==mr.getStatus());
		assert(mr2.getStatus()==MasterRecord.INVESTIGATE);
	
	}
	
	@Test
	public void testUpdateNoNationalId() throws MpiException {
		MasterRecord mr = new MasterRecord();
		mr.setDateOfBirth(getDate("1962-08-31")).setGender("M");
		mr.setGivenName("Nick").setSurname("Jones");
		mr.setEffectiveDate(getDate("2017-08-22"));
		exception.expect(MpiException.class);
		MasterRecordDAO.create(mr);
		assert(true);
	}

	@Test
	public void testCreateDupl() throws MpiException {
		MasterRecord mr = new MasterRecord();
		mr.setDateOfBirth(getDate("1962-08-31")).setGender("M");
		mr.setGivenName("Nick").setSurname("Jones");
		mr.setNationalId("NHS0000002").setNationalIdType("NHS");
		mr.setEffectiveDate(getDate("2017-08-22"));
		MasterRecordDAO.create(mr);
		assert(true);
		
		// Second insert should fail
		exception.expect(MpiException.class);
		MasterRecordDAO.create(mr);
	}
	
	@Test
	public void testFind() throws MpiException {
		MasterRecord mr = new MasterRecord();
		mr.setDateOfBirth(getDate("1962-08-31")).setGender("M");
		mr.setGivenName("Nick").setSurname("Jones");
		mr.setNationalId("NHS0000003").setNationalIdType("NHS");
		mr.setEffectiveDate(getDate("2017-08-22"));
		MasterRecordDAO.create(mr);
		assert(true);
		
		MasterRecord mr2 = MasterRecordDAO.findByNationalId("NHS0000003","NHS");
		assert(mr2.getId()==(mr.getId()));
		assert(mr2.getDateOfBirth().compareTo(mr.getDateOfBirth())==0);
		assert(mr2.getGender().equals(mr.getGender()));
		assert(mr2.getGivenName().equals(mr.getGivenName()));
		assert(mr2.getSurname().equals(mr.getSurname()));
		assert(mr2.getNationalId().equals(mr.getNationalId()));
		assert(mr2.getNationalIdType().trim().equals(mr.getNationalIdType().trim()));
		assert(mr2.getLastUpdated().compareTo(mr.getLastUpdated())==0);
		assert(mr2.getEffectiveDate().compareTo(mr.getEffectiveDate())==0);
		assert(mr2.getStatus()==mr.getStatus());
		assert(mr2.getStatus()==MasterRecord.OK);
	}

	@Test
	public void testFindByDemographics1() throws MpiException {
		
		MasterRecord mr = new MasterRecord();
		mr.setDateOfBirth(getDate("1962-08-31")).setGender("M");
		mr.setGivenName("BILL").setSurname("SMITH");
		mr.setNationalId("NHS0000006").setNationalIdType("NHS");
		mr.setEffectiveDate(getDate("2017-08-22"));
		MasterRecordDAO.create(mr);
		assert(true);
		
		Person person = new Person();
		person.setDateOfBirth(getDate("1962-08-31")).setGivenName("BILL").setSurname("SMITH");
		
		List<MasterRecord> mrl = MasterRecordDAO.findByDemographics(person);
		assert(mrl.size()==1);
		
		MasterRecord mr2 = mrl.get(0);
		assert(mr2.getDateOfBirth().compareTo(mr.getDateOfBirth())==0);
		assert(mr2.getGivenName().equals(mr.getGivenName()));
		assert(mr2.getSurname().equals(mr.getSurname()));
		assert(mr2.getLastUpdated().compareTo(mr.getLastUpdated())==0);
		assert(mr2.getEffectiveDate().compareTo(mr.getEffectiveDate())==0);
		assert(mr2.getStatus()==mr.getStatus());
		assert(mr2.getStatus()==MasterRecord.OK);
	}
	
	@Test
	public void testFindByDemographics2() throws MpiException {
		
		MasterRecord mr = new MasterRecord();
		mr.setDateOfBirth(getDate("1962-08-31")).setGender("M");
		mr.setGivenName("BRIAN").setSurname("MAY");
		mr.setNationalId("NHS0000007").setNationalIdType("NHS");
		mr.setEffectiveDate(getDate("2017-08-22"));
		MasterRecordDAO.create(mr);
		assert(true);
		mr.setDateOfBirth(getDate("1962-08-31")).setGender("M");
		mr.setGivenName("BRIAN").setSurname("MAY");
		mr.setNationalId("NHS0000008").setNationalIdType("NHS");
		mr.setEffectiveDate(getDate("2017-08-22"));
		MasterRecordDAO.create(mr);
		assert(true);
		
		Person person = new Person();
		person.setDateOfBirth(getDate("1962-08-31")).setGivenName("BRIAN").setSurname("MAY");
		
		List<MasterRecord> mrl = MasterRecordDAO.findByDemographics(person);
		assert(mrl.size()==2);
		for (MasterRecord mr2 : mrl) {
			assert(mr2.getDateOfBirth().compareTo(mr.getDateOfBirth())==0);
			assert(mr2.getGivenName().equals(mr.getGivenName()));
			assert(mr2.getSurname().equals(mr.getSurname()));
			assert(mr2.getLastUpdated().compareTo(mr.getLastUpdated())==0);
			assert(mr2.getEffectiveDate().compareTo(mr.getEffectiveDate())==0);
			assert(mr2.getStatus()==mr.getStatus());
			assert(mr2.getStatus()==MasterRecord.OK);
		}

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
}
