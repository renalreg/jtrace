package com.agiloak.mpi.index.persistence;

import java.util.Date;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.index.MasterRecord;
import com.agiloak.mpi.index.Person;

public class MasterRecordTest {

	@Rule
	public final ExpectedException exception = ExpectedException.none();

	@Before
	public void setup()  throws MpiException {
		// delete test data
		MasterRecordDAO.deleteByNationalId("NHS0000001","NHS");
		MasterRecordDAO.deleteByNationalId("NHS0000002","NHS");
		MasterRecordDAO.deleteByNationalId("NHS0000003","NHS");
		MasterRecordDAO.deleteByNationalId("NHS0000004","NHS");
		MasterRecordDAO.deleteByNationalId("NHS0000005","NHS");
	}
	
	@Test
	public void testDelete() throws MpiException {
		MasterRecord mr = new MasterRecord();
		mr.setDateOfBirth(new Date()).setGender("M");
		mr.setGivenName("Nick").setSurname("Jones");
		mr.setNationalId("NHS0000004").setNationalIdType("NHS");
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
		mr.setDateOfBirth(new Date()).setGender("M");
		mr.setGivenName("Nick").setSurname("Jones");
		mr.setNationalId("NHS0000001").setNationalIdType("NHS");
		MasterRecordDAO.create(mr);
		assert(true);
	}

	@Test
	public void testUpdate() throws MpiException {
		MasterRecord mr = new MasterRecord();
		mr.setDateOfBirth(new Date()).setGender("M");
		mr.setGivenName("Nick").setSurname("Jones");
		mr.setNationalId("NHS0000005").setNationalIdType("NHS");
		MasterRecordDAO.create(mr);
		assert(true);
		
		mr.setGender("F");
		mr.setGivenName("Nicholas").setSurname("James");
		MasterRecordDAO.update(mr);
		assert(true);
		
		MasterRecord mr2 = MasterRecordDAO.findByNationalId("NHS0000005","NHS");
		assert(mr2.getId()==(mr.getId()));
		// TODO Rework the date only check to work
		//assert(mr2.getDateOfBirth().compareTo(mr.getDateOfBirth())==0);
		assert(mr2.getGender().equals("F"));
		assert(mr2.getGivenName().equals("Nicholas"));
		assert(mr2.getSurname().equals("James"));
		assert(mr2.getNationalId().equals(mr.getNationalId()));
		assert(mr2.getNationalIdType().trim().equals(mr.getNationalIdType().trim()));
		assert(mr2.getLastUpdated().compareTo(mr.getLastUpdated())==0);
		
	}

	@Test
	public void testCreateDupl() throws MpiException {
		MasterRecord mr = new MasterRecord();
		mr.setDateOfBirth(new Date()).setGender("M");
		mr.setGivenName("Nick").setSurname("Jones");
		mr.setNationalId("NHS0000002").setNationalIdType("NHS");
		MasterRecordDAO.create(mr);
		assert(true);
		
		// Second insert should fail
		exception.expect(MpiException.class);
		MasterRecordDAO.create(mr);
	}
	
	@Test
	public void testFind() throws MpiException {
		MasterRecord mr = new MasterRecord();
		mr.setDateOfBirth(new Date()).setGender("M");
		mr.setGivenName("Nick").setSurname("Jones");
		mr.setNationalId("NHS0000003").setNationalIdType("NHS");
		MasterRecordDAO.create(mr);
		assert(true);
		
		MasterRecord mr2 = MasterRecordDAO.findByNationalId("NHS0000003","NHS");
		assert(mr2.getId()==(mr.getId()));
		// TODO Rework the date only check to work
		//assert(mr2.getDateOfBirth().compareTo(mr.getDateOfBirth())==0);
		assert(mr2.getGender().equals(mr.getGender()));
		assert(mr2.getGivenName().equals(mr.getGivenName()));
		assert(mr2.getSurname().equals(mr.getSurname()));
		assert(mr2.getNationalId().equals(mr.getNationalId()));
		assert(mr2.getNationalIdType().trim().equals(mr.getNationalIdType().trim()));
		assert(mr2.getLastUpdated().compareTo(mr.getLastUpdated())==0);
	}

	@Test
	public void testFindByDemographics1() throws MpiException {
		
		MasterRecord mr = new MasterRecord();
		mr.setDateOfBirth(new Date()).setGender("M");
		mr.setGivenName("Nick").setSurname("Jones");
		mr.setNationalId("NHS0000003").setNationalIdType("NHS");
		MasterRecordDAO.create(mr);
		assert(true);
		
		Person person = new Person();
		person.setDateOfBirth(new Date()).setGivenName("Nick").setSurname("Jones");
		
		List<MasterRecord> mrl = MasterRecordDAO.findByDemographics(person);
		assert(mrl.size()==1);
		/*
        assert(mr2.getId()==(mr.getId()));
		assert(mr2.getDateOfBirth().compareTo(mr.getDateOfBirth())==0);
		assert(mr2.getGender().equals(mr.getGender()));
		assert(mr2.getGivenName().equals(mr.getGivenName()));
		assert(mr2.getSurname().equals(mr.getSurname()));
		assert(mr2.getNationalId().equals(mr.getNationalId()));
		assert(mr2.getNationalIdType().trim().equals(mr.getNationalIdType().trim()));
		assert(mr2.getLastUpdated().compareTo(mr.getLastUpdated())==0);
		 */
	}
	@Test
	public void testFindByDemographics2() throws MpiException {
		
		MasterRecord mr = new MasterRecord();
		mr.setDateOfBirth(new Date()).setGender("M");
		mr.setGivenName("Nick").setSurname("Jones");
		mr.setNationalId("NHS0000006").setNationalIdType("NHS");
		MasterRecordDAO.create(mr);
		assert(true);
		mr.setDateOfBirth(new Date()).setGender("M");
		mr.setGivenName("Nick").setSurname("Jones");
		mr.setNationalId("NHS0000007").setNationalIdType("NHS");
		MasterRecordDAO.create(mr);
		assert(true);
		
		Person person = new Person();
		person.setDateOfBirth(new Date()).setGivenName("Nick").setSurname("Jones");
		
		List<MasterRecord> mrl = MasterRecordDAO.findByDemographics(person);
		assert(mrl.size()==2);
		/*
        assert(mr2.getId()==(mr.getId()));
		assert(mr2.getDateOfBirth().compareTo(mr.getDateOfBirth())==0);
		assert(mr2.getGender().equals(mr.getGender()));
		assert(mr2.getGivenName().equals(mr.getGivenName()));
		assert(mr2.getSurname().equals(mr.getSurname()));
		assert(mr2.getNationalId().equals(mr.getNationalId()));
		assert(mr2.getNationalIdType().trim().equals(mr.getNationalIdType().trim()));
		assert(mr2.getLastUpdated().compareTo(mr.getLastUpdated())==0);
		 */
	}
}
