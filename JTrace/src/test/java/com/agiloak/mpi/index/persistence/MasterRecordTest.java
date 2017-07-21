package com.agiloak.mpi.index.persistence;

import java.util.Date;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.index.MasterRecord;

public class MasterRecordTest {

	@Rule
	public final ExpectedException exception = ExpectedException.none();

	@Before
	public void setup()  throws MpiException {
		// delete test data
		MasterRecordDAO.DeleteByNationalId("NHS0000001");
		MasterRecordDAO.DeleteByNationalId("NHS0000002");
		MasterRecordDAO.DeleteByNationalId("NHS0000003");
		assert(true);
	}
	
	@Test
	public void testDelete() throws MpiException {
		MasterRecord mr = new MasterRecord();
		mr.setDateOfBirth(new Date()).setGender("M").setGivenName("Nick").setSurname("Jones").setNationalId("NHS0000001");
		MasterRecordDAO.insert(mr);
		assert(true);
		MasterRecord mr2 = MasterRecordDAO.findMasterRecordByNationalId("NHS0000001");
		assert(mr2.getId()==(mr.getId()));
		MasterRecordDAO.DeleteByNationalId("NHS0000001");
		assert(true);
		MasterRecord mr3 = MasterRecordDAO.findMasterRecordByNationalId("NHS0000001");
		assert(mr3==null);
	}

	@Test
	public void testCreate() throws MpiException {
		MasterRecord mr = new MasterRecord();
		mr.setDateOfBirth(new Date()).setGender("M").setGivenName("Nick").setSurname("Jones").setNationalId("NHS0000001");
		MasterRecordDAO.insert(mr);
		assert(true);
	}

	@Test
	public void testCreateDupl() throws MpiException {
		MasterRecord mr = new MasterRecord();
		mr.setDateOfBirth(new Date()).setGender("M").setGivenName("Nick").setSurname("Jones").setNationalId("NHS0000002");
		MasterRecordDAO.insert(mr);
		assert(true);
		
		// Second insert should fail
		exception.expect(MpiException.class);
		MasterRecordDAO.insert(mr);
	}
	
	@Test
	public void testFind() throws MpiException {
		MasterRecord mr = new MasterRecord();
		mr.setDateOfBirth(new Date()).setGender("M").setGivenName("Nick").setSurname("Jones").setNationalId("NHS0000003");
		MasterRecordDAO.insert(mr);
		assert(true);
		MasterRecord mr2 = MasterRecordDAO.findMasterRecordByNationalId("NHS0000003");
		assert(mr2.getId()==(mr.getId()));
		assert(mr2.getDateOfBirth().compareTo(mr.getDateOfBirth())==0);
		assert(mr2.getGender().equals(mr.getGender()));
		assert(mr2.getGivenName().equals(mr.getGivenName()));
		assert(mr2.getSurname().equals(mr.getSurname()));
		assert(mr2.getNationalId().equals(mr.getNationalId()));
		assert(mr2.getLastUpdated().compareTo(mr.getLastUpdated())==0);
	}

}
