package com.agiloak.mpi.index.persistence;

import java.util.Date;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.index.LinkRecord;
import com.agiloak.mpi.index.MasterRecord;
import com.agiloak.mpi.index.Person;

public class LinkRecordTest {
	final static String UKRDC_TYPE = "UKRDC";
	final static String RR1 = "RR1";

	@Rule
	public final ExpectedException exception = ExpectedException.none();

	@Before
	public void setup() throws MpiException {
		LinkRecordDAO.deleteByPerson(1);
		LinkRecordDAO.deleteByPerson(2);
		LinkRecordDAO.deleteByPerson(3);
		LinkRecordDAO.deleteByPerson(4);
		LinkRecordDAO.deleteByPerson(5);
		LinkRecordDAO.deleteByPerson(6);
		LinkRecordDAO.deleteByPerson(7);
		LinkRecordDAO.deleteByPerson(8);
		LinkRecordDAO.deleteByPerson(9);
		LinkRecordDAO.deleteByPerson(10);
		MasterRecordDAO.deleteByNationalId(RR1, UKRDC_TYPE);
	}
	
	@Test
	public void testCreate() throws MpiException {
		int personToTest = 1;
		LinkRecord lr = new LinkRecord(1,personToTest);
		LinkRecordDAO.create(lr);
		
		List<LinkRecord> links = LinkRecordDAO.findByPerson(personToTest);
		assert(links.size()==1);

		LinkRecord lr2 = links.get(0);
		
		verifyEqual(lr2, lr);
	}
	
	@Test
	public void testCreateAllFields() throws MpiException {
		int personToTest = 1;
		LinkRecord lr = new LinkRecord(1,personToTest);
		lr.setUpdatedBy("Nick");
		lr.setLinkCode(1);
		lr.setLinkType(2);
		lr.setLinkDesc("XYZ uses preferred name of patient");
		LinkRecordDAO.create(lr);
		
		List<LinkRecord> links = LinkRecordDAO.findByPerson(personToTest);
		assert(links.size()==1);

		LinkRecord lr2 = links.get(0);
		verifyEqual(lr2, lr);

	}
	
	@Test
	public void testFind() throws MpiException {
		int personToTest = 2;
		LinkRecord lr = new LinkRecord(1,personToTest);
		LinkRecordDAO.create(lr);
		List<LinkRecord> links = LinkRecordDAO.findByPerson(personToTest);
		assert(links.size()==1);

		LinkRecord lr2 =LinkRecordDAO.find(1,personToTest);
		assert(lr.getId()==lr2.getId());
	}
	
	@Test
	public void testFindByPerson() throws MpiException {
		int personToTest = 3;
		LinkRecord lr = new LinkRecord(1,personToTest);
		LinkRecordDAO.create(lr);
		
		List<LinkRecord> links =LinkRecordDAO.findByPerson(personToTest);
		assert(lr.getId()==links.get(0).getId());
		assert(links.size()==1);
	}
	
	@Test
	public void testNoFindByPerson() throws MpiException {
		int personToTest = 4;
		LinkRecord lr = new LinkRecord(1,personToTest);
		LinkRecordDAO.create(lr);
		List<LinkRecord> links = LinkRecordDAO.findByPerson(personToTest);
		assert(links.size()==1);
		
		List<LinkRecord> links2 =LinkRecordDAO.findByPerson(99);
		assert(links2.size()==0);
	}

	@Test
	public void testNoFind() throws MpiException {
		int personToTest = 5;
		LinkRecord lr = new LinkRecord(1,personToTest);
		LinkRecordDAO.create(lr);
		List<LinkRecord> links = LinkRecordDAO.findByPerson(personToTest);
		assert(links.size()==1);

		LinkRecord lr2 =LinkRecordDAO.find(2,personToTest);
		assert(lr2==null);
	}

	@Test
	public void testDelete() throws MpiException {
		int personToTest = 6;
		LinkRecord lr = new LinkRecord(1,personToTest);
		LinkRecordDAO.create(lr);
		List<LinkRecord> links = LinkRecordDAO.findByPerson(personToTest);
		assert(links.size()==1);

		LinkRecordDAO.delete(lr);
		List<LinkRecord> links2 = LinkRecordDAO.findByPerson(personToTest);
		assert(links2.size()==0);
	}
	
	@Test
	public void testDeleteByPerson() throws MpiException {
		int personToTest = 7;
		LinkRecord lr = new LinkRecord(1,personToTest);
		LinkRecordDAO.create(lr);
		List<LinkRecord> links = LinkRecordDAO.findByPerson(personToTest);
		assert(links.size()==1);
		
		LinkRecordDAO.deleteByPerson(personToTest);
		List<LinkRecord> links2 = LinkRecordDAO.findByPerson(personToTest);
		assert(links2.size()==0);
		
	}

	@Test
	public void testFindByPersonAndType() throws MpiException {
		int personToTest = 8;
		MasterRecord mr = new MasterRecord();
		mr.setNationalId(RR1).setNationalIdType(UKRDC_TYPE);
		mr.setDateOfBirth(new Date());
		mr.setEffectiveDate(new Date());
		MasterRecordDAO.create(mr);
		LinkRecord lr = new LinkRecord(mr.getId(),personToTest);
		LinkRecordDAO.create(lr);
		
		LinkRecord link = LinkRecordDAO.findByPersonAndType(personToTest, UKRDC_TYPE);
		assert(lr.getId()==link.getId());
	}

	@Test
	public void testCountByMasterAndOriginator() throws MpiException {
		
		Person person1 = new Person();
		person1.setOriginator("TORG1").setLocalId("TST1000001").setLocalIdType("MR");
		person1.setTitle("MR").setGivenName("NICK").setSurname("JONES");
		person1.setDateOfBirth(new Date());
		person1.setGender("1");
		PersonDAO.create(person1);

		Person person2 = new Person();
		person2.setOriginator("TORG1").setLocalId("TST1000002").setLocalIdType("MR");
		person2.setTitle("MR").setGivenName("NICK").setSurname("JONES");
		person2.setDateOfBirth(new Date());
		person2.setGender("1");
		PersonDAO.create(person2);

		MasterRecord mr = new MasterRecord();
		mr.setNationalId(RR1).setNationalIdType(UKRDC_TYPE);
		mr.setDateOfBirth(new Date());
		mr.setEffectiveDate(new Date());
		MasterRecordDAO.create(mr);
		LinkRecord lr = new LinkRecord(mr.getId(),person1.getId());
		LinkRecordDAO.create(lr);
		
		int count = LinkRecordDAO.countByMasterAndOriginator(mr.getId(), "TORG1");
		assert(count==1);
	
		lr = new LinkRecord(mr.getId(),person2.getId());
		LinkRecordDAO.create(lr);
		
		count = LinkRecordDAO.countByMasterAndOriginator(mr.getId(), "TORG1");
		assert(count==2);

	}

	private void verifyEqual(LinkRecord lr2, LinkRecord lr1) throws MpiException {
		assert(lr2.getId()==lr1.getId());
		assert(lr2.getMasterId()==lr1.getMasterId());
		assert(lr2.getPersonId()==lr1.getPersonId());
		// Note the following assertion fails if coded the other way around! looks like a bug in compareTo - beware
		assert(lr2.getLastUpdated().compareTo(lr1.getLastUpdated())==0);
		assert(lr2.getLinkCode()==lr1.getLinkCode());
		assert(lr2.getLinkType()==lr1.getLinkType());
		if (lr2.getUpdatedBy()==null) {
			assert(lr2.getUpdatedBy()==null);
		} else {
			assert(lr2.getUpdatedBy().equals(lr1.getUpdatedBy()));
		}
		
	}
	
}
