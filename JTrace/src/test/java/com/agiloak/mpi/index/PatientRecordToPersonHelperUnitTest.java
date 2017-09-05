package com.agiloak.mpi.index;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.ukrdc.repository.model.Address;
import org.ukrdc.repository.model.Name;
import org.ukrdc.repository.model.Patient;
import org.ukrdc.repository.model.PatientNumber;
import org.ukrdc.repository.model.PatientRecord;
import org.ukrdc.repository.model.SendingFacility;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.index.persistence.LinkRecordDAO;
import com.agiloak.mpi.index.persistence.MasterRecordDAO;
import com.agiloak.mpi.index.persistence.PersonDAO;
import com.agiloak.mpi.trace.persistence.TraceDAO;
import com.agiloak.mpi.workitem.WorkItem;
import com.agiloak.mpi.workitem.persistence.WorkItemDAO;

public class PatientRecordToPersonHelperUnitTest {
	
	/**
	 * Note this is a fairly basic unit test. Integration testing will be the main test of the Helper class 
	 */
	
	@Rule
	public final ExpectedException exception = ExpectedException.none();

	private Date d1 = getDate("1962-08-31");
	private Date d2 = getDate("1962-08-30");

	public static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
	
	@BeforeClass
	public static void setup()  throws MpiException {

		clear( "HLP10000L1", "HLP1");
		MasterRecordDAO.deleteByNationalId("NHS01000N1",NationalIdentity.NHS_TYPE);
		MasterRecordDAO.deleteByNationalId("UKRR1000R1",NationalIdentity.UKRR_TYPE);
		MasterRecordDAO.deleteByNationalId("CHI01000C1",NationalIdentity.CHI_TYPE);

	}

	@Test
	public void testSimple() throws MpiException, IOException {

		String originator = "HLP1";
		String idBase = originator+"0000";

		PatientRecord pr = getTestPatientRecord(originator, idBase);
		
		Person person = PatientRecordToPersonHelper.convertPatientRecordToPerson(pr);
		
		UKRDCIndexManager im = new UKRDCIndexManager();
		im.createOrUpdate(person);
		person = PersonDAO.findByLocalId("MRN", idBase+"1", originator);
		assert(person!=null);
		MasterRecord master = MasterRecordDAO.findByNationalId(idBase+"R1", NationalIdentity.UKRR_TYPE);
		assert(master.getSurname().equals("JONES"));
		assert(master!=null);
		List<LinkRecord> links = LinkRecordDAO.findByPerson(person.getId());
		assert(links.size()==3);
		List<WorkItem> items = WorkItemDAO.findByPerson(person.getId());
		assert(items.size()==0);

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
	
	public static void clear(String localId, String originator)  throws MpiException {
		
		Person person = PersonDAO.findByLocalId("MR", localId, originator);
		if (person != null) {
			LinkRecordDAO.deleteByPerson(person.getId());
			WorkItemDAO.deleteByPerson(person.getId());
			PersonDAO.delete(person);
			String traceId = TraceDAO.getTraceId(localId, "MR", originator, "AUTO");
			if (traceId != null) {
				TraceDAO.clearByTraceId(traceId);
			}
		}

	}	
	
	public PatientRecord getTestPatientRecord(String originator, String idBase) {
		PatientRecord pr = new PatientRecord();
		pr.setLocalPatientId(idBase+"L1");
		SendingFacility sf = new SendingFacility();
		sf.setValue(originator);
		pr.setSendingFacility(sf);
		
		Patient pat = new Patient();
		pat.setBirthTime(d1);
		pat.setDeathTime(d2);
		pat.setGender("1");
		List<Address> addrList = new ArrayList<Address>();
		Address a1 = new Address();
		a1.setUse("H");
		a1.setPostcode("CH1 1AA");
		a1.setStreet("High Street");
		addrList.add(a1);
		Address a2 = new Address();
		a2.setUse("W");
		a2.setPostcode("CH2 2AA");
		a2.setStreet("Work Street");
		addrList.add(a2);
		pat.setAddresses(addrList);
		
		List<Name> nameList = new ArrayList<Name>();
		Name n1 = new Name();
		n1.setFamily("JONES");
		List<String> givenNames = new ArrayList<String>();
		givenNames.add("NICK");
		givenNames.add("I");
		n1.setGiven(givenNames);
		n1.setUse("L");
		n1.setPrefix("MR");
		nameList.add(n1);
		Name n2 = new Name();
		n2.setFamily("SMITH");
		List<String> givenNames2 = new ArrayList<String>();
		givenNames2.add("BOB");
		givenNames2.add("B");
		n2.setGiven(givenNames2);
		n2.setUse("A");
		n2.setPrefix("Master");
		nameList.add(n2);
		pat.setNames(nameList);
		
		List<PatientNumber> pnList = new ArrayList<PatientNumber>();
		PatientNumber pn1 = new PatientNumber();
		pn1.setNumberType("NI");
		pn1.setNumber(idBase+"N1");
		pn1.setOrganization("NHS");
		pnList.add(pn1);
		PatientNumber pn2 = new PatientNumber();
		pn2.setNumberType("NI");
		pn2.setNumber(idBase+"C1");
		pn2.setOrganization("CHI");
		pnList.add(pn2);
		PatientNumber pn3 = new PatientNumber();
		pn3.setNumberType("MRN");
		pn3.setNumber(idBase+"L1");
		pn3.setOrganization("RXF01");
		pnList.add(pn3);
		PatientNumber pn4 = new PatientNumber();
		pn4.setNumberType("NI");
		pn4.setNumber(idBase+"R1");
		pn4.setOrganization("UKRR");
		pnList.add(pn4);
		
		pat.setPatientNumbers(pnList);
		
		pr.setPatient(pat);
		return pr;
		
	}
}
