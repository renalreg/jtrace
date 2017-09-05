package com.agiloak.mpi.index;

import java.util.Date;

import org.ukrdc.repository.model.Address;
import org.ukrdc.repository.model.Name;
import org.ukrdc.repository.model.Patient;
import org.ukrdc.repository.model.PatientRecord;
import org.ukrdc.repository.model.PatientNumber;

import com.agiloak.mpi.MpiException;

public class PatientRecordToPersonHelper {

	public Person convertPatientRecordtoPerson(PatientRecord pr) throws MpiException {
		Person person = new Person();
		Patient patient = pr.getPatient();
		person.setDateOfBirth(patient.getBirthTime());
		person.setGender(patient.getGender());
		
		// Find the Usual Name
		Name usualName = null;
		for ( Name name : patient.getNames()) {
			if (name.getUse().equals("L")) {
				usualName = name;
			}
		}
		
		if (usualName != null) {
			person.setGivenName(usualName.getGiven1());
			person.setOtherGivenNames(usualName.getGiven2());
			person.setSurname(usualName.getFamily());
			person.setTitle(usualName.getPrefix());
		}
		
		// Find the Home Address
		Address homeAddress = null;
		for ( Address address : patient.getAddresses()) {
			if (address.getUse().equals("H")) {
				homeAddress = address;
			}
		}
		
		if (homeAddress != null) {
			person.setPostcode(homeAddress.getPostcode());
			person.setStreet(homeAddress.getStreet());
		}
		
		person.setDateOfDeath(patient.getDeathTime());
		person.setOriginator(pr.getSendingFacility().getValue());
		person.setLocalId(pr.getLocalPatientId());
		person.setLocalIdType("MRN");
		
		// Set the National Ids
		for ( PatientNumber patNum : pr.getPatient().getPatientNumbers()) {
			if (patNum.getNumberType().equals("NI")) {
				if (patNum.getOrganization().equals(NationalIdentity.UKRR_TYPE)) {
					person.setPrimaryId(patNum.getNumber());
					person.setPrimaryIdType(NationalIdentity.UKRR_TYPE);
				} else {
					NationalIdentity nationalId = new NationalIdentity(patNum.getOrganization(), patNum.getNumber());
					person.addNationalId(nationalId);
				}
			}
		}
		
		// Derive the effective date
		person.setEffectiveDate(new Date()); // TODO - where should the effective date come from?
		return person;
	}
}
