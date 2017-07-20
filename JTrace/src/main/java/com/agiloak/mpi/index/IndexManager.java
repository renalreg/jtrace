package com.agiloak.mpi.index;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.agiloak.mpi.index.persistence.PersonDAO;
import com.agiloak.mpi.normalization.NormalizationManager;

public class IndexManager {
	
	private final static Logger logger = LoggerFactory.getLogger(IndexManager.class);

	public void create(Person person) {
		// Does person exist?
		// -- check by local id and type - WARN - Throw exception
		Person storedPerson = PersonDAO.findPerson(person.getLocalIdType(), person.getLocalId(), person.getLocalIdOriginator());
		if (storedPerson != null) {
			logger.error("ERROR: Person id already exists for this domain");
			return;
		}
		// -- check by NHS Number (or all 2ndary ids?) - ERROR - Throw exception
		// Not sure this applies in the EMPI world - do we want to stop this?
		
		// Validate
		
		// Standardise
		person.setStdSurname(NormalizationManager.getStandardSurname(person.getSurname()));
		person.setStdGivenName(NormalizationManager.getStandardGivenName(person.getGivenName()));
		person.setStdPostcode(NormalizationManager.getStandardPostcode(person.getPostcode()));
		
		// Store the record
		PersonDAO.insert(person);
		
	}
	
	public void update(Person person) {
		// Does person exist?
		Person storedPerson = PersonDAO.findPerson(person.getLocalIdType(), person.getLocalId(), person.getLocalIdOriginator());
		if (storedPerson==null) {
			logger.error("ERROR: Person id does not exist for this domain");
			return;
		}
		
		// -- check by NHS Number (or all 2ndary ids?) - ERROR - Throw exception
		// Not sure this applies in the EMPI world - do we want to stop this?
		
		// Validate
				
		// Standardise
		person.setStdSurname(NormalizationManager.getStandardSurname(person.getSurname()));
		person.setStdGivenName(NormalizationManager.getStandardGivenName(person.getGivenName()));
		person.setStdPostcode(NormalizationManager.getStandardPostcode(person.getPostcode()));
		if (!storedPerson.getSurname().equals(person.getSurname())){
			// surname change
			person.setPrevSurname(storedPerson.getSurname());
			person.setStdPrevSurname(NormalizationManager.getStandardSurname(storedPerson.getSurname()));
		}
		// Store the record
		person.setMasterId(storedPerson.getMasterId());
		PersonDAO.update(person);
		
	}
	
	public Person read(String localidtype, String localid, String originator){
		Person person = PersonDAO.findPerson(localidtype, localid, originator);
		return person;
	}
}
