package com.agiloak.mpi.index;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.agiloak.mpi.index.persistence.PersonDAO;
import com.agiloak.mpi.normalization.NormalizationManager;

public class UKRDCIndexManager {
	
	private final static Logger logger = LoggerFactory.getLogger(UKRDCIndexManager.class);

	/**
	 *  Created for the UKRDC this is a combined createOrUpdate. This method updates the person, the master (as appropriate) and the link records ( as appropriate)
	 *  
	 * @param person
	 */
	public void createOrUpdate(Person person) {
		
		// Does person exist?
		Person storedPerson = PersonDAO.findPerson(person.getLocalIdType(), person.getLocalId(), person.getLocalIdOriginator());
		if (storedPerson != null) {
			logger.info("RECORD EXISTS");
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
		
	public Person read(String localidtype, String localid, String originator){
		Person person = PersonDAO.findPerson(localidtype, localid, originator);
		return person;
	}
}
