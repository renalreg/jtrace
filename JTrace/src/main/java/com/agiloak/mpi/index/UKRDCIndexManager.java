package com.agiloak.mpi.index;

import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.index.persistence.LinkRecordDAO;
import com.agiloak.mpi.index.persistence.MasterRecordDAO;
import com.agiloak.mpi.index.persistence.PersonDAO;
import com.agiloak.mpi.normalization.NormalizationManager;
import com.agiloak.mpi.trace.TraceManager;
import com.agiloak.mpi.trace.TraceRequest;
import com.agiloak.mpi.trace.TraceResponse;
import com.agiloak.mpi.workitem.WorkItem;
import com.agiloak.mpi.workitem.persistence.WorkItemDAO;

public class UKRDCIndexManager {
	
	private final static Logger logger = LoggerFactory.getLogger(UKRDCIndexManager.class);

	/**
	 *  Created for the UKRDC this is a combined createOrUpdate. This method updates the person, the master (as appropriate) and the link records ( as appropriate)
	 *  
	 * @param person
	 */
	public void createOrUpdate(Person person) throws MpiException {

		logger.debug("*********Processing person record:"+person);
		// TODO - Add auditing
		
		// Does person exist?
		Person storedPerson = PersonDAO.findByLocalId(person.getLocalIdType(), person.getLocalId(), person.getOriginator());
		if (storedPerson != null) {
			logger.debug("RECORD EXISTS");
			person.setId(storedPerson.getId());

			// Standardise
			person.setStdSurname(NormalizationManager.getStandardSurname(person.getSurname()));
			person.setStdGivenName(NormalizationManager.getStandardGivenName(person.getGivenName()));
			person.setStdPostcode(NormalizationManager.getStandardPostcode(person.getPostcode()));

			// Store the record
			PersonDAO.update(person);
			
			// If nationalId was previously blank, check the current national id and link as appropriate
			if (storedPerson.getNationalId()==null) {
				logger.debug("Previous record had no national id");
				updateMasterRecords(person);
			} else {
				logger.debug("Previous record had national id");
				
				// national id was previously present on this record

				// If not present on the current record then de-link
				if  (person.getNationalId()==null || person.getNationalId().length()==0) {
					logger.debug("This record has no national id - DELINK");
					delinkMasterRecords(storedPerson);
					warnDemographicMatch(person, 0);
					warnDemographicAlgorithmicMatch(person, 0);
					return;
				}
				if (nationalIdMatch(storedPerson.getNationalId(), person.getNationalId(),
									storedPerson.getNationalIdType(), person.getNationalIdType())) {
					// If no change in the national id, check for changed demographics
					logger.debug("No change in national id");
					if (demographicsChanged(person, storedPerson)) {
						logger.debug("Demographics have changed - update master and verify links");
						// get the master record
						MasterRecord master = MasterRecordDAO.findByNationalId(person.getNationalId(), person.getNationalIdType());
						// update it
						master.updateDemographics(person);
						MasterRecordDAO.update(master);
						// verify and delink all links as appropriate
						verifyLinks(master,storedPerson);
					} else {
						logger.debug("No change in demographics");
					}
				} else {
					// National Id has changed on this record, delete the old link and add the new link
					logger.debug("National Id has changed - DELINK and RELINK");
					delinkMasterRecords(storedPerson);
					updateMasterRecords(person);
				}
			}
			
		} else {
			logger.debug("NEW RECORD");
			// Standardise
			person.setStdSurname(NormalizationManager.getStandardSurname(person.getSurname()));
			person.setStdGivenName(NormalizationManager.getStandardGivenName(person.getGivenName()));
			person.setStdPostcode(NormalizationManager.getStandardPostcode(person.getPostcode()));
			
			// Store the record
			PersonDAO.create(person);
			
			updateMasterRecords(person);
			
		}
		
	}

	/**
	 * Checks for and links to a master record. If none found then one may be created
	 * 
	 * @param person
	 */
	private void updateMasterRecords(Person person) throws MpiException {

		logger.debug("UpdateMasterRecords");
		
		if ((person.getNationalIdType() != null) && (person.getNationalId() != null)) {
			logger.debug("National Id found on the incoming record");
			
			MasterRecord master = MasterRecordDAO.findByNationalId(person.getNationalId(), person.getNationalIdType());
			if (master != null) {
				
				logger.debug("Master found for this national id. MASTERID:"+master.getId());
				// a master record exists for this national id
				// Verify that the details match
				if (verifyMatch(person, master)) {
					logger.debug("Record verified - creating link");
					LinkRecord link = new LinkRecord(master.getId(), person.getId());
					LinkRecordDAO.create(link);
				} else {
					logger.debug("Record not verified - creating work item");
					WorkItem work = new WorkItem(WorkItem.TYPE_NOLINK_DEMOG_NOT_VERIFIED, person.getId(), "Master Record: "+master.getId());
					WorkItemDAO.create(work);
				}
				
			} else {
				
				logger.debug("No Master found for this national id - creating it");
				
				// a master record does not exist for this national id so create one
				master = new MasterRecord(person);
				MasterRecordDAO.create(master);

				logger.debug("Linking to the new master record");
				// and link this record to it
				LinkRecord link = new LinkRecord(master.getId(), person.getId());
				LinkRecordDAO.create(link);

				warnDemographicMatch(person, master.getId());
				warnDemographicAlgorithmicMatch(person, master.getId());
				
			}
			
		} else {
			logger.debug("No National Id found on the incoming record");
			// No national id on the incoming record
			
			warnDemographicMatch(person, 0);
			warnDemographicAlgorithmicMatch(person, 0);
			
		}
		
	}
	
	private void warnDemographicMatch(Person person, int masterId) throws MpiException {

		// DQ Check - do these demogs exist for another National Id?
		List <MasterRecord> otherMasters = MasterRecordDAO.findByDemographics(person);
		for (MasterRecord otherMaster : otherMasters) {
			if (otherMaster.getId() != masterId) {
				logger.debug("Matching demographics on another Master Record - WORK");
			    WorkItem work = new WorkItem(WorkItem.TYPE_DEMOGS_MATCH_OTHER_NATIONAL_ID, person.getId(), "Master Record: "+otherMaster.getId());
			    WorkItemDAO.create(work);
			}
		}
		
	}
		
	private void warnDemographicAlgorithmicMatch(Person person, int masterId) throws MpiException {

		TraceManager tm = new TraceManager();
		TraceRequest request = new TraceRequest(person);
		request.setTraceType("AUTO");
		request.setNameSwap("N");
		TraceResponse response = tm.trace(request);
		if (response.getMatchCount() > 0) {
			if (response.getMaxWeight() > 95) {
				logger.debug("Algorithmic Matching demographics on MPI - WORK:"+response.getMaxWeight());
			    WorkItem work = new WorkItem(WorkItem.TYPE_DEMOGS_NEAR_MATCH, person.getId(), "Alg match:"+response.getMaxWeight()+". traceid:"+response.getTraceId());
			    WorkItemDAO.create(work);
			} else {
				logger.debug("Algorithmic Match below threshhold:"+response.getMaxWeight());
			}
		} else {
			logger.debug("Algorithmic Matching found no potential matches");
		}
		
	}
		
	/**
	 * Checks for and removes any links to a master record. 
	 * If, once the link is removed, a master record has no more links then delete the master record
	 * 
	 * @param person
	 */
	private void delinkMasterRecords(Person person) throws MpiException {
		
		if ((person.getNationalIdType() != null) && (person.getNationalId() != null)) {
			
			MasterRecord master = MasterRecordDAO.findByNationalId(person.getNationalId(), person.getNationalIdType());
			if (master != null) {
				
				logger.debug("Deleting the link record");
				// a master record exists for this national id
				// Delete the link from this person record
				LinkRecord link = new LinkRecord(master.getId(), person.getId());
				LinkRecordDAO.delete(link);
				
				// Any other links?
				List<Person> linkedPersons = PersonDAO.findByMasterId(master.getId());
				if (linkedPersons.size()==0) {
					logger.debug("No remaining link records - delete the master record");
					// No other records linked to this master so delete it
					MasterRecordDAO.deleteByNationalId(person.getNationalId(), person.getNationalIdType());
				} else {
					logger.debug("Other records linked - master record will not be deleted");
				}
				
			} else {
				
				// this should not happen - WARN
				logger.warn("deleteMasterRecords unable to find a master for:"+person.getNationalIdType()+":"+person.getNationalId());
			}
			
		}
		
	}
		
	/**
	 * Verifies all links to a master record and deletes if appropriate
	 * Used after the demographics on a master are updated
	 * @param person
	 */
	private void verifyLinks(MasterRecord master, Person person) throws MpiException {

		logger.debug("Verifying Links:");
		List<Person> linkedPersons = PersonDAO.findByMasterId(master.getId());
		
		for (Person linkedPerson : linkedPersons) {
			if (linkedPerson.getId()==person.getId()) {
				logger.debug("Skipping link to current person. PERSONID:"+linkedPerson.getId());
			} else {
				if (!verifyMatch(linkedPerson,master)) {
					logger.debug("Link record no longer verifies - DELINK and raise WORK. PERSONID:"+linkedPerson.getId());
					LinkRecord link = new LinkRecord(master.getId(), linkedPerson.getId());
					LinkRecordDAO.delete(link);
					
				    WorkItem work = new WorkItem(WorkItem.TYPE_DELINK_DUE_TO_CHANGED_DEMOG, linkedPerson.getId(), "Master Record: "+master.getId());
				    WorkItemDAO.create(work);
				} else {
					logger.debug("Link still valid. PERSONID:"+linkedPerson.getId());
				}
			}
		}
		
	}

	/**
	 * UKRDC specific rules, currently mirroring the National Verify NHS Number rules
	 * @param person
	 * @param master
	 * @return
	 */
	protected boolean verifyMatch(Person person, MasterRecord master) {
		
		boolean nomatch = false;
		boolean match = true;
		
		if (person.getNationalIdType() == null || person.getNationalIdType().length()==0) {
			return nomatch;
		}
		if (!person.getNationalIdType().equals(master.getNationalIdType())) {
			return nomatch;
		}
		
		if (person.getNationalId() == null || person.getNationalId().length()==0) {
			return nomatch;
		}
		if (!person.getNationalId().equals(master.getNationalId())) {
			return nomatch;
		}
		
		// National ids match so check the other demographics
		
		// If DOB Matches exactly then this is a match
		if (person.getDateOfBirth().compareTo(master.getDateOfBirth())==0) {
			return match;
		}
		
		int dobMatch = matchDobParts(person.getDateOfBirth(), master.getDateOfBirth());
		
		if (dobMatch >= 2) {
			if ( getSafeSubstring( person.getSurname(),3 ).equals(getSafeSubstring( master.getSurname(),3 )) ) {
				if ( getSafeSubstring( person.getGivenName(),1 ).equals(getSafeSubstring( master.getGivenName(),1 )) ) {
					return match;
				}
			}
		}
		
		return nomatch;
	}
	
	private boolean demographicsChanged(Person person, Person storedPerson) {
		boolean changed = true;
		boolean unchanged = false;
		
		if (person.getDateOfBirth().compareTo(storedPerson.getDateOfBirth())!=0){
			logger.debug("DOB changed");
			return changed;
		}
		if (!person.getSurname().equals(storedPerson.getSurname())) {
			logger.debug("Surname");
			return changed;
		}
		if (!person.getGivenName().equals(storedPerson.getGivenName())) {
			logger.debug("Given Name");
			return changed;
		}
		if (!person.getGender().equals(storedPerson.getGender())) {
			logger.debug("Gender");
			return changed;
		}
		
		return unchanged;
	}
		
	/*
	 * could be used by the verifyMatch method above
	 */
	protected boolean nationalIdMatch(String natId1, String natId2, String natIdType1, String natIdType2) {
		
		boolean nomatch = false;
		boolean match = true;
		
		if (natIdType1 == null || natIdType1.length()==0) return nomatch;
		if (natIdType2 == null || natIdType2.length()==0) return nomatch;
		if (!natIdType1.equals(natIdType2)) return nomatch;
		
		if (natId1 == null || natId1.length()==0) return nomatch;
		if (natId2 == null || natId2.length()==0) return nomatch;
		if (!natId1.equals(natId2)) return nomatch;
		
		return match;
	}

	protected String getSafeSubstring(String s, int length) {
		
		if (s==null) return "";
		if (length==0) return "";
				
		if (s.length() < length) return s;
		return s.substring(0, length);
		
	}

	protected int matchDobParts(Date pDate, Date mDate) {
		
		if (pDate==null) return 0;
		if (mDate==null) return 0;
		
		int matchCount = 0;

		Calendar pCal = Calendar.getInstance();
	    pCal.setTime(pDate);

	    Calendar mCal = Calendar.getInstance();
	    mCal.setTime(mDate);
	    
	    if (pCal.get(Calendar.YEAR) == mCal.get(Calendar.YEAR)) matchCount ++;
	    if (pCal.get(Calendar.MONTH) == mCal.get(Calendar.MONTH)) matchCount ++;
	    if (pCal.get(Calendar.DAY_OF_MONTH) == mCal.get(Calendar.DAY_OF_MONTH)) matchCount ++;
		
		return matchCount;
		
	}
}
