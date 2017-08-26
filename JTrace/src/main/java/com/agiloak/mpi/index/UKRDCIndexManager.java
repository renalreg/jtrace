package com.agiloak.mpi.index;

import java.util.ArrayList;
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
		
		if (person.getEffectiveDate()==null) {
			person.setEffectiveDate(new Date());
		}
		
		if (person.getPrimaryIdType()!=null && person.getPrimaryIdType() != MasterRecord.UKRDC_TYPE) {
			logger.error("If provided, the primaryIdType must be UKRDC");
			throw new MpiException("Invalid primary id type");
		}
		
		Person storedPerson = PersonDAO.findByLocalId(person.getLocalIdType(), person.getLocalId(), person.getOriginator());
		if (storedPerson != null) {
			logger.debug("RECORD EXISTS:"+storedPerson.getId());
			person.setId(storedPerson.getId());

			// Standardise
			person.setStdSurname(NormalizationManager.getStandardSurname(person.getSurname()));
			person.setStdGivenName(NormalizationManager.getStandardGivenName(person.getGivenName()));
			person.setStdPostcode(NormalizationManager.getStandardPostcode(person.getPostcode()));
			if (!person.getSurname().equals(storedPerson.getSurname())) {
				person.setPrevSurname(storedPerson.getSurname());
				person.setStdPrevSurname(NormalizationManager.getStandardSurname(person.getPrevSurname()));
			}

			// Store the record
			PersonDAO.update(person);
			
			// Remove any national identifiers which are no longer in the person record (expensive!)
			List<LinkRecord> links = LinkRecordDAO.findByPerson(storedPerson.getId());
			List<String> processed = new ArrayList<String>();
			for (LinkRecord link : links) {
				
				MasterRecord master = MasterRecordDAO.get(link.getMasterId());
				boolean found = false;
				
				for (NationalIdentity natId : person.getNationalIds()) {
					// if the National Identifier in this link is on the person record (id and type) then update it
					// If the type is there but the id is different then this is the same as not found. Let it be deleted then recreated correctly 
					if (nationalIdMatch(master.getNationalId(), natId.getId(), master.getNationalIdType(), natId.getType()) ) {
						found = true;
						updateNationalIdLinks(person, storedPerson, master, link);
						processed.add(natId.getType());
					}
				}			

				if (!found) {
					// The Link on the database is not in the new record so delete the old link. If no other links to this master then delete the master
					LinkRecordDAO.delete(link);
					List<LinkRecord> remainingLinks = LinkRecordDAO.findByMaster(master.getId());
					if (remainingLinks.size() == 0) {
						MasterRecordDAO.delete(master);
					}
				}
			}

			// Update any national identifiers not marked as processed - these will be new links so process as for a new record
			for (NationalIdentity natId : person.getNationalIds()) {
				if (!processed.contains(natId.getType())) {
					createNationalIdLinks(person, natId.getId(), natId.getType());
				}
			}			
			
			// Update the UKRDC link
			updateUKRDCLink(person, storedPerson);
			
		} else {
			logger.debug("NEW RECORD");
			// Standardise
			person.setStdSurname(NormalizationManager.getStandardSurname(person.getSurname()));
			person.setStdGivenName(NormalizationManager.getStandardGivenName(person.getGivenName()));
			person.setStdPostcode(NormalizationManager.getStandardPostcode(person.getPostcode()));
			
			// Store the record
			PersonDAO.create(person);
			
			// Link to other national identifiers as required
			for (NationalIdentity natId : person.getNationalIds()) {
				createNationalIdLinks(person, natId.getId(), natId.getType());
			}			
			
			// Link to the UKRDC number
			createUKRDCLink(person);
			
		}
		
	}
	/**
	 * Checks for and links to a master record. If none found then one may be created
	 * 
	 * @param person
	 */
	private void updateUKRDCLink(Person person, Person storedPerson) throws MpiException {

		// Find current link to a UKRDC Master, if it exists
		LinkRecord masterLink = LinkRecordDAO.findByPersonAndType(person.getId(), MasterRecord.UKRDC_TYPE);
		if (masterLink == null) {
			// No current link - process as for new record
			createUKRDCLink(person);
		} else {
			// UKRDC master exists for this record - get the details
			MasterRecord ukrdcMaster = MasterRecordDAO.get(masterLink.getMasterId());
			// If no primary id on the incoming record OR it is the same as currently stored then just reverify
			if ( (person.getPrimaryId()==null) || 
					(nationalIdMatch(ukrdcMaster.getNationalId(), person.getPrimaryId(), ukrdcMaster.getNationalIdType(), person.getPrimaryIdType())) ) {
				
				// If the demographics have changed 
				if (demographicsChanged(person, storedPerson)) {
					if (person.getEffectiveDate().compareTo(ukrdcMaster.getEffectiveDate())>0) {
						// If the effective date is later on the inbound record - update the record and reverify
						logger.debug("Demographics have changed - update master and verify links");
						ukrdcMaster.updateDemographics(person);
						MasterRecordDAO.update(ukrdcMaster);
						verifyLinks(ukrdcMaster, person);
					} else {
						// If the effective date is not later then just reverify this person against the master and raise a work item if required
						if (!verifyMatch(person,ukrdcMaster)) {
							WorkItem work = new WorkItem(WorkItem.TYPE_INVESTIGATE_DEMOG_NOT_VERIFIED, person.getId(), "Master Record: "+ukrdcMaster.getId());
							WorkItemDAO.create(work);
							ukrdcMaster.setStatus(MasterRecord.INVESTIGATE);
							MasterRecordDAO.update(ukrdcMaster);
						}
					}
				}
			} else {
				// If primary id is different
				// Drop the prior link
				LinkRecordDAO.delete(masterLink);
				// Create the UKRDC link as for a new person
				createUKRDCLink(person);
			}
		}

	}

	/**
	 * Checks for and links to a master record. If none found then one may be created
	 * 
	 * @param person
	 */
	private void createUKRDCLink(Person person) throws MpiException {

		logger.debug("createUKRDCLink");
		
		if ((person.getPrimaryIdType() != null) && (person.getPrimaryId() != null)) {
			logger.debug("Primary Id found on the incoming record");
			
			MasterRecord master = MasterRecordDAO.findByNationalId(person.getPrimaryId(), person.getPrimaryIdType());
			if (master != null) {
				
				logger.debug("Master found for this Primary id. MASTERID:"+master.getId());
				// a master record exists for this Primary id
				// Verify that the details match
				if (verifyMatch(person, master)) {
					logger.debug("Record verified - creating link");
					LinkRecord link = new LinkRecord(master.getId(), person.getId());
					LinkRecordDAO.create(link);
					if (master.getEffectiveDate().compareTo(person.getEffectiveDate()) < 0 ) {
						logger.debug("TEST:T4-2");
						master.updateDemographics(person);
						MasterRecordDAO.update(master);
					} else {
						logger.debug("TEST:T4-3");						
					}
				} else {
					logger.debug("Record not verified - creating work item, link and mark master for investigation");
					WorkItem work = new WorkItem(WorkItem.TYPE_INVESTIGATE_DEMOG_NOT_VERIFIED, person.getId(), "Master Record: "+master.getId());
					WorkItemDAO.create(work);
					LinkRecord link = new LinkRecord(master.getId(), person.getId());
					LinkRecordDAO.create(link);
					master.setStatus(MasterRecord.INVESTIGATE);
					if (master.getEffectiveDate().compareTo(person.getEffectiveDate()) < 0 ) {
						logger.debug("TEST:T5-2");
						master.updateDemographics(person);
					} else {
						logger.debug("TEST:T5-3");						
					}
					MasterRecordDAO.update(master);
				}

			} else {
				
				logger.debug("No Master found for this Primary id - creating it");
				
				// a master record does not exist for this Primary id so create one
				master = new MasterRecord(person);		
				MasterRecordDAO.create(master);

				logger.debug("Linking to the new master record");
				// and link this record to it
				LinkRecord link = new LinkRecord(master.getId(), person.getId());
				LinkRecordDAO.create(link);

			}
			
		} else {
			logger.debug("No Primary Id found on the incoming record");
			// No Primary id on the incoming record - try to match using another national id to corroborate
			
			// For each national id on this record
			MasterRecord master;
			for (NationalIdentity natId : person.getNationalIds()) {
				
				// get the master for this national id (e.g. NHS Number)
				master = MasterRecordDAO.findByNationalId(natId.getId(), natId.getType());
				if (master!=null) {
					
					// Get any records linked to this national id
					List<LinkRecord> links = LinkRecordDAO.findByMaster(master.getId());
					
					for (LinkRecord link : links ){
						// Ignore the link to the record being processed
						if (link.getPersonId()!= person.getId()) {
							
							// Find the UKRDC Master linked to this person (if any)
							LinkRecord ukrdcLink = LinkRecordDAO.findByPersonAndType(link.getPersonId(), "UKRDC");
							
							if (ukrdcLink != null) {
								
								// If found - we have identified a Person linked to a UKRDC Number AND by National Id to the incoming Person
								//            Does the incoming Person verify against the UKRDC Master
								MasterRecord ukrdcMaster = MasterRecordDAO.get(ukrdcLink.getMasterId());
								boolean verified = verifyMatch(person, ukrdcMaster);
								if (verified) {
									logger.debug("Linking to the found and verified master record");
									LinkRecord newLink = new LinkRecord(ukrdcMaster.getId(), person.getId());
									LinkRecordDAO.create(newLink);
								} else {
									logger.debug("Link to potential UKRDC Match not verified");
									WorkItem work = new WorkItem(WorkItem.TYPE_INVESTIGATE_DEMOG_NOT_VERIFIED, person.getId(), "Link to potential UKRDC Match not verified: "+master.getId());
									WorkItemDAO.create(work);
								}
							}
						}
					}
				}
			}			
		}
		
	}
	
	/**
	 * Checks for and links to a master record. If none found then one may be created
	 * 
	 * @param person
	 */
	private void createNationalIdLinks(Person person, String id, String type) throws MpiException {

		logger.debug("createNationalIdLinks");
		
		if ((type == null) || (id == null)) {
			logger.error("No Id set");
			throw new MpiException("NationalId not set");
		}
			
		MasterRecord master = MasterRecordDAO.findByNationalId(id, type);
		if (master != null) {
			
			logger.debug("Master found for this national id. MASTERID:"+master.getId());
			// a master record exists for this national id so link to it
			LinkRecord link = new LinkRecord(master.getId(), person.getId());
			LinkRecordDAO.create(link);
			
			// Verify that the details match
			if (!verifyMatch(person, master)) {
				logger.debug("Record not verified - creating a work item and mark the master for invesigation");
				WorkItem work = new WorkItem(WorkItem.TYPE_INVESTIGATE_DEMOG_NOT_VERIFIED, person.getId(), "Master Record: "+master.getId());
				WorkItemDAO.create(work);
				master.setStatus(MasterRecord.INVESTIGATE);
				MasterRecordDAO.update(master);
			}
			
			// Update the master demographics
			if (person.getEffectiveDate().compareTo(master.getEffectiveDate())>0) {
				master.updateDemographics(person);
				MasterRecordDAO.update(master);
			}

		} else {
			
			logger.debug("No Master found for this national id - creating it");
			
			// a master record does not exist for this national id so create one
			// demographics from the person
			master = new MasterRecord(person);
			// use the national id being processed
			master.setNationalId(id).setNationalIdType(type);
			MasterRecordDAO.create(master);

			logger.debug("Linking to the new master record");
			// and link this record to it
			LinkRecord link = new LinkRecord(master.getId(), person.getId());
			LinkRecordDAO.create(link);

		}
	}

	/**
	 * Checks for and links to a National ID master record. If none found then one may be created
	 * 
	 * @param person
	 */
	private void updateNationalIdLinks(Person person, Person storedPerson, MasterRecord master, LinkRecord link) throws MpiException {

		logger.debug("updateNationalIdLinks");
		
		if ((person == null) || (master == null) || (link==null)) {
			logger.error("Invalid parameters");
			throw new MpiException("Invalid Parameters");
		}
		
		if (demographicsChanged(person, storedPerson)) {
			logger.debug("Demographics have changed - update master and verify links");
			if (person.getEffectiveDate().compareTo(master.getEffectiveDate())>0) {
				// If the effective date is later on the inbound record - update the record and reverify
				logger.debug("Demographics have changed - update master and verify links");
				master.updateDemographics(person);
				MasterRecordDAO.update(master);
				verifyLinks(master, person);
			} else {
				// If the effective date is not later then just reverify this person against the master and raise a work item if required
				if (!verifyMatch(person,master)) {
					WorkItem work = new WorkItem(WorkItem.TYPE_INVESTIGATE_DEMOG_NOT_VERIFIED, person.getId(), "Master Record: "+master.getId());
					WorkItemDAO.create(work);
					master.setStatus(MasterRecord.INVESTIGATE);
					MasterRecordDAO.update(master);
				}
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
					logger.debug("Link record no longer verifies - Mark master for INVESTIGATION and raise WORK. PERSONID:"+linkedPerson.getId());
					master.setStatus(MasterRecord.INVESTIGATE);
					MasterRecordDAO.update(master);
				    WorkItem work = new WorkItem(WorkItem.TYPE_INVESTIGATE_DUE_TO_CHANGED_DEMOG, linkedPerson.getId(), "Master Record: "+master.getId());
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
