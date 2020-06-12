package com.agiloak.mpi.index;

import java.sql.Connection;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.audit.Audit;
import com.agiloak.mpi.audit.AuditManager;
import com.agiloak.mpi.audit.persistence.AuditDAO;
import com.agiloak.mpi.index.persistence.LinkRecordDAO;
import com.agiloak.mpi.index.persistence.MasterRecordDAO;
import com.agiloak.mpi.index.persistence.PersonDAO;
import com.agiloak.mpi.index.persistence.PidXREFDAO;
import com.agiloak.mpi.normalization.NormalizationManager;
import com.agiloak.mpi.workitem.WorkItemManager;
import com.agiloak.mpi.workitem.WorkItemType;

public class UKRDCIndexManager {
	
	private final static Logger logger = LoggerFactory.getLogger(UKRDCIndexManager.class);
	public static SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd");
	
	/**
	 * Allow manual linking of records
	 * @param patientId
	 * @param masterId
	 * @param user
	 * @param linkCode
	 * @param linkDesc
	 */
	protected void linkInternal(Connection conn, int personId, int masterId, String user, int linkCode, String linkDesc) throws MpiException {

		if (personId==0 || masterId==0 || linkCode==0 || user==null || user.length()==0 || 
			linkDesc==null || linkDesc.length()==0) {
			// LT1-2
			logger.error("Incomplete parameters for link");
			throw new MpiException("Incomplete parameters for link");
		}
		
		LinkRecord link = LinkRecordDAO.find(conn, masterId, personId);
		
		if (link!=null) {
			// LT1-3
			// TODO - Consider if this error is appropriate or if this should be idempotent
			logger.error("Link already exists");
			throw new MpiException("Link already exists");
		}

		LinkRecord ukrdcLink = LinkRecordDAO.findByPersonAndType(conn, personId, NationalIdentity.UKRDC_TYPE);
		if (ukrdcLink!=null) {
			// LT1-4
			
			int oldMaster = ukrdcLink.getMasterId();
			// Drop the prior link
			LinkRecordDAO.delete(conn, ukrdcLink);
			// And if no links remain to the master, drop the master
			List<LinkRecord> remainingLinks = LinkRecordDAO.findByMaster(conn, oldMaster);
			if (remainingLinks.size() == 0) {
				logger.debug("Master has no remaining links so delete");
				MasterRecordDAO.delete(conn, oldMaster);
			} else {
				logger.debug("Master has remaining linked records so will not be deleted");
			}
		}
		
		// LT1-1
		link = new LinkRecord(masterId, personId);
		link.setUpdatedBy(user).setLinkCode(linkCode).setLinkDesc(linkDesc);
		link.setLinkType(LinkRecord.MANUAL_TYPE);
		LinkRecordDAO.create(conn, link);
		
		AuditManager am = new AuditManager();
		am.create(conn, Audit.MANUAL_LINK, personId, masterId, linkDesc, user);
		
	}
	
	protected void unlinkInternal(Connection conn, int personId, int masterId, String user, String reason) throws MpiException {

		LinkRecord link = LinkRecordDAO.find(conn, masterId, personId);
		
		if (link==null) {
			logger.error("Link Record does not exist");
			throw new MpiException("Link Record does not exists");
		}
		LinkRecordDAO.delete(conn, link);
		AuditManager am = new AuditManager();
		am.create(conn, Audit.MANUAL_UNLINK, personId, masterId, reason, user);
	}

	protected void resetMaster(Connection conn, MasterRecord master, String user, String reason) throws MpiException {

		// Get last linked patient
		List<LinkRecord> links = LinkRecordDAO.findByMaster(conn, master.getId());
		
		// Delete if records remain linked
		if (links.size()==0) {
			MasterRecordDAO.delete(conn, master.getId());
			AuditManager am = new AuditManager();
			am.create(conn, Audit.MASTER_RECORD_DELETED_REDUNDANT, -1, master.getId(), reason, user);
		} else {
			// findByMaster is ordered so the first is the latest
			LinkRecord latestLink = links.get(0);
			Person lastLinkedPerson = PersonDAO.get(conn, latestLink.getPersonId());
			
			if (lastLinkedPerson==null) {
				logger.error("Linked Person Record does not exist");
				throw new MpiException("Linked Person Record does not exists");
			}
			
			// reset demographics from incoming record
			master.setGender(lastLinkedPerson.getGender());
			master.setDateOfBirth(lastLinkedPerson.getDateOfBirth());
			master.setGivenName(lastLinkedPerson.getGivenName());
			master.setSurname(lastLinkedPerson.getSurname());
			
			// reset status
			master.setStatus(MasterRecord.OK);
			master.setEffectiveDate(new Date());
			
			MasterRecordDAO.update(conn, master);
			AuditManager am = new AuditManager();
			am.create(conn, Audit.MASTER_RECORD_UPDATED, lastLinkedPerson.getId(), master.getId(), reason, user);
		}
			
		
	}

	/**
	 * Search for a UKRDC Identity for these patient details.
	 * @param psr
	 * @return
	 * @throws MpiException
	 */
	protected String searchInternal(Connection conn, ProgrammeSearchRequest psr) throws MpiException {
		String ukrdcId = null;
		
		if (psr==null) {
			// ST3-1
			logger.error("SearchRequest not provided");
			throw new MpiException("SearchRequest not provided");
		}
		
		if (psr.getNationalId()==null) {
			// ST2-1
			logger.error("NationalId Must be provided");
			throw new MpiException("NationalId Must be provided");
		}

		Date dob;
	    try {
		   dob = dateFormatter.parse(psr.getDateOfBirth());
		} catch (ParseException e) {
			logger.error("Invalid Date Of Birth Format");
			throw new MpiException("Invalid Date Of Birth Format");
		}	

		MasterRecord nationalMaster = MasterRecordDAO.findByNationalId(conn, psr.getNationalId().getId(), psr.getNationalId().getType());
		// If the national record is not found then we can't match to a UKRDC id
		if (nationalMaster == null) {
			// ST4-1
			logger.debug("National Identity not known");
			return null;
		}
	
		// Create a person from the request to use in the verify process
		Person searchPerson = new Person();
		searchPerson.setDateOfBirth(dob);
		searchPerson.setGivenName(psr.getGivenName());
		searchPerson.setSurname(psr.getSurname());
		
		// Get any records linked to this national id
		List<LinkRecord> links = LinkRecordDAO.findByMaster(conn, nationalMaster.getId());
		
		for (LinkRecord link : links ){
				
			// Find the UKRDC Master linked to this person (if any)
			LinkRecord ukrdcLink = LinkRecordDAO.findByPersonAndType(conn, link.getPersonId(), NationalIdentity.UKRDC_TYPE);
			
			if (ukrdcLink != null) {
				
				// If found - we have identified a Person linked to a UKRDC Number AND by National Id to the incoming Person
				//            Does the incoming Person verify against the UKRDC Master
				MasterRecord ukrdcMaster = MasterRecordDAO.get(conn, ukrdcLink.getMasterId());
				
				boolean verified = verifyMatch(searchPerson, ukrdcMaster);
				if (verified) {
					// ST1-1 / ST1-3
					ukrdcId = ukrdcMaster.getNationalId();
				} else {
					// ST1-2 / ST1-4 / ST1-5
				}
		
			} else {
				// ST5-1 == CANT HAPPEN FOLLOWING CHANGE TO ALLOCATE IF NOT MATCHED (V1.1.0)
				logger.debug("No UKRDC Number found for person linked by National Id");
			}

		}
		
		return ukrdcId;
	}

	protected void validateInternal(Person person) throws MpiException {
		
		if (person.getPrimaryIdType()!=null && person.getPrimaryIdType() != NationalIdentity.UKRDC_TYPE) {
			logger.error("If provided, the primaryIdType must be UKRDC");
			throw new MpiException("Invalid primary id type");
		}
		if (person.getSurname()==null || person.getSurname().length() < 2) {
			logger.error("Surname must be at least 2 characters");
			throw new MpiException("Surname must be at least 2 characters");
		}
		if (person.getGivenName()==null || person.getGivenName().length() < 1) {
			logger.error("Given Name must be at least 1 charactera");
			throw new MpiException("Given Name must be at least 1 character");
		}
		if (person.getGender()==null || person.getGender().length() < 1) {
			logger.error("Gender must be at least 1 character");
			throw new MpiException("Gender must be at least 1 character");
		}
		if (person.getDateOfBirth()==null) {
			logger.error("Date Of Birth is mandatory");
			throw new MpiException("Date Of Birth is mandatory");
		}
		if (person.getLocalId()==null || person.getLocalId().length() < 1) {
			logger.error("LocalId must be at least 1 character");
			throw new MpiException("LocalId must be at least 1 character");
		}
		if (person.getLocalIdType()==null || person.getLocalIdType().length() < 1) {
			logger.error("Local Id Type must be present");
			throw new MpiException("LocalId type must be present");
		}
		if (person.getOriginator()==null || person.getOriginator().length() < 1) {
			logger.error("Originator must be present");
			throw new MpiException("Originator must be present");
		}
	}
	
	protected void validateWithEMPI(Connection conn, Person person) throws MpiException {

		if (person.isSkipDuplicateCheck()) return;

		for (NationalIdentity natId : person.getNationalIds()) {
			
			// For each National Identity - see if already used for this org.
			validateNationalIdWithEMPI(conn, person, natId.getId(), natId.getType());
		}			

	}
	
	private void validateNationalIdWithEMPI(Connection conn, Person person, String id, String type) throws MpiException {

		// This check needs to ignore the same patient record when detecting duplicates, so pick up the id from the MPI (where it exists)
		Person storedPerson = PersonDAO.findByLocalId(conn, person.getLocalIdType(), person.getLocalId(), person.getOriginator());
		if (storedPerson != null) {
			person.setId(storedPerson.getId());
		}
		
		MasterRecord master = MasterRecordDAO.findByNationalId(conn, id, type);
		if (master != null) {
			
			// Check for duplicates for this Originator and Master
			int count = LinkRecordDAO.countByMasterAndOriginatorExcludingPid(conn, master.getId(), person.getOriginator(), person.getId());
			if (count > 0) {
				String errorMsg = "Another record from Unit:"+person.getOriginator()+" already linked to master:"+master.getId(); 
				logger.error(errorMsg);
				throw new MpiException(errorMsg);
			}
		}

	}
	
	protected void standardise(Person person) throws MpiException {
		
		person.setGivenName(person.getGivenName().trim().toUpperCase());
		person.setSurname(person.getSurname().trim().toUpperCase());
		person.setGender(person.getGender().trim().toUpperCase());
		
		// optional fields
		if (person.getPostcode()!=null) {
			String postcode = person.getPostcode();
			postcode = postcode.trim().toUpperCase().replaceAll(" ", "");
			if ((postcode.length() >=5) && (postcode.length() <=7)) {
				int spacePosition = postcode.length()-3;
				person.setPostcode(postcode.substring(0, spacePosition)+" "+postcode.substring(spacePosition));
			} else {
				person.setPostcode(postcode);
			}
		}
		if (person.getTitle() != null) {
			person.setTitle(person.getTitle().trim().toUpperCase());
		}
		if (person.getOtherGivenNames()!=null) {
			person.setOtherGivenNames(person.getOtherGivenNames().trim().toUpperCase());
		}
		if (person.getStreet()!=null) {
			person.setStreet(person.getStreet().trim().toUpperCase());
		}

	}

	/*
	 * API FUNCTIONS
	 */

	public UKRDCIndexManagerResponse validate(Person person) {
		ValidateCommand cmd = new ValidateCommand(person);
		UKRDCIndexManagerResponse resp = cmd.executeAPICommand(this);
		return resp;
	}

	public UKRDCIndexManagerResponse store(Person person) {
		StoreCommand cmd = new StoreCommand(person);
		UKRDCIndexManagerResponse resp = cmd.executeAPICommand(this);
		return resp;
	}

	public UKRDCIndexManagerResponse getUKRDCId(int masterId) {
		GetUKRDCIdCommand cmd = new GetUKRDCIdCommand(masterId);
		UKRDCIndexManagerResponse resp = cmd.executeAPICommand(this);
		return resp;
	}

	public UKRDCIndexManagerResponse merge(int superceedingId, int supercededId) {
		MergeCommand cmd = new MergeCommand(superceedingId, supercededId);
		UKRDCIndexManagerResponse resp = cmd.executeAPICommand(this);
		return resp;		
	}

	/**
	 * Allow manual unlinking of records and optionally reset of Master Demographics
	 * @param patientId
	 * @param masterId
	 * @param user - user requesting the update
	 * @param reason - reason for the update
	 */
	public UKRDCIndexManagerResponse unlink(int personId, int masterId, String user, String reason) {
		UnlinkCommand cmd = new UnlinkCommand(personId, masterId, user, reason);
		UKRDCIndexManagerResponse resp = cmd.executeAPICommand(this);
		return resp;		
	}

	public UKRDCIndexManagerResponse search(ProgrammeSearchRequest psr) {
		SearchCommand cmd = new SearchCommand(psr);
		UKRDCIndexManagerResponse resp = cmd.executeAPICommand(this);
		return resp;
	}

	public UKRDCIndexManagerResponse link(int personId, int masterId, String user, int linkCode, String linkDesc) {
		LinkCommand cmd = new LinkCommand(personId, masterId, user, linkCode, linkDesc);
		UKRDCIndexManagerResponse resp = cmd.executeAPICommand(this);
		return resp;
	}

	public UKRDCIndexManagerResponse setLocalPID(Person person, String sendingFacility, String sendingExtract) {
		SetLocalPidCommand cmd = new SetLocalPidCommand(person, sendingFacility, sendingExtract) ;
		UKRDCIndexManagerResponse resp = cmd.executeAPICommand(this);
		return resp;
	}

	public UKRDCIndexManagerResponse getLocalPID(Person person, String sendingFacility, String sendingExtract) {
		GetLocalPidCommand cmd = new GetLocalPidCommand(person, sendingFacility, sendingExtract) ;
		UKRDCIndexManagerResponse resp = cmd.executeAPICommand(this);
		return resp;
	}


	/*
	 * API - END
	 */
	
	
	protected String setLocalPIDInternal(Connection conn, Person person, String sendingFacility, String sendingExtract) throws MpiException {
		String pid = null;
		boolean matchesFound = false;
		
		PidXREF xref = PidXREFDAO.findByLocalId(conn, sendingFacility, sendingExtract, person.getUnconsolidatedLocalId());
		if (xref!=null) {
			return xref.getPid();
		}
		
		for (NationalIdentity nid : person.getNationalIds()) {

			//  Look for this NI linked to a local id for this SF AND SE - looking for situations where only the number has changed.
			//  Person joined to LR joined to MR identified by NI and joined to XREF with this SF and SE. New code in LinkRecordDAO
			
			List<Person> matchPersons = PidXREFDAO.FindByNationalIdAndFacility(conn, sendingFacility, sendingExtract, nid.getType(), nid.getId());
			
			for (Person potentialMatch : matchPersons) { 
				matchesFound = true;
				
				// Verify full demographics - Look for 100% match with inbound record on GENDER, DOB, SURNAME, FORENAME
				
				int dobMatch = matchDobParts(person.getDateOfBirth(), potentialMatch.getDateOfBirth());
				boolean matched = person.getGender().equals(potentialMatch.getGender()) &&
						(dobMatch==3) &&
						person.getSurname().equals(potentialMatch.getSurname()) &&
						person.getGivenName().equals(potentialMatch.getGivenName()) ;
				
				// Feels inefficient, but correct - get the master id that has facilitated the match
				MasterRecord matchedMaster = MasterRecordDAO.findByNationalId(conn, nid.getId(), nid.getType());

				// Set up attributes for either the Audit or the WorkItem
				Map<String,String> attributes = new HashMap<String, String>();
				attributes.put("SF", sendingFacility);
				attributes.put("SE", sendingExtract);
				attributes.put("MRN", person.getLocalId());

				if (matched) {
					
					// Insert the PIDXREF
					PidXREF newXref = new PidXREF(sendingFacility, sendingExtract, person.getUnconsolidatedLocalId()); 
					newXref.setPid(potentialMatch.getLocalId());
					PidXREFDAO.create(conn, newXref);
					
					// Audit the Match
					Audit audit = new Audit(Audit.NEW_PIDXREF, potentialMatch.getId(), matchedMaster.getId(), "PIDXREF Match", attributes);
					AuditDAO.create(conn, audit);
					pid = newXref.getPid();
					
					// Don't process any more potential matches or we will get multiple links to the same PID
					break;

				} else {
					if ( !person.getGender().equals(potentialMatch.getGender()) ) attributes.put("Gender", person.getGender()+":"+potentialMatch.getGender());
					if ( !person.getSurname().equals(potentialMatch.getSurname()) ) attributes.put("Surname", person.getSurname()+":"+potentialMatch.getSurname());
					if ( !person.getGivenName().equals(potentialMatch.getGivenName()) ) attributes.put("GivenName", person.getGivenName()+":"+potentialMatch.getGivenName());
					if ( dobMatch < 3 ) attributes.put("DOB", dateFormatter.format(person.getDateOfBirth()) + ":" + dateFormatter.format(potentialMatch.getDateOfBirth()));

					WorkItemManager wim = new WorkItemManager();
					int personToLog = person.getId();
					if (personToLog==0) personToLog = 999999999; // Person is not known at this point
					wim.create(conn, WorkItemType.TYPE_XREF_MATCHED_NOT_VERIFIED, personToLog, matchedMaster.getId(), "Person matched by facility, extract and national id - not matched by demographics", attributes);
				}
				
			}
			
		}

		// If no matches found, this record has not been seen by the EMPI before and no local link is found so it will be allocated a new PID and linked to it
		if (!matchesFound) {
			PidXREF newXref = new PidXREF(sendingFacility, sendingExtract, person.getLocalId()); 
			PidXREFDAO.create(conn, newXref);
			pid = newXref.getPid();
		}
		
		return pid;
	}
		
	/**
	 * @param person
	 * @param sendingFacility
	 * @param sendingExtract
	 * @return
	 * @throws MpiException
	 */
	protected String getLocalPIDInternal(Connection conn, Person person, String sendingFacility, String sendingExtract) throws MpiException {
		String outcome = "NEW";
		boolean matchesFound = false;
		
		PidXREF xref = PidXREFDAO.findByLocalId(conn, sendingFacility, sendingExtract, person.getUnconsolidatedLocalId());
		if (xref!=null) {
			return xref.getPid();
		}
		
		for (NationalIdentity nid : person.getNationalIds()) {

			//  Look for this NI linked to a local id for this SF AND SE - looking for situations where only the number has changed.
			//  Person joined to LR joined to MR identified by NI and joined to XREF with this SF and SE. New code in LinkRecordDAO
			
			List<Person> matchPersons = PidXREFDAO.FindByNationalIdAndFacility(conn, sendingFacility, sendingExtract, nid.getType(), nid.getId());
			
			for (Person potentialMatch : matchPersons) { 
				matchesFound = true;
				
				// Verify full demographics - Look for 100% match with inbound record on GENDER, DOB, SURNAME, FORENAME
				
				int dobMatch = matchDobParts(person.getDateOfBirth(), potentialMatch.getDateOfBirth());
				boolean matched = person.getGender().equals(potentialMatch.getGender()) &&
						(dobMatch==3) &&
						person.getSurname().equals(potentialMatch.getSurname()) &&
						person.getGivenName().equals(potentialMatch.getGivenName()) ;
				
				if (matched) {
					// If matched - This record has not been seen by the EMPI before but it will link to another local record. 
					// return the PID from this person (this is the consolidated Id)
					return potentialMatch.getLocalId();

				} else {
					// This record has not been seen by the EMPI before but it is related by NI to another local record with different demographics. 
					// Set outcome to "REJECT", but carry on looking
					// Set up attributes for either the WorkItem or Audit as appropriate 

					// Inefficient - get the master id that has facilitated the match
					MasterRecord matchedMaster = MasterRecordDAO.findByNationalId(conn, nid.getId(), nid.getType());
					Map<String,String> attributes = new HashMap<String, String>();
					attributes.put("SF", sendingFacility);
					attributes.put("SE", sendingExtract);
					attributes.put("MRN", person.getLocalId());
					if ( !person.getGender().equals(potentialMatch.getGender()) ) attributes.put("Gender", person.getGender()+":"+potentialMatch.getGender());
					if ( !person.getSurname().equals(potentialMatch.getSurname()) ) attributes.put("Surname", person.getSurname()+":"+potentialMatch.getSurname());
					if ( !person.getGivenName().equals(potentialMatch.getGivenName()) ) attributes.put("GivenName", person.getGivenName()+":"+potentialMatch.getGivenName());
					if ( dobMatch < 3 ) attributes.put("DOB", dateFormatter.format(person.getDateOfBirth()) + ":" + dateFormatter.format(potentialMatch.getDateOfBirth()));

					WorkItemManager wim = new WorkItemManager();
					int personToLog = person.getId();
					if (personToLog==0) personToLog = 999999999; // Person is not known at this point
					wim.create(conn, WorkItemType.TYPE_XREF_MATCHED_NOT_VERIFIED, personToLog, matchedMaster.getId(), "Person matched by facility, extract and national id - not matched by demographics", attributes);

					outcome = "REJECT";
				}
				
			}
			
		}
		// If no matches found, this record has not been seen by the EMPI before and no local link is found so it will be allocated a new PID on update
		if (!matchesFound) {
			outcome = "NEW";
		}
		
		return outcome;
	}
	
	/**
	 *  Created for the UKRDC this is a combined createOrUpdate. This method updates the person, the master (as appropriate) and the link records ( as appropriate)
	 *  
	 * @param person
	 */
	protected NationalIdentity createOrUpdate(Connection conn, Person person) throws MpiException {

		logger.debug("*********Processing person record:"+person);
		NationalIdentity ukrdcId = null;
		boolean nationalIdSetChanged = false;
		
		if (person.getEffectiveDate()==null) {
			person.setEffectiveDate(new Date());
		}
		
		validateInternal(person);
		
		standardise(person);
		
		Person storedPerson = PersonDAO.findByLocalId(conn, person.getLocalIdType(), person.getLocalId(), person.getOriginator());
		if (storedPerson != null) {
			logger.debug("RECORD EXISTS:"+storedPerson.getId());
			person.setId(storedPerson.getId());

			// Standardise
			person.setStdSurname(NormalizationManager.getStandardSurname(person.getSurname()));
			person.setStdGivenName(NormalizationManager.getStandardGivenName(person.getGivenName()));
			person.setStdPostcode(NormalizationManager.getStandardPostcode(person.getPostcode()));
			if (!person.getSurname().equals(storedPerson.getSurname())) {
				// TEST:UT1-1
				person.setPrevSurname(storedPerson.getSurname());
				person.setStdPrevSurname(NormalizationManager.getStandardSurname(person.getPrevSurname()));
			}

			// Store the record
			PersonDAO.update(conn, person);
			
			// Remove any national identifiers which are no longer in the person record (expensive!)
			List<LinkRecord> links = LinkRecordDAO.findByPerson(conn, storedPerson.getId());
			List<String> processed = new ArrayList<String>();
			for (LinkRecord link : links) {
				logger.debug("National Id link exists on db - comparing to inbound record.");

				MasterRecord master = MasterRecordDAO.get(conn, link.getMasterId());
				
				// Don't process UKRDC links here - these will be updated later
				if (!master.getNationalIdType().equals(NationalIdentity.UKRDC_TYPE) ){
					// TEST:UT4-2

					boolean found = false;
					
					for (NationalIdentity natId : person.getNationalIds()) {
						// if the National Identifier in this link is on the person record (id and type) then update it
						// If the type is there but the id is different then this is the same as not found. Let it be deleted then recreated correctly 
						if (nationalIdMatch(master.getNationalId(), natId.getId(), master.getNationalIdType(), natId.getType()) ) {
							// TEST:UT4-2 [NHS record]
							logger.debug("A national link record on the database was matched with the inbound record - updating the link as required");
							found = true;
							updateNationalIdLinks(conn, person, storedPerson, master, link);
							processed.add(natId.getType());
						}
					}			

					if (!found) {
						// TEST:UT4-2 [CHI record]
						logger.debug("A national link record on the database is not on this inbound record - delete the link");
						nationalIdSetChanged = true;
						// The Link on the database is not in the new record so delete the old link. If no other links to this master then delete the master
						LinkRecordDAO.delete(conn, link);
						List<LinkRecord> remainingLinks = LinkRecordDAO.findByMaster(conn, master.getId());
						if (remainingLinks.size() == 0) {
							// TEST:UT4-2
							logger.debug("No links remain to the master - deleting the master");
							MasterRecordDAO.delete(conn, master);
						} else {
							// TEST:UT4-??
							logger.debug("Links remain to the master so the master will not be deleted");
						}
					} else {
						logger.debug("A national link record on the database was matched with the inbound record");
					}
					
				} else {
					// TEST:UT4-2 [UKRDC record]
					logger.debug("Skipping UKRDC link in National Id processing");
				}
			}

			// Update any national identifiers not marked as processed - these will be new links so process as for a new record
			for (NationalIdentity natId : person.getNationalIds()) {
				if (!processed.contains(natId.getType())) {
					// TEST:UT4-1
					// TEST:UT4-2
					nationalIdSetChanged = true;
					createNationalIdLinks(conn, person, natId.getId(), natId.getType());
				}
			}			
			
			// Update the UKRDC link
			if (nationalIdSetChanged) {
//				ukrdcId = createUKRDCLink(conn, person);
				System.out.println("NationalIdSetChanged");
				ukrdcId = updateUKRDCLink(conn, person, storedPerson);			
			} else {
				System.out.println("NationalIdSet NOT Changed");
				ukrdcId = updateUKRDCLink(conn, person, storedPerson);			
			}
			
		} else {
			logger.debug("NEW RECORD");
			// Standardise
			person.setStdSurname(NormalizationManager.getStandardSurname(person.getSurname()));
			person.setStdGivenName(NormalizationManager.getStandardGivenName(person.getGivenName()));
			person.setStdPostcode(NormalizationManager.getStandardPostcode(person.getPostcode()));
			
			// Store the record
			PersonDAO.create(conn, person);
			
			// Link to other national identifiers as required
			for (NationalIdentity natId : person.getNationalIds()) {
				createNationalIdLinks(conn, person, natId.getId(), natId.getType());
			}			
			
			// Link to the UKRDC number
			ukrdcId = createUKRDCLink(conn, person);
			
		}
		
		return ukrdcId;
		
	}
	/**
	 * Checks for and links to a master record. If none found then one may be created
	 * 
	 * @param person
	 */
	private NationalIdentity updateUKRDCLink(Connection conn, Person person, Person storedPerson) throws MpiException {

		logger.debug("updateUKRDCLink");
		NationalIdentity ukrdcIdentity = null;
		
		// Find current link to a UKRDC Master, if it exists
		LinkRecord masterLink = LinkRecordDAO.findByPersonAndType(conn, person.getId(), NationalIdentity.UKRDC_TYPE);
		if (masterLink == null) {
			// No current link - process as for new record
			// TEST:UT1-1
			ukrdcIdentity = createUKRDCLink(conn, person);
		} else {
			// UKRDC master exists for this record - get the details
			MasterRecord ukrdcMaster = MasterRecordDAO.get(conn, masterLink.getMasterId());
			// If no primary id on the incoming record OR it is the same as currently stored then just reverify
			if ( (person.getPrimaryId()==null) || 
					(nationalIdMatch(ukrdcMaster.getNationalId(), person.getPrimaryId(), ukrdcMaster.getNationalIdType(), person.getPrimaryIdType())) ) {
				// TEST:UT2-2  (no primary id)
				// TEST:UT2-2A (primary id the same as stored)
				ukrdcIdentity = new NationalIdentity(ukrdcMaster.getNationalId());
				
				// If the demographics have changed 
				if (demographicsChanged(person, storedPerson)) {
					// TEST:UT2-3
					if (person.getEffectiveDate().compareTo(ukrdcMaster.getEffectiveDate())>0) {
						// If the effective date is later on the inbound record - update the record and reverify
						// TEST:UT2-3
						logger.debug("Demographics have changed - update master and verify links");
						ukrdcMaster.updateDemographics(person);
						MasterRecordDAO.update(conn, ukrdcMaster);
						verifyLinks(conn, ukrdcMaster, person);
					} else {
						// TEST:UT2-4
						logger.debug("Demographics have changed but record older than current master - no update to master");
						// If the effective date is not later then just reverify this person against the master and raise a work item if required
						if (!verifyMatch(person,ukrdcMaster)) {
							// TEST:UT2-5
							logger.debug("Record no longer verifies with master");
							WorkItemManager wim = new WorkItemManager();
							Map<String,String> attributes = getVerifyAttributes(person, ukrdcMaster);
							wim.create(conn, WorkItemType.TYPE_STALE_DEMOGS_NOT_VERIFIED_PRIMARY, person.getId(), ukrdcMaster.getId(), "Stale Demographics Not Verified Against PrimaryId", attributes);

							ukrdcMaster.setStatus(MasterRecord.INVESTIGATE);
							MasterRecordDAO.update(conn, ukrdcMaster);
						}
					}
				} else {
					// TEST:UT2-2
					logger.debug("Demographics have not changed - no updates to master or links");
				}
			} else {
				// TEST:UT3-1 
				logger.debug("Primary Id has changed - delete the link and re-add");
				// If primary id is different
				// Drop the prior link
				LinkRecordDAO.delete(conn, masterLink);
				// And if no links remain to the master, drop the master
				List<LinkRecord> remainingLinks = LinkRecordDAO.findByMaster(conn, ukrdcMaster.getId());
				if (remainingLinks.size() == 0) {
					// TEST:UT3-1 
					logger.debug("Master has no remaining links so delete");
					MasterRecordDAO.delete(conn, ukrdcMaster);
				} else {
					// TEST:UT3-2
					logger.debug("Master has remaining linked records so will not be deleted");
				}
				// Create the UKRDC link as for a new person
				ukrdcIdentity = createUKRDCLink(conn, person);
			}
		}
		
		return ukrdcIdentity;

	}

	/**
	 * Checks for and links to a master record. If none found then one may be created
	 * 
	 * @param person
	 */
	protected NationalIdentity createUKRDCLink(Connection conn, Person person) throws MpiException {

		logger.debug("createUKRDCLink");

		NationalIdentity ukrdcIdentity = null;
		
		if ((person.getPrimaryIdType() != null) && (person.getPrimaryId() != null)) {
			logger.debug("Primary Id found on the incoming record");
			ukrdcIdentity = new NationalIdentity(person.getPrimaryId());

			MasterRecord master = MasterRecordDAO.findByNationalId(conn, person.getPrimaryId(), person.getPrimaryIdType());
			if (master != null) {
				
				logger.debug("Master found for this Primary id. MASTERID:"+master.getId());
				// a master record exists for this Primary id
				// Verify that the details match
				if (verifyMatch(person, master)) {
					logger.debug("Record verified - creating link");
					LinkRecord link = new LinkRecord(master.getId(), person.getId());
					LinkRecordDAO.create(conn, link);
					if (master.getEffectiveDate().compareTo(person.getEffectiveDate()) < 0 ) {
						logger.debug("TEST:T4-2");
						master.updateDemographics(person);
						MasterRecordDAO.update(conn, master);
					} else {
						logger.debug("TEST:T4-3");						
					}
				} else {
					logger.debug("Record not verified - creating work item, link and mark master for investigation");
					WorkItemManager wim = new WorkItemManager();
					Map<String,String> attributes = getVerifyAttributes(person, master);
					wim.create(conn, WorkItemType.TYPE_CLAIMED_LINK_NOT_VERIFIED_PRIMARY, person.getId(), master.getId(), "Claimed Link to Primary Id Not Verified", attributes);

					LinkRecord link = new LinkRecord(master.getId(), person.getId());
					LinkRecordDAO.create(conn, link);
					master.setStatus(MasterRecord.INVESTIGATE);
					if (master.getEffectiveDate().compareTo(person.getEffectiveDate()) < 0 ) {
						logger.debug("TEST:T5-2");
						master.updateDemographics(person);
					} else {
						logger.debug("TEST:T5-3");						
					}
					MasterRecordDAO.update(conn, master);
				}

			} else {
				// TEST:UT2-1
				
				logger.debug("No Master found for this Primary id - creating it");
				
				// a master record does not exist for this Primary id so create one
				master = new MasterRecord(person);		
				MasterRecordDAO.create(conn, master);
				
				logger.debug("Linking to the new master record");
				// and link this record to it
				LinkRecord link = new LinkRecord(master.getId(), person.getId());
				LinkRecordDAO.create(conn, link);
				
			}
			
		} else {
			logger.debug("No Primary Id found on the incoming record");
			// No Primary id on the incoming record - try to match using another national id to corroborate
			
			// For each national id on this record
			MasterRecord master = null;
			MasterRecord ukrdcMaster = null;
			boolean linked = false;
			for (NationalIdentity natId : person.getNationalIds()) {
				
				// get the master for this national id (e.g. NHS Number)
				master = MasterRecordDAO.findByNationalId(conn, natId.getId(), natId.getType());
				if (master!=null) {
					
					// Get any records linked to this national id
					List<LinkRecord> links = LinkRecordDAO.findByMaster(conn, master.getId());
					
					for (LinkRecord link : links ){
						// Ignore the link to the record being processed
						if (link.getPersonId()!= person.getId()) {
							
							// Find the UKRDC Master linked to this person (if any)
							LinkRecord ukrdcLink = LinkRecordDAO.findByPersonAndType(conn, link.getPersonId(), NationalIdentity.UKRDC_TYPE);
							
							if (ukrdcLink != null) {
								
								// If found - we have identified a Person linked to a UKRDC Number AND by National Id to the incoming Person
								//            Does the incoming Person verify against the UKRDC Master
								ukrdcMaster = MasterRecordDAO.get(conn, ukrdcLink.getMasterId());
								boolean verified = verifyMatch(person, ukrdcMaster);
								if (verified) {
									logger.debug("Linking to the found and verified master record");
									LinkRecord newLink = new LinkRecord(ukrdcMaster.getId(), person.getId());
									LinkRecordDAO.create(conn, newLink);
									
									// Audit the creation of the new UKRDC Number
									AuditManager am = new AuditManager();
									Map<String,String> attr = new HashMap<String, String>();
									attr.put("NationalIdType", natId.getType());
									attr.put("NationalId", natId.getId());

									am.create(conn, Audit.NEW_MATCH_THROUGH_NATIONAL_ID, person.getId(), ukrdcMaster.getId(),"Matched By National Id", null, attr);

									linked = true;
									break; //Only want to link once
								} else {
									logger.debug("Link to potential UKRDC Match not verified");
									WorkItemManager wim = new WorkItemManager();
									Map<String,String> attributes = getVerifyAttributes(person, ukrdcMaster);
									wim.create(conn, WorkItemType.TYPE_INFERRED_LINK_NOT_VERIFIED_PRIMARY, person.getId(), ukrdcMaster.getId(), "Link to Inferred PrimaryId not verified", attributes);
								}
							}
						}
					}
				}
			}		
			
			// If the record was not linked then allocate
			if (!linked) {
				logger.debug("No link found - allocating a new UKRDC Number");
				
				String ukrdcId = MasterRecordDAO.allocate(conn);
				
				// a master record does not exist for this Primary id so create one
				ukrdcMaster = new MasterRecord(person);
				ukrdcMaster.setNationalId(ukrdcId);
				ukrdcMaster.setNationalIdType(NationalIdentity.UKRDC_TYPE);
				MasterRecordDAO.create(conn, ukrdcMaster);

				// Audit the creation of the new UKRDC Number
				AuditManager am = new AuditManager();
				Map<String,String> attr = new HashMap<String, String>();
				attr.put("NationalIdType", NationalIdentity.UKRDC_TYPE);
				attr.put("NationalId", ukrdcMaster.getNationalId());

				am.create(conn, Audit.NO_MATCH_ASSIGN_NEW, person.getId(), ukrdcMaster.getId(), "UKRDC Number Allocated", null, attr);

				logger.debug("Linking to the new master record");
				// and link this record to it
				LinkRecord link = new LinkRecord(ukrdcMaster.getId(), person.getId());
				LinkRecordDAO.create(conn, link);
			}
			
			ukrdcIdentity = new NationalIdentity(ukrdcMaster.getNationalId());
		}
		
		return ukrdcIdentity;
		
	}
	
	/**
	 * Checks for and links to a master record. If none found then one may be created
	 * 
	 * @param person
	 */
	private void createNationalIdLinks(Connection conn, Person person, String id, String type) throws MpiException {

		logger.debug("createNationalIdLinks");
		
		if ((type == null) || (id == null)) {
			logger.error("No Id set");
			throw new MpiException("NationalId not set");
		}
			
		MasterRecord master = MasterRecordDAO.findByNationalId(conn, id, type);
		if (master != null) {
			
			logger.debug("Master found for this national id. MASTERID:"+master.getId());
			// a master record exists for this national id so link to it
			LinkRecord link = new LinkRecord(master.getId(), person.getId());
			LinkRecordDAO.create(conn, link);
			
			// Verify that the details match
			if (!verifyMatch(person, master)) {
				logger.debug("Record not verified - creating a work item and mark the master for invesigation");
				WorkItemManager wim = new WorkItemManager();
				Map<String,String> attributes = getVerifyAttributes(person, master);
				wim.create(conn, WorkItemType.TYPE_CLAIMED_LINK_NOT_VERIFIED_NATIONAL, person.getId(), master.getId(), "Claimed Link to NationalId Not Verified", attributes);
				master.setStatus(MasterRecord.INVESTIGATE);
				MasterRecordDAO.update(conn, master);
			}
			
			// Update the master demographics
			if (person.getEffectiveDate().compareTo(master.getEffectiveDate())>0) {
				master.updateDemographics(person);
				MasterRecordDAO.update(conn, master);
			}
			
			// Check for duplicates for this Originator and Master (unless suppressed)
			if (person.isSkipDuplicateCheck()) {
				logger.debug("Duplicate check skipped at client request");
			}
			else {
				int count = LinkRecordDAO.countByMasterAndOriginator(conn, master.getId(), person.getOriginator());
				if (count > 1) {
					String warnMsg = "More than 1 record from Unit:"+person.getOriginator()+" linked to master:"+master.getId(); 
					logger.debug(warnMsg);
					WorkItemManager wim = new WorkItemManager();
					Map<String,String> attributes = new HashMap<String,String>();
					attributes.put("Originator", person.getOriginator());
					attributes.put(master.getNationalIdType(), master.getNationalId());

					wim.create(conn, WorkItemType.TYPE_MULTIPLE_NATID_LINKS_FROM_ORIGINATOR, person.getId(), master.getId(), warnMsg, attributes);
					master.setStatus(MasterRecord.INVESTIGATE);
					MasterRecordDAO.update(conn, master);
				}
			}

		} else {
			
			logger.debug("No Master found for this national id - creating it");
			
			// a master record does not exist for this national id so create one
			// demographics from the person
			master = new MasterRecord(person);
			// use the national id being processed
			master.setNationalId(id).setNationalIdType(type);
			MasterRecordDAO.create(conn, master);

			logger.debug("Linking to the new master record");
			// and link this record to it
			LinkRecord link = new LinkRecord(master.getId(), person.getId());
			LinkRecordDAO.create(conn, link);

		}
	}

	/**
	 * Checks for and links to a National ID master record. If none found then one may be created
	 * 
	 * @param person
	 */
	private void updateNationalIdLinks(Connection conn, Person person, Person storedPerson, MasterRecord master, LinkRecord link) throws MpiException {
	
		logger.debug("updateNationalIdLinks");
		
		if ((person == null) || (master == null) || (link==null)) {
			logger.error("Invalid parameters");
			throw new MpiException("Invalid Parameters");
		}
		
		if (demographicsChanged(person, storedPerson)) {
			// TEST:UT4-3
			logger.debug("Demographics have changed - update master and verify links");
			if (person.getEffectiveDate().compareTo(master.getEffectiveDate())>0) {
				// TEST:UT4-3
				// If the effective date is later on the inbound record - update the record and reverify
				logger.debug("Demographics have changed and this record has a later effective date - update master and verify links");
				master.updateDemographics(person);
				MasterRecordDAO.update(conn, master);
				verifyLinks(conn, master, person);
			} else {
				// TEST:UT4-4
				logger.debug("Demographics have changed but this record is stale so master is not updated. Reverify against master.");
				// If the effective date is not later then just reverify this person against the master and raise a work item if required
				if (!verifyMatch(person,master)) {
					WorkItemManager wim = new WorkItemManager();
					Map<String,String> attributes = getVerifyAttributes(person, master);
					wim.create(conn, WorkItemType.TYPE_STALE_DEMOGS_NOT_VERIFIED_NATIONAL, person.getId(), master.getId(), "Stale Demographics Not Verified Against NationalId",attributes);
					master.setStatus(MasterRecord.INVESTIGATE);
					MasterRecordDAO.update(conn, master);
				}
			}
		} else {
			// TEST:UT4-2
			logger.debug("Demographics have not changed so the master will not be updated");
		}
	
	}

	/**
	 * Verifies all links to a master record and deletes if appropriate
	 * Used after the demographics on a master are updated
	 * @param person
	 */
	private void verifyLinks(Connection conn, MasterRecord master, Person person) throws MpiException {

		logger.debug("Verifying Links:");
		List<Person> linkedPersons = PersonDAO.findByMasterId(conn, master.getId());
		
		for (Person linkedPerson : linkedPersons) {
			if (linkedPerson.getId()==person.getId()) {
				logger.debug("Skipping link to current person. PERSONID:"+linkedPerson.getId());
			} else {
				if (!verifyMatch(linkedPerson,master)) {
					logger.debug("Link record no longer verifies - Mark master for INVESTIGATION and raise WORK. PERSONID:"+linkedPerson.getId());
					master.setStatus(MasterRecord.INVESTIGATE);
					MasterRecordDAO.update(conn, master);
					int type;
					String desc=null;
					if (master.getNationalIdType().equals(NationalIdentity.UKRDC_TYPE)) {
						type = WorkItemType.TYPE_DEMOGS_NOT_VERIFIED_AFTER_PRIMARY_UPDATE;
						desc = "Demographics Not Verified Following Update of Primary Id";
					} else {
						type = WorkItemType.TYPE_DEMOGS_NOT_VERIFIED_AFTER_NATIONAL_UPDATE;
						desc = "Demographics Not Verified Following Update of National Id";
					}
					WorkItemManager wim = new WorkItemManager();
					Map<String,String> attributes = getVerifyAttributes(linkedPerson, master);
					wim.create(conn, type, linkedPerson.getId(), master.getId(), desc, attributes);
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

		logger.debug("verifyMatch");

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

	protected Map<String,String> getVerifyAttributes(Person person, MasterRecord master) {
		logger.debug("getVerifyAttributes");
		Map<String,String> attributes = new HashMap<String, String>();
		if ( !person.getGender().equals(master.getGender()) ) attributes.put("Gender", person.getGender()+":"+master.getGender());
		if ( !person.getSurname().equals(master.getSurname()) ) attributes.put("Surname", person.getSurname()+":"+master.getSurname());
		if ( !person.getGivenName().equals(master.getGivenName()) ) attributes.put("GivenName", person.getGivenName()+":"+master.getGivenName());
		if (person.getDateOfBirth().compareTo(master.getDateOfBirth())!=0) 
			attributes.put("DOB", dateFormatter.format(person.getDateOfBirth()) + ":" + dateFormatter.format(master.getDateOfBirth()));
		return attributes;
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
