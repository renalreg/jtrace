package com.agiloak.mpi.index;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.audit.Audit;
import com.agiloak.mpi.audit.AuditManager;
import com.agiloak.mpi.index.persistence.LinkRecordDAO;
import com.agiloak.mpi.index.persistence.MasterRecordDAO;
import com.agiloak.mpi.index.persistence.PersonDAO;
import com.agiloak.mpi.normalization.NormalizationManager;
import com.agiloak.mpi.workitem.WorkItem;
import com.agiloak.mpi.workitem.WorkItemManager;

public class UKRDCIndexManager {
	
	private final static Logger logger = LoggerFactory.getLogger(UKRDCIndexManager.class);
	public static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
	
	/**
	 * Allow manual linking of records
	 * @param patientId
	 * @param masterId
	 * @param user
	 * @param linkCode
	 * @param linkDesc
	 */
	private void linkInternal(int personId, int masterId, String user, int linkCode, String linkDesc) throws MpiException {

		if (personId==0 || masterId==0 || linkCode==0 || user==null || user.length()==0 || 
			linkDesc==null || linkDesc.length()==0) {
			// LT1-2
			logger.error("Incomplete parameters for link");
			throw new MpiException("Incomplete parameters for link");
		}
		
		LinkRecord link = LinkRecordDAO.find(masterId, personId);
		
		if (link!=null) {
			// LT1-3
			// TODO - Consider if this error is appropriate or if this should be idempotent
			logger.error("Link already exists");
			throw new MpiException("Link already exists");
		}

		LinkRecord ukrdcLink = LinkRecordDAO.findByPersonAndType(personId, NationalIdentity.UKRDC_TYPE);
		if (ukrdcLink!=null) {
			// LT1-4
			
			int oldMaster = ukrdcLink.getMasterId();
			// Drop the prior link
			LinkRecordDAO.delete(ukrdcLink);
			// And if no links remain to the master, drop the master
			List<LinkRecord> remainingLinks = LinkRecordDAO.findByMaster(oldMaster);
			if (remainingLinks.size() == 0) {
				logger.debug("Master has no remaining links so delete");
				MasterRecordDAO.delete(oldMaster);
			} else {
				logger.debug("Master has remaining linked records so will not be deleted");
			}
		}
		
		// LT1-1
		link = new LinkRecord(masterId, personId);
		link.setUpdatedBy(user).setLinkCode(linkCode).setLinkDesc(linkDesc);
		link.setLinkType(LinkRecord.MANUAL_TYPE);
		LinkRecordDAO.create(link);
		
	}
	
	/**
	 * Search for a UKRDC Identity for these patient details.
	 * @param psr
	 * @return
	 * @throws MpiException
	 */
	private String searchInternal(ProgrammeSearchRequest psr) throws MpiException {
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
		   dob = formatter.parse(psr.getDateOfBirth());
		} catch (ParseException e) {
			logger.error("Invalid Date Of Birth Format");
			throw new MpiException("Invalid Date Of Birth Format");
		}	

		MasterRecord nationalMaster = MasterRecordDAO.findByNationalId(psr.getNationalId().getId(), psr.getNationalId().getType());
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
		List<LinkRecord> links = LinkRecordDAO.findByMaster(nationalMaster.getId());
		
		for (LinkRecord link : links ){
				
			// Find the UKRDC Master linked to this person (if any)
			LinkRecord ukrdcLink = LinkRecordDAO.findByPersonAndType(link.getPersonId(), NationalIdentity.UKRDC_TYPE);
			
			if (ukrdcLink != null) {
				
				// If found - we have identified a Person linked to a UKRDC Number AND by National Id to the incoming Person
				//            Does the incoming Person verify against the UKRDC Master
				MasterRecord ukrdcMaster = MasterRecordDAO.get(ukrdcLink.getMasterId());
				
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

	private void validateInternal(Person person) throws MpiException {
		
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

	public UKRDCIndexManagerResponse validate(Person person) {
		UKRDCIndexManagerResponse resp = new UKRDCIndexManagerResponse();
		try {
			validateInternal(person);
			resp.setStatus(UKRDCIndexManagerResponse.SUCCESS);
		} catch (Exception e) {
			resp.setStatus(UKRDCIndexManagerResponse.FAIL);
			resp.setMessage(e.getMessage());
			resp.setStackTrace(ExceptionUtils.getStackTrace(e));
		}
		return resp;
	}
	
	public UKRDCIndexManagerResponse store(Person person) {
		UKRDCIndexManagerResponse resp = new UKRDCIndexManagerResponse();
		try {
			NationalIdentity natId = createOrUpdate(person);
			resp.setStatus(UKRDCIndexManagerResponse.SUCCESS);
			resp.setNationalIdentity(natId);
		} catch (Exception e) {
			resp.setStatus(UKRDCIndexManagerResponse.FAIL);
			resp.setMessage(e.getMessage());
			resp.setStackTrace(ExceptionUtils.getStackTrace(e));
		}
		return resp;
	}

	public UKRDCIndexManagerResponse search(ProgrammeSearchRequest psr) {
		UKRDCIndexManagerResponse resp = new UKRDCIndexManagerResponse();
		try {
			String ukrdcId = searchInternal(psr);
			NationalIdentity natId = new NationalIdentity(ukrdcId);
			resp.setNationalIdentity(natId);
			resp.setStatus(UKRDCIndexManagerResponse.SUCCESS);
		} catch (Exception e) {
			resp.setStatus(UKRDCIndexManagerResponse.FAIL);
			resp.setMessage(e.getMessage());
			resp.setStackTrace(ExceptionUtils.getStackTrace(e));
		}
		return resp;
	}

	public UKRDCIndexManagerResponse link(int personId, int masterId, String user, int linkCode, String linkDesc) {
		UKRDCIndexManagerResponse resp = new UKRDCIndexManagerResponse();
		try {
			linkInternal(personId, masterId, user, linkCode, linkDesc);
			resp.setStatus(UKRDCIndexManagerResponse.SUCCESS);
		} catch (Exception e) {
			resp.setStatus(UKRDCIndexManagerResponse.FAIL);
			resp.setMessage(e.getMessage());
			resp.setStackTrace(ExceptionUtils.getStackTrace(e));
		}
		return resp;
	}
	
	
	/**
	 *  Created for the UKRDC this is a combined createOrUpdate. This method updates the person, the master (as appropriate) and the link records ( as appropriate)
	 *  
	 * @param person
	 */
	private NationalIdentity createOrUpdate(Person person) throws MpiException {

		logger.debug("*********Processing person record:"+person);
		NationalIdentity ukrdcId = null;
		
		if (person.getEffectiveDate()==null) {
			person.setEffectiveDate(new Date());
		}
		
		validateInternal(person);
		
		standardise(person);
		
		Person storedPerson = PersonDAO.findByLocalId(person.getLocalIdType(), person.getLocalId(), person.getOriginator());
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
			PersonDAO.update(person);
			
			// Remove any national identifiers which are no longer in the person record (expensive!)
			List<LinkRecord> links = LinkRecordDAO.findByPerson(storedPerson.getId());
			List<String> processed = new ArrayList<String>();
			for (LinkRecord link : links) {
				logger.debug("National Id link exists on db - comparing to inbound record.");

				MasterRecord master = MasterRecordDAO.get(link.getMasterId());
				
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
							updateNationalIdLinks(person, storedPerson, master, link);
							processed.add(natId.getType());
						}
					}			

					if (!found) {
						// TEST:UT4-2 [CHI record]
						logger.debug("A national link record on the database is not on this inbound record - delete the link");
						// The Link on the database is not in the new record so delete the old link. If no other links to this master then delete the master
						LinkRecordDAO.delete(link);
						List<LinkRecord> remainingLinks = LinkRecordDAO.findByMaster(master.getId());
						if (remainingLinks.size() == 0) {
							// TEST:UT4-2
							logger.debug("No links remain to the master - deleting the master");
							MasterRecordDAO.delete(master);
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
					createNationalIdLinks(person, natId.getId(), natId.getType());
				}
			}			
			
			// Update the UKRDC link
			ukrdcId = updateUKRDCLink(person, storedPerson);
			
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
			ukrdcId = createUKRDCLink(person);
			
		}
		
		return ukrdcId;
		
	}
	/**
	 * Checks for and links to a master record. If none found then one may be created
	 * 
	 * @param person
	 */
	private NationalIdentity updateUKRDCLink(Person person, Person storedPerson) throws MpiException {

		logger.debug("updateUKRDCLink");
		NationalIdentity ukrdcIdentity = null;
		
		// Find current link to a UKRDC Master, if it exists
		LinkRecord masterLink = LinkRecordDAO.findByPersonAndType(person.getId(), NationalIdentity.UKRDC_TYPE);
		if (masterLink == null) {
			// No current link - process as for new record
			// TEST:UT1-1
			ukrdcIdentity = createUKRDCLink(person);
		} else {
			// UKRDC master exists for this record - get the details
			MasterRecord ukrdcMaster = MasterRecordDAO.get(masterLink.getMasterId());
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
						MasterRecordDAO.update(ukrdcMaster);
						verifyLinks(ukrdcMaster, person);
					} else {
						// TEST:UT2-4
						logger.debug("Demographics have changed but record older than current master - no update to master");
						// If the effective date is not later then just reverify this person against the master and raise a work item if required
						if (!verifyMatch(person,ukrdcMaster)) {
							// TEST:UT2-5
							logger.debug("Record no longer verifies with master");
							WorkItemManager wim = new WorkItemManager();
							wim.create(WorkItem.TYPE_STALE_DEMOGS_NOT_VERIFIED_PRIMARY, person.getId(), ukrdcMaster.getId(), "Stale Demographics Not Verified Against PrimaryId");

							ukrdcMaster.setStatus(MasterRecord.INVESTIGATE);
							MasterRecordDAO.update(ukrdcMaster);
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
				LinkRecordDAO.delete(masterLink);
				// And if no links remain to the master, drop the master
				List<LinkRecord> remainingLinks = LinkRecordDAO.findByMaster(ukrdcMaster.getId());
				if (remainingLinks.size() == 0) {
					// TEST:UT3-1 
					logger.debug("Master has no remaining links so delete");
					MasterRecordDAO.delete(ukrdcMaster);
				} else {
					// TEST:UT3-2
					logger.debug("Master has remaining linked records so will not be deleted");
				}
				// Create the UKRDC link as for a new person
				ukrdcIdentity = createUKRDCLink(person);
			}
		}
		
		return ukrdcIdentity;

	}

	/**
	 * Checks for and links to a master record. If none found then one may be created
	 * 
	 * @param person
	 */
	private NationalIdentity createUKRDCLink(Person person) throws MpiException {

		logger.debug("createUKRDCLink");

		NationalIdentity ukrdcIdentity = null;
		
		if ((person.getPrimaryIdType() != null) && (person.getPrimaryId() != null)) {
			logger.debug("Primary Id found on the incoming record");
			ukrdcIdentity = new NationalIdentity(person.getPrimaryId());

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
					WorkItemManager wim = new WorkItemManager();
					wim.create(WorkItem.TYPE_CLAIMED_LINK_NOT_VERIFIED_PRIMARY, person.getId(), master.getId(), "Claimed Link to Primary Id Not Verified");

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
				// TEST:UT2-1
				
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
			MasterRecord master = null;
			MasterRecord ukrdcMaster = null;
			boolean linked = false;
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
							LinkRecord ukrdcLink = LinkRecordDAO.findByPersonAndType(link.getPersonId(), NationalIdentity.UKRDC_TYPE);
							
							if (ukrdcLink != null) {
								
								// If found - we have identified a Person linked to a UKRDC Number AND by National Id to the incoming Person
								//            Does the incoming Person verify against the UKRDC Master
								ukrdcMaster = MasterRecordDAO.get(ukrdcLink.getMasterId());
								boolean verified = verifyMatch(person, ukrdcMaster);
								if (verified) {
									logger.debug("Linking to the found and verified master record");
									LinkRecord newLink = new LinkRecord(ukrdcMaster.getId(), person.getId());
									LinkRecordDAO.create(newLink);
									
									// Audit the creation of the new UKRDC Number
									AuditManager am = new AuditManager();
									am.create(Audit.NEW_MATCH_THROUGH_NATIONAL_ID, person.getId(), ukrdcMaster.getId(), 
												"Common NatID["+natId.getType()+":"+natId.getId()+"]");

									linked = true;
									break; //Only want to link once
								} else {
									logger.debug("Link to potential UKRDC Match not verified");
									WorkItemManager wim = new WorkItemManager();
									wim.create(WorkItem.TYPE_INFERRED_LINK_NOT_VERIFIED_PRIMARY, person.getId(), ukrdcMaster.getId(), "Link to Inferred PrimaryId not verified");
								}
							}
						}
					}
				}
			}		
			
			// If the record was not linked then allocate
			if (!linked) {
				logger.debug("No link found - allocating a new UKRDC Number");
				
				String ukrdcId = MasterRecordDAO.allocate();
				
				// a master record does not exist for this Primary id so create one
				ukrdcMaster = new MasterRecord(person);
				ukrdcMaster.setNationalId(ukrdcId);
				ukrdcMaster.setNationalIdType(NationalIdentity.UKRDC_TYPE);
				MasterRecordDAO.create(ukrdcMaster);

				// Audit the creation of the new UKRDC Number
				AuditManager am = new AuditManager();
				am.create(Audit.NO_MATCH_ASSIGN_NEW, person.getId(), ukrdcMaster.getId(), "UKRDC Number Allocated");

				logger.debug("Linking to the new master record");
				// and link this record to it
				LinkRecord link = new LinkRecord(ukrdcMaster.getId(), person.getId());
				LinkRecordDAO.create(link);
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
				WorkItemManager wim = new WorkItemManager();
				wim.create(WorkItem.TYPE_CLAIMED_LINK_NOT_VERIFIED_NATIONAL, person.getId(), master.getId(), "Claimed Link to NationalId Not Verified");
				master.setStatus(MasterRecord.INVESTIGATE);
				MasterRecordDAO.update(master);
			}
			
			// Update the master demographics
			if (person.getEffectiveDate().compareTo(master.getEffectiveDate())>0) {
				master.updateDemographics(person);
				MasterRecordDAO.update(master);
			}
			
			// Check for duplicates for this Originator and Master (unless suppressed)
			if (person.isSkipDuplicateCheck()) {
				logger.debug("Duplicate check skipped at client request");
			}
			else {
				int count = LinkRecordDAO.countByMasterAndOriginator(master.getId(), person.getOriginator());
				if (count > 1) {
					String warnMsg = "More than 1 record from Originator:"+person.getOriginator()+" linked to master:"+master.getId(); 
					logger.debug(warnMsg);
					WorkItemManager wim = new WorkItemManager();
					wim.create(WorkItem.TYPE_MULTIPLE_NATID_LINKS_FROM_ORIGINATOR, person.getId(), master.getId(), warnMsg);
					master.setStatus(MasterRecord.INVESTIGATE);
					MasterRecordDAO.update(master);
				}
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
			// TEST:UT4-3
			logger.debug("Demographics have changed - update master and verify links");
			if (person.getEffectiveDate().compareTo(master.getEffectiveDate())>0) {
				// TEST:UT4-3
				// If the effective date is later on the inbound record - update the record and reverify
				logger.debug("Demographics have changed and this record has a later effective date - update master and verify links");
				master.updateDemographics(person);
				MasterRecordDAO.update(master);
				verifyLinks(master, person);
			} else {
				// TEST:UT4-4
				logger.debug("Demographics have changed but this record is stale so master is not updated. Reverify against master.");
				// If the effective date is not later then just reverify this person against the master and raise a work item if required
				if (!verifyMatch(person,master)) {
					WorkItemManager wim = new WorkItemManager();
					wim.create(WorkItem.TYPE_STALE_DEMOGS_NOT_VERIFIED_NATIONAL, person.getId(), master.getId(), "Stale Demographics Not Verified Against NationalId");
					
					master.setStatus(MasterRecord.INVESTIGATE);
					MasterRecordDAO.update(master);
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
					int type;
					String desc=null;
					if (master.getNationalIdType().equals(NationalIdentity.UKRDC_TYPE)) {
						type = WorkItem.TYPE_DEMOGS_NOT_VERIFIED_AFTER_PRIMARY_UPDATE;
						desc = "Demographics Not Verified Following Update of Primary Id";
					} else {
						type = WorkItem.TYPE_DEMOGS_NOT_VERIFIED_AFTER_NATIONAL_UPDATE;
						desc = "Demographics Not Verified Following Update of National Id";
					}
					WorkItemManager wim = new WorkItemManager();
					wim.create(type, linkedPerson.getId(), master.getId(), desc);
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
