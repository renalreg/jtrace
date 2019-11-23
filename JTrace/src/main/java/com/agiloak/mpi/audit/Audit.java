package com.agiloak.mpi.audit;

import java.lang.reflect.Type;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.agiloak.mpi.index.NationalIdentity;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class Audit {

	// ALLOCATION AUDITS
	public final static int NO_MATCH_ASSIGN_NEW = 1;
	public final static int NEW_MATCH_THROUGH_NATIONAL_ID = 2;
	
	// MERGE AUDITS
	public final static int UKRDC_MERGE = 11;
	
	// WORK ITEM AUDITS
	public final static int WORK_ITEM_CREATED = 21;
	public final static int WORK_ITEM_UPDATED = 22;

	// PIDXREF
	public final static int NEW_PIDXREF = 31;
	
	// ADMIN AUDITS
	public final static int LINK_DELETED = 41;
	public final static int MASTER_RECORD_UPDATED = 42;
	public final static int MASTER_RECORD_DELETED_REDUNDANT_ADMIN = 43;

	Gson gson = new Gson();

	public Audit(int type, int personId, int masterId, String desc, Map<String,String> attributes) {
		this.type = type;
		this.personId = personId;
		this.masterId = masterId;
		this.description = desc;
		this.lastUpdated = new Date();
		this.attributes = attributes;
	}
	
	public Audit(int type, int personId, int masterId, String desc, NationalIdentity mainNationalIdentity, Map<String,String>  attributes) {
		this.type = type;
		this.personId = personId;
		this.masterId = masterId;
		this.description = desc;
		this.lastUpdated = new Date();
		this.mainNationalIdentity = mainNationalIdentity;
		this.attributes = attributes;
	}
	
	public Audit(int type, int personId, int masterId, String desc, String natId, String natIdType, String attributes) {
		this.type = type;
		this.personId = personId;
		this.masterId = masterId;
		this.description = desc;
		this.lastUpdated = new Date();
		if (natId != null && natIdType != null) {
			NationalIdentity ni = new NationalIdentity(natIdType, natId);
			this.mainNationalIdentity = ni;
		}
	    if (attributes == null || attributes.equalsIgnoreCase("null")) {
	    	this.attributes = new HashMap<String,String>();
	    } else {
			Type reqType = new TypeToken<Map<String,String>>() {}.getType();
			Map<String,String> attrMap = gson.fromJson(attributes, reqType);
			this.attributes = attrMap;
	    }
	}

	private int id;
	private Date lastUpdated;
	private String description;
	private int personId;
	private int masterId;
	private int type;
	private String updatedBy;
	private Map<String,String> attributes;
	private NationalIdentity mainNationalIdentity;
	
	public int getId() {
		return id;
	}
	public Audit setId(int id) {
		this.id = id;
		return this;
	}
	public int getPersonId() {
		return personId;
	}
	public Audit setPersonId(int personId) {
		this.personId = personId;
		return this;
	}
	public int getMasterId() {
		return masterId;
	}
	public Audit setMasterId(int masterId) {
		this.masterId = masterId;
		return this;
	}
	public int getType() {
		return type;
	}
	public Audit setType(int type) {
		this.type = type;
		return this;
	}
	public Date getLastUpdated() {
		return lastUpdated;
	}
	public Audit setLastUpdated(Date lastUpdated) {
		this.lastUpdated = lastUpdated;
		return this;
	}
	public String getDescription() {
		return description;
	}
	public Audit setDescription(String description) {
		this.description = description;
		return this;
	}
	public String getUpdatedBy() {
		return updatedBy;
	}
	public void setUpdatedBy(String updatedBy) {
		this.updatedBy = updatedBy;
	}
	public NationalIdentity getMainNationalIdentity() {
		return mainNationalIdentity;
	}
	public void setMainNationalIdentity(NationalIdentity mainNationalIdentity) {
		this.mainNationalIdentity = mainNationalIdentity;
	}
	public Map<String,String> getAttributes() {
		return attributes;
	}
	public void setAttributes(Map<String,String> attributes) {
		this.attributes = attributes;
	}
	public String getAttributesJson() {
		Type respType = new TypeToken<Map<String,String>>() {}.getType();
        String jsonResponse = gson.toJson(this.attributes, respType);
		return jsonResponse;
	}	
}
