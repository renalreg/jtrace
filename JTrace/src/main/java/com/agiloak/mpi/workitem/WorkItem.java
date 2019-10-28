package com.agiloak.mpi.workitem;

import java.lang.reflect.Type;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class WorkItem {

	Gson gson = new Gson();

	/**
	 * @param type The type of WorkItem {@link WorkItemType}
	 * @param personId The id of the person record this refers to
	 * @param masterId The masterId that this refers to
	 * @param desc The description of the issue requiring resolution
	 */
	public WorkItem(int type, int personId, int masterId, String desc, Map<String, String> attributes) {
		this.type = type;
		this.personId = personId;
		this.masterId = masterId;
		this.description = desc;
		this.attributes = attributes;
		this.status = WorkItemStatus.STATUS_OPEN;
	}

	/**
	 * @param type The type of WorkItem {@link WorkItemType}
	 * @param personId The id of the person record this refers to
	 * @param masterId The masterId that this refers to
	 * @param desc The description of the issue requiring resolution
	 */
	public WorkItem(int type, int personId, int masterId, String desc) {
		this.type = type;
		this.personId = personId;
		this.masterId = masterId;
		this.description = desc;
		this.status = WorkItemStatus.STATUS_OPEN;
	}
	
	/**
	 * @param type The type of WorkItem {@link WorkItemType}
	 * @param personId The id of the person record this refers to
	 * @param masterId The masterId that this refers to
	 * @param desc The description of the issue requiring resolution
	 * @param attributes The attributes of the work item which vary with the circumstance
	 */
	public WorkItem(int type, int personId, int masterId, String desc, String attributes) {
		this.type = type;
		this.personId = personId;
		this.masterId = masterId;
		this.description = desc;
	    if (attributes == null || attributes.equalsIgnoreCase("null")) {
	    	this.attributes = new HashMap<String,String>();
	    } else {
			Type reqType = new TypeToken<Map<String,String>>() {}.getType();
			Map<String,String> attrMap = gson.fromJson(attributes, reqType);
			this.attributes = attrMap;
	    }
	}

	private int id;
	private Timestamp lastUpdated;
	private Timestamp creationDate;
	private String description;
	private int status;
	private int personId;
	private int masterId;
	private int type;
	private String updatedBy;
	private String updateDesc;
	private Map<String,String> attributes;

	public int getId() {
		return id;
	}
	public WorkItem setId(int id) {
		this.id = id;
		return this;
	}
	public int getPersonId() {
		return personId;
	}
	public WorkItem setPersonId(int personId) {
		this.personId = personId;
		return this;
	}
	public int getMasterId() {
		return masterId;
	}
	public WorkItem setMasterId(int masterId) {
		this.masterId = masterId;
		return this;
	}
	public int getType() {
		return type;
	}
	public WorkItem setType(int type) {
		this.type = type;
		return this;
	}
	public Timestamp getLastUpdated() {
		return lastUpdated;
	}
	public WorkItem setLastUpdated(Timestamp lastUpdated) {
		this.lastUpdated = lastUpdated;
		return this;
	}
	public Timestamp getCreationDate() {
		return creationDate;
	}

	public void setCreationDate(Timestamp creationDate) {
		this.creationDate = creationDate;
	}
	public String getDescription() {
		return description;
	}
	public WorkItem setDescription(String description) {
		this.description = description;
		return this;
	}
	public int getStatus() {
		return status;
	}
	public WorkItem setStatus(int status) {
		this.status = status;
		return this;
	}
	public String getUpdatedBy() {
		return updatedBy;
	}
	public void setUpdatedBy(String updatedBy) {
		this.updatedBy = updatedBy;
	}
	public String getUpdateDesc() {
		return updateDesc;
	}
	public void setUpdateDesc(String updateDesc) {
		this.updateDesc = updateDesc;
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
