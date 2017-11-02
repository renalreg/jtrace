package com.agiloak.mpi.index;

public class NationalIdentity {
	 
	public static final String NHS_TYPE = "NHS";
	public static final String CHI_TYPE = "CHI";
	public static final String HSC_TYPE = "HSC";
	public static final String UKRR_TYPE = "UKRR";
	public static final String SRR_TYPE = "SRR";
	public static final String NHSBT_TYPE = "NHSBT";
	public static final String RADAR_TYPE = "RADAR";
	public static final String BAPN_TYPE = "BAPN";
	public static final String LOCALHOSP = "LOCALHOSP";

	public static final String UKRDC_TYPE = "UKRDC";

	public NationalIdentity() {
	}
	public NationalIdentity(String type, String id) {
		this.type = type;
		this.id = id;
	}

	/**
	 * Default to the UKRDC_TYPE
	 * @param id
	 */
	public NationalIdentity(String id) {
		this.type = UKRDC_TYPE;
		this.id = id;
	}
	
	private String type;
	private String id;
	public String getType() {
		return type;
	}
	public NationalIdentity setType(String type) {
		this.type = type;
		return this;
	}
	public String getId() {
		return id;
	}
	public NationalIdentity setId(String id) {
		this.id = id;
		return this;
	}
	
}
