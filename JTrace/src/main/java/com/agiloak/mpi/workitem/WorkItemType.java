package com.agiloak.mpi.workitem;

public interface WorkItemType {

	public static final int TYPE_STALE_DEMOGS_NOT_VERIFIED_PRIMARY = 1;
	public static final int TYPE_CLAIMED_LINK_NOT_VERIFIED_PRIMARY = 2;
	public static final int TYPE_INFERRED_LINK_NOT_VERIFIED_PRIMARY = 3;
	public static final int TYPE_CLAIMED_LINK_NOT_VERIFIED_NATIONAL = 4;
	public static final int TYPE_STALE_DEMOGS_NOT_VERIFIED_NATIONAL = 5;
	public static final int TYPE_DEMOGS_NOT_VERIFIED_AFTER_PRIMARY_UPDATE = 6;
	public static final int TYPE_DEMOGS_NOT_VERIFIED_AFTER_NATIONAL_UPDATE = 7;
	public static final int TYPE_MULTIPLE_NATID_LINKS_FROM_ORIGINATOR = 8;

	public static final int TYPE_XREF_MATCHED_NOT_VERIFIED = 9;

}