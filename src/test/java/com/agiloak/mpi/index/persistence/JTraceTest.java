package com.agiloak.mpi.index.persistence;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class JTraceTest {

	public static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

	protected static java.util.Date getDate(String sDate) {
		
		java.util.Date uDate = null;
	    try {
		   uDate = formatter.parse(sDate);
		} catch (ParseException e) {
			e.printStackTrace();
			assert(false);
		}	
	    return uDate;
	    
	}

	public JTraceTest() {
		super();
	}

}