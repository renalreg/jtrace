package com.agiloak.mpi.trace;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.junit.BeforeClass;
import org.junit.Test;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.SimpleConnectionManager;

public class TraceManagerSingleTest {

	public static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
	@BeforeClass
	public static void setup()  throws MpiException {
		SimpleConnectionManager.configure("postgres", "postgres","localhost", "5432", "JTRACE");
	}
	
	private static java.util.Date getDate(String sDate){
		
		java.util.Date uDate = null;
	    try {
		   uDate = formatter.parse(sDate);
		} catch (ParseException e) {
			e.printStackTrace();
			assert(false);
		}	
	    return uDate;
	    
	}

	@Test
	public void testTrace() throws MpiException {
		TraceResponse response;
		TraceRequest request = new TraceRequest();
		
		request.setTraceType("AUTO");
		request.setNameSwap("N");
		
		request.setLocalId("999999");
		request.setLocalIdType("TST");
		request.setOriginator("JUNIT");
		
		request.setGivenName("Nicholas");
		request.setOtherGivenNames("Ioan");
		request.setSurname("Jones");
		request.setDateOfBirthStart(getDate("1962-08-31"));
		request.setDateOfBirthEnd(getDate("1963-08-31"));
		request.setGender("1");
		request.setPostcode("CH1 6LB");
		request.setStreet("Oakdene, Townfield Lane");

		TraceManager tracer = new TraceManager();
		
		response = tracer.trace(request);
		System.out.println("Count:"+response.getMatchCount());
		System.out.println("Weight:"+response.getMaxWeight());
		System.out.println("Status:"+response.getStatus());
		System.out.println("Start:"+response.getTraceStartTime());
		System.out.println("End:"+response.getTraceEndTime());

		request = new TraceRequest();
		request.setTraceType("AUTO");
		request.setNameSwap("N");
		
		request.setLocalId("100002");
		request.setLocalIdType("TST");
		request.setOriginator("JUNIT");
		
		request.setGivenName("Nicholas");
		request.setOtherGivenNames("Ioan");
		request.setSurname("Jones");
		request.setDateOfBirthStart(getDate("1962-08-31"));
		request.setDateOfBirthEnd(getDate("1963-08-31"));
		request.setGender("1");
		request.setPostcode("CH1 6LB");
		request.setStreet("Oakdene, Townfield Lane");

		response = tracer.trace(request);

		System.out.println("Count:"+response.getMatchCount());
		System.out.println("Weight:"+response.getMaxWeight());
		System.out.println("Status:"+response.getStatus());
		System.out.println("Start:"+response.getTraceStartTime());
		System.out.println("End:"+response.getTraceEndTime());
	}
	
}
