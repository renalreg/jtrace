package com.agiloak.mpi.trace;

import java.lang.reflect.Type;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.junit.Test;

import com.agiloak.mpi.index.Person;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class TraceManagerTest {

	public static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
	
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
	public void testTrace() {
		TraceResponse response;
		TraceRequest request = new TraceRequest();
		
		request.setTraceType("AUTO");
		request.setNameSwap("N");
		
		request.setLocalId("1000002");
		request.setLocalIdType("TST");
		request.setLocalIdOriginator("JUNIT");
		
		request.setGivenName("Brittany");
		request.setSurname("Newman");
		request.setDateOfBirthStart(getDate("1968-03-24"));
		request.setDateOfBirthEnd(getDate("1968-03-24"));
		request.setGender("e");
		request.setPostcode("37203");
		request.setStreet("One Park Plaza");

		TraceManager tracer = new TraceManager();
		
		response = tracer.trace(request);
		System.out.println("Count:"+response.getMatchCount());
		System.out.println("Weight:"+response.getMaxWeight());
		System.out.println("Status:"+response.getStatus());
		System.out.println("Start:"+response.getTraceStartTime());
		System.out.println("End:"+response.getTraceEndTime());

		request = new TraceRequest();
		request.setTraceType("AUTO");
		request.setNameSwap("Y");
		
		request.setLocalId("1000002");
		request.setLocalIdType("TST");
		request.setLocalIdOriginator("JUNIT");
		
		request.setGivenName("Nick");
		request.setSurname("JONES");
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
	
	@Test
	public void testJsonTrace() {
		TraceRequest request = new TraceRequest();
		
		// remove the trace id so it is set when the json handler creates the new request
		request.setTraceId(null);
		request.setTraceType("AUTO");
		request.setNameSwap("N");
		
		request.setLocalId("1000002");
		request.setLocalIdType("TST");
		request.setLocalIdOriginator("JUNIT");
		
		request.setGivenName("Nick");
		request.setSurname("JONES");
		request.setDateOfBirthStart(getDate("1962-08-31"));
		request.setDateOfBirthEnd(getDate("1963-08-31"));
		request.setGender("1");
		request.setPostcode("CH1 6LB");
		request.setStreet("Oakdene, Townfield Lane");

		Gson gson = new Gson();
        Type type = new TypeToken<TraceRequest>() {}.getType();
        String jsonRequest = gson.toJson(request, type);
        System.out.println("REQUEST:"+jsonRequest);
		
		TraceManager tracer = new TraceManager();
		
		String jsonResponse = tracer.trace(jsonRequest);
		
		System.out.println("RESPONSE:"+jsonResponse);

	}
	
	@Test
	public void testTraceNameSwitch() {
		TraceRequest request = new TraceRequest();
		request.setTraceType("AUTO");
		request.setNameSwap("N");
		
		request.setLocalId("1000002");
		request.setLocalIdType("TST");
		request.setLocalIdOriginator("JUNIT");
		
		request.setGivenName("JONES");
		request.setSurname("Nick");
		request.setDateOfBirthStart(getDate("1960-08-31"));
		request.setDateOfBirthEnd(getDate("1963-08-31"));
		request.setGender("1");
		request.setPostcode("CH1 6LB");
		request.setStreet("Oakdene, Townfield Lane");
		
		TraceManager tracer = new TraceManager();
		
		TraceResponse response = tracer.trace(request);
		System.out.println("Count:"+response.getMatchCount());
		System.out.println("Weight:"+response.getMaxWeight());
		System.out.println("Status:"+response.getStatus());
		System.out.println("Start:"+response.getTraceStartTime());
		System.out.println("End:"+response.getTraceEndTime());
		
		request = new TraceRequest();
		request.setTraceType("AUTO");
		request.setNameSwap("Y");
		
		request.setLocalId("1000002");
		request.setLocalIdType("TST");
		request.setLocalIdOriginator("JUNIT");
		
		request.setGivenName("JONES");
		request.setSurname("Nick");
		request.setDateOfBirthStart(getDate("1960-08-31"));
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
	@Test
	public void testTraceDOB() {
		TraceRequest request = new TraceRequest();
		request.setTraceType("AUTO");
		request.setNameSwap("N");
		
		request.setLocalId("1000002");
		request.setLocalIdType("TST");
		request.setLocalIdOriginator("JUNIT");

		request.setDateOfBirthStart(getDate("1962-08-31"));
		
		TraceManager tracer = new TraceManager();
		
		TraceResponse response = tracer.trace(request);
		System.out.println("Count:"+response.getMatchCount());
		System.out.println("Weight:"+response.getMaxWeight());
		System.out.println("Status:"+response.getStatus());
		System.out.println("Start:"+response.getTraceStartTime());
		System.out.println("End:"+response.getTraceEndTime());
	}

	@Test
	public void testGetResponse() {
		TraceManager tracer = new TraceManager();
		
		TraceResponse response = tracer.getTraceResponse("f086aeb1-e6b7-498b-b78a-5675c1b55da2");
		System.out.println("Count:"+response.getMatchCount());
		System.out.println("Weight:"+response.getMaxWeight());
		System.out.println("Status:"+response.getStatus());
		System.out.println("Start:"+response.getTraceStartTime());
		System.out.println("End:"+response.getTraceEndTime());
        Gson gson = new Gson();
        Type type = new TypeToken<TraceResponse>() {}.getType();
        String json = gson.toJson(response, type);
        System.out.println(json);
		
	}
	
	//@Test
	public void testEmptyCompare() {
		TraceRequest request = new TraceRequest();
		
		TraceResponseLine candidate = new TraceResponseLine();
		
		TraceManager tracer = new TraceManager();
		double weight = tracer.compare(request, candidate);
		System.out.println("Weight:"+weight);
		assert(weight==0.00);

	}

	//@Test
	public void testNameMatch() {
		TraceRequest request = new TraceRequest();
		request.setGivenName("Nick");
		request.setSurname("JONES");
		
		TraceResponseLine candidate = new TraceResponseLine();
		candidate.setGivenName("Nick");
		candidate.setSurname("Jones");
		
		TraceManager tracer = new TraceManager();
		double weight = tracer.compare(request, candidate);
		System.out.println("Weight:"+weight);
		//TODO: Change this as it is too dependent on configuration values
		double expect = 41.17647058823529;
		assert(weight==expect);

	}
	//@Test
	public void testFullMatch() {
		TraceRequest request = new TraceRequest();
		request.setGivenName("Nick");
		request.setSurname("JONES");
		request.setDateOfBirthStart(getDate("1962-08-31"));
		request.setGender("1");
		request.setPostcode("CH1 6LB");
		request.setStreet("Oakdene, Townfield Lane");
		
		TraceResponseLine candidate = new TraceResponseLine();
		candidate.setGivenName("Nick");
		candidate.setSurname("Jones");
		candidate.setDateOfBirth(getDate("1962-08-31"));
		candidate.setGender("1");
		candidate.setPostcode("CH1 6LB");
		candidate.setStreet("Oakdene, Townfield Lane");
		
		TraceManager tracer = new TraceManager();
		double weight = tracer.compare(request, candidate);
		System.out.println("Weight:"+weight);
		double expect = 100.00;
		assert(weight==expect);

	}
	//@Test
	public void testFullMatchPersonConstructor() {
		Person person = new Person();
		person.setGivenName("Nick");
		person.setSurname("JONES");
		person.setDateOfBirth(getDate("1962-08-31"));
		person.setGender("1");
		person.setPostcode("CH1 6LB");
		person.setStreet("Oakdene, Townfield Lane");
		TraceRequest request = new TraceRequest(person);
		
		TraceResponseLine candidate = new TraceResponseLine();
		candidate.setGivenName("Nick");
		candidate.setSurname("Jones");
		candidate.setDateOfBirth(getDate("1962-08-31"));
		candidate.setGender("1");
		candidate.setPostcode("CH1 6LB");
		candidate.setStreet("Oakdene, Townfield Lane");
		
		TraceManager tracer = new TraceManager();
		double weight = tracer.compare(request, candidate);
		System.out.println("Weight:"+weight);
		double expect = 100.00;
		assert(weight==expect);

	}
}
