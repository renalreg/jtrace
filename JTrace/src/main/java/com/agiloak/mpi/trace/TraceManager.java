package com.agiloak.mpi.trace;

import java.io.IOException;
import java.lang.reflect.Type;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.normalization.NormalizationManager;
import com.agiloak.mpi.trace.persistence.TraceDAO;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

public class TraceManager {
	
	private final static Logger logger = LoggerFactory.getLogger(TraceManager.class);

	protected static final String configPropertiesLocation = "/trace/config.properties";
	private static Properties config = new Properties();
	
	
	static {
		try {
			config.load(TraceManager.class.getResourceAsStream(configPropertiesLocation));
		} catch (IOException e) {
			logger.error("Failed to set System Properties",e);
			throw new RuntimeException(e);
		}
	}
	
	public String trace(String jsonRequest){

		logger.debug("JSON Request is:"+jsonRequest);

		String jsonResponse = "";
		Gson gson = new Gson();
		
		try {
	    
			Type reqType = new TypeToken<TraceRequest>() {}.getType();
			TraceRequest request = gson.fromJson(jsonRequest, reqType);
			
			TraceResponse response = trace(request);
	
			Type respType = new TypeToken<TraceResponse>() {}.getType();
	        jsonResponse = gson.toJson(response, respType);
	        
	        logger.debug(jsonResponse);
	        
		} catch (Exception e) {
			
			logger.error("failed processing json request.",e);
		}
		
		return jsonResponse;
	}

	public TraceResponse trace(TraceRequest request) throws MpiException {
		
		TraceResponse response = null ;
		
		response = new TraceResponse();
		response.setTraceId(request.getTraceId());
		String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss.SSS").format(new Date());
		response.setTraceStartTime(timeStamp);
		
		response = doTrace(request, response);
		
		timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss.SSS").format(new Date());
		response.setTraceEndTime(timeStamp);
		
		TraceDAO.saveResponse(response);
			
		return response;
	}

	public TraceResponse getTraceResponse(String traceId) throws MpiException {
		return TraceDAO.getResponse(traceId);
	}
	
	private TraceResponse doTrace(TraceRequest request, TraceResponse response) throws MpiException {
		
		// 1) Validate and store request
		if (request==null){
			logger.error("ERROR: No request provided");
			response.setStatus("FAILED");
			return response;
		}
		
		TraceDAO.saveRequest(request);
		
		// 2) Get candidate pool
		List<TraceResponseLine> candidates;
		if (request.getSurname()==null || request.getSurname().equals("")){
			candidates = TraceDAO.findCandidatesByDOB(request, config);
		} else {
			candidates = TraceDAO.findCandidates(request, config);
		}
		
		// 3) Check pool size
		if (candidates == null){
			logger.error("ERROR: Candidate search failed");
			response.setStatus("FAILED");
			return response;
		}

		if (candidates.size()==0){
			logger.debug("INFO: No matches found");
			response.setStatus("NOMATCH");
			return response;
		}
		
		if (candidates.size() > getDouble("MaxNumberOfCandidates")){
			logger.debug("INFO: Too many matches found");
			response.setStatus("TOOMANY");
			return response;
		}
		
		// 4) Run algorithms
		Double weight = 0.00;
		Double maxWeight = 0.00;
		response.setStatus("POSSIBLE");
		TraceResponseLine candidate;
		Iterator<TraceResponseLine> i = candidates.iterator();
		while (i.hasNext()){
			candidate = i.next();
			weight = compare(request, candidate);
			if (weight > maxWeight){
				maxWeight = weight;
			}
			candidate.setWeight(weight);
			response.addResponseLines(candidate);
		}
		
		if (Double.compare(maxWeight,  100.00)==0){
			response.setStatus("EXACT");
		}
		
		// 5) Store results
		response.setMaxWeight(maxWeight);
		response.setMatchCount(candidates.size());
		
		return response;
	}

	public Double compare(TraceRequest request, TraceResponseLine candidate){
		
		//TODO Support Name Swap
		boolean caseSensitive = false;
		
		double givenNameWeight = getDouble("GivenNameWeight");
		double otherGivenNameWeight = getDouble("OtherGivenNameWeight");
		double surnameWeight = getDouble("SurnameWeight");
		double postcodeWeight = getDouble("PostcodeWeight");
		double dateOfBirthWeight = getDouble("DateOfBirthWeight");
		double genderWeight = getDouble("GenderWeight");
		double streetWeight = getDouble("StreetWeight");

		double givenNameSimilarity = NormalizationManager.getSimilarity(candidate.getGivenName(), request.getGivenName(), caseSensitive);
		if (getBoolean("UseOtherGivenName")){
			double otherNameSimilarity = NormalizationManager.getSimilarity(candidate.getOtherGivenNames(), request.getGivenName(), caseSensitive);
			if (otherNameSimilarity > givenNameSimilarity) {
				givenNameSimilarity = otherNameSimilarity;
				givenNameWeight = otherGivenNameWeight;
			}
		}
		double surnameSimilarity = 0.00;
		if (candidate.getPrevSurname()== null || candidate.getPrevSurname().equals("")){ 
			surnameSimilarity = NormalizationManager.getSimilarity(candidate.getSurname(), request.getSurname(), caseSensitive);
		} else {
			surnameSimilarity = NormalizationManager.getSimilarity(candidate.getPrevSurname(), request.getSurname(), caseSensitive);
		}
		
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
		String candidateDob = formatter.format(candidate.getDateOfBirth());
		String requestDob = formatter.format(request.getDateOfBirthStart());
		double postcodeSimilarity = NormalizationManager.getSimilarity(candidate.getPostcode(), request.getPostcode(), caseSensitive);
		double dateOfBirthSimilarity = NormalizationManager.getSimilarity(candidateDob, requestDob, caseSensitive);
		double genderSimilarity = NormalizationManager.getSimilarity(candidate.getGender(), request.getGender(), caseSensitive);
		double streetSimilarity = NormalizationManager.getSimilarity(candidate.getStreet(), request.getStreet(), caseSensitive);
		
		double sumOfWeights = givenNameWeight + 
								 surnameWeight +
								 postcodeWeight +
								 dateOfBirthWeight +
								 genderWeight + 
								 streetWeight;
		
		double overallSimilarity = (givenNameSimilarity   * givenNameWeight)+
								   (surnameSimilarity     * surnameWeight)+
								   (postcodeSimilarity    * postcodeWeight)+
								   (dateOfBirthSimilarity * dateOfBirthWeight)+
								   (genderSimilarity      * genderWeight)+
								   (streetSimilarity      * streetWeight);
		
		double percentMatch = (overallSimilarity / sumOfWeights) * 100.00 ;
		
		return percentMatch;
		
	}

	public double getDouble(String name){
		double value = 0.00;
		try { 
			String valueString = config.getProperty(name); 
			value = Double.parseDouble(valueString);
		} catch (Exception e) {
			logger.error("Error reading Double config for:"+name,e);
		}
		return value;
	}
	public boolean getBoolean(String name){
		boolean value = false;
		try { 
			String valueString = config.getProperty(name); 
			value = Boolean.parseBoolean(valueString);
		} catch (Exception e) {
			logger.error("Error reading Boolean config for:"+name,e);
		}
		return value;
	}

	
}
