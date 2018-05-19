package com.agiloak.mpi.trace.persistence;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.SimpleConnectionManager;
import com.agiloak.mpi.normalization.NormalizationManager;
import com.agiloak.mpi.trace.TraceRequest;
import com.agiloak.mpi.trace.TraceResponse;
import com.agiloak.mpi.trace.TraceResponseLine;

public class TraceDAO {

	private final static Logger logger = LoggerFactory.getLogger(TraceDAO.class);

	public static List<TraceResponseLine> findCandidatesByDOB(TraceRequest request, Properties config) throws MpiException {
		
		logger.info("start of findCandidatesByDOB");
		
		List<TraceResponseLine> candidates = new ArrayList<TraceResponseLine>();
		
		String traceDOBSQL = "select * from jtrace.person where dateofbirth = ? ";
		
		PreparedStatement preparedStatement = null;
		Connection conn = null;
		ResultSet rs = null;
		
		try {

			conn = SimpleConnectionManager.getDBConnection();
			
			preparedStatement = conn.prepareStatement(traceDOBSQL);
			preparedStatement.setDate(1,getSqlDate(request.getDateOfBirthStart()));

			rs = preparedStatement.executeQuery();
			
			while (rs.next()){
				TraceResponseLine candidate;
				candidate = new TraceResponseLine();
				candidate.setPersonId(rs.getInt("id"));
				java.sql.Date dob = rs.getDate("dateofbirth");
				candidate.setDateOfBirth(new java.util.Date(dob.getTime()));
				candidate.setGender(rs.getString("gender"));
				candidate.setGivenName(rs.getString("givenname"));
				candidate.setOtherGivenNames(rs.getString("othergivennames"));
				candidate.setSurname(rs.getString("surname"));
				candidate.setPostcode(rs.getString("postcode"));
				candidate.setStreet(rs.getString("street"));
				if (request.getLocalId().equals(rs.getString("localid"))) {
					// don't match on the same candidate
					// TODO : extend to match on type and orginator
				} else {
					candidates.add(candidate);
				}
			}
			
		} catch (SQLException e) {
			logger.error("Failure tracing Person:"+e.getErrorCode()+":"+e.getMessage());
			logger.error("SQLException finding by DOB:",e);

		} finally {

			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					logger.error("Failure closing resultset.",e);
				}
			}
			
			if (preparedStatement != null) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					logger.error("Failure closing prepared statement.",e);
				}
			}
			if(conn!= null) {
				try {
					conn.close();
				} catch (SQLException e) {
					logger.error("Failure closing Connection",e);
					throw new MpiException("Trace read failed. "+e.getMessage());
				}
			}

		}
		
		return candidates;		
	}

	public static List<TraceResponseLine> findCandidates(TraceRequest request, Properties config) throws MpiException {

		logger.debug("Starting");

		// Normalize request
		String stdPostcode  = NormalizationManager.getStandardPostcode(request.getPostcode());
		// Support Name Swap
		String stdSurname, stdGivenName = "";
		if (getSafeString(request.getNameSwap()).equals("Y")) {
			stdSurname   = NormalizationManager.getStandardSurname(request.getGivenName());
			stdGivenName = NormalizationManager.getStandardGivenName(request.getSurname());
		} else {
			stdSurname   = NormalizationManager.getStandardSurname(request.getSurname());
			stdGivenName = NormalizationManager.getStandardGivenName(request.getGivenName());
		}
		
		// initialise parameter list
		List<Object> parms = new ArrayList<Object>();
		
		List<TraceResponseLine> candidates = new ArrayList<TraceResponseLine>();
				
		String baseSQL = "select * from jtrace.person where dateofdeath is null ";
		
		// build SQL blocks
		String fullSQL = baseSQL;
		int blockCount = 0;
		
		// BLOCK 1 GN / SN / DOB
		if ( (getBoolean(config, "TraceByGivenNameSurnameDOB")) &&
			 (!stdGivenName.equals("")) && 
			 (!stdSurname.equals("")) && 
			 (!(request.getDateOfBirthStart()==null)) ){
			blockCount ++;
			fullSQL += getStringQuery(parms, "stdgivenname", stdGivenName);
			fullSQL += getSurnameQuery(parms, stdSurname, config);
			fullSQL += getDobQuery(parms, request);
		}

		// BLOCK 2 SN / Gender / DOB / PC
		if ( (getBoolean(config, "TraceBySurnameGenderDOBPcode")) &&
			 (!stdSurname.equals("")) && 
			 (!getSafeString(request.getGender()).equals("")) && 
			 (!(request.getDateOfBirthStart()==null)) &&
			 (!stdPostcode.equals("")) ){
			if (blockCount > 0) {
				fullSQL += " UNION "+baseSQL;
			}
			blockCount ++;
			fullSQL += getStringQuery(parms, "stdsurname", stdSurname);
			fullSQL += getStringQuery(parms, "gender", getSafeString(request.getGender()));
			fullSQL += getStringQuery(parms, "stdpostcode", stdPostcode);
			fullSQL += getDobQuery(parms, request);
		}

		// BLOCK 3 GN / Gender / DOB / PC
		if ( (getBoolean(config, "TraceByGivenNameGenderDOBPcode")) &&
			 (!stdGivenName.equals("")) && 
			 (!getSafeString(request.getGender()).equals("")) && 
			 (!(request.getDateOfBirthStart()==null)) &&
			 (!stdPostcode.equals("")) ){
			if (blockCount > 0) {
				fullSQL += " UNION "+baseSQL;
			}
			blockCount ++;
			fullSQL += getStringQuery(parms, "stdgivenname", stdGivenName);
			fullSQL += getStringQuery(parms, "gender", getSafeString(request.getGender()));
			fullSQL += getStringQuery(parms, "stdpostcode", stdPostcode);
			fullSQL += getDobQuery(parms, request);
		}

		// BLOCK 4 SN / PC
		if ( (getBoolean(config, "TraceBySurnamePcode")) &&
			 (!stdSurname.equals("")) && 
			 (!stdPostcode.equals("")) ){
			if (blockCount > 0) {
				fullSQL += " UNION "+baseSQL;
			}
			blockCount ++;
			fullSQL += getSurnameQuery(parms, stdSurname, config);
			fullSQL += getStringQuery(parms, "stdpostcode", stdPostcode);
		}

		// BLOCK 5 SN / DOB
		if ( (getBoolean(config, "TraceBySurnameDOB")) &&
			 (!stdSurname.equals("")) && 
			 (!(request.getDateOfBirthStart()==null)) ){
			if (blockCount > 0) {
				fullSQL += " UNION "+baseSQL;
			}
			blockCount ++;
			fullSQL += getSurnameQuery(parms, stdSurname, config);
			fullSQL += getDobQuery(parms, request);
		}

		// BLOCK 6 GN DOB
		if ( (getBoolean(config, "TraceByGivenNameDOB")) &&
			 (!stdGivenName.equals("")) && 
			 (!(request.getDateOfBirthStart()==null)) ){
			if (blockCount > 0) {
				fullSQL += " UNION "+baseSQL;
			}
			blockCount ++;
			fullSQL += getStringQuery(parms, "stdgivenname", stdGivenName);
			fullSQL += getDobQuery(parms, request);
		}

		// BLOCK 7 GN SN
		if ( (getBoolean(config, "TraceByGivenNameSurname")) &&
				(!stdGivenName.equals("")) && 
			 (!stdSurname.equals("")) ){
			if (blockCount > 0) {
				fullSQL += " UNION "+baseSQL;
			}
			blockCount ++;
			fullSQL += getStringQuery(parms, "stdgivenname", stdGivenName);
			fullSQL += getSurnameQuery(parms, stdSurname, config);
		}

		logger.debug("BlockCount:"+blockCount);
		logger.debug("FullSQL:"+fullSQL);
		
		PreparedStatement queryStmt = null;
		Connection conn = null;
		ResultSet rs = null;
		
		try {

			conn = SimpleConnectionManager.getDBConnection();
			
			queryStmt = conn.prepareStatement(fullSQL);
			
			Iterator<Object> pIt = parms.iterator();
			int pNo = 0;
			while (pIt.hasNext()){
				Object pObj = pIt.next();
				pNo ++;
				
				if (pObj instanceof String){
					queryStmt.setString(pNo, (String) pObj);
				}
				if (pObj instanceof java.sql.Date){
					queryStmt.setDate(pNo, (java.sql.Date) pObj);
				}
				
			}
			
			rs = queryStmt.executeQuery();
			
			while (rs.next()){
				TraceResponseLine candidate;
				candidate = new TraceResponseLine();
				candidate.setPersonId(rs.getInt("id"));
				java.sql.Date dob = rs.getDate("dateofbirth");
				candidate.setDateOfBirth(new java.util.Date(dob.getTime()));
				candidate.setGender(rs.getString("gender"));
				candidate.setGivenName(rs.getString("givenname"));
				candidate.setOtherGivenNames(rs.getString("othergivennames"));
				candidate.setSurname(rs.getString("surname"));
				candidate.setPostcode(rs.getString("postcode"));
				candidate.setStreet(rs.getString("street"));
				if (request.getLocalId().equals(rs.getString("localid").trim())) {
					logger.info("Same record found - skipping");
					// don't match on the same candidate
					// TODO : extend to match on type and orginator
				} else {
					logger.info("RLID:"+request.getLocalId()+", CLID:"+rs.getString("localid"));
					candidates.add(candidate);
				}
			}
			
		} catch (SQLException e) {
			logger.error("Failure tracing Person:"+e.getErrorCode()+":"+e.getMessage());

		} finally {

			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					logger.error("Failure closing resultset.",e);
				}
			}
			
			if (queryStmt != null) {
				try {
					queryStmt.close();
				} catch (SQLException e) {
					logger.error("Failure closing prepared statement.",e);
				}
			}
			if(conn!= null) {
				try {
					conn.close();
				} catch (SQLException e) {
					logger.error("Failure closing Connection",e);
					throw new MpiException("Trace read failed. "+e.getMessage());
				}
			}
		}
		
		return candidates;		
	}

	private static String getStringQuery(List<Object> parms, String fName, String fValue) {
		String stringQuery = " and " + fName + " = ? ";
		parms.add(fValue);
		return stringQuery;
	}

	private static String getSurnameQuery(List<Object> parms, String stdSurname, Properties config) {
		String stringQuery = "";
		if (getBoolean(config, "TracePreviousSurname")){
			stringQuery = " and ( stdsurname = ? or stdprevsurname = ? ) ";
			parms.add(stdSurname);
			parms.add(stdSurname);
		} else {
			stringQuery = " and stdsurname = ? ";
			parms.add(stdSurname);
		}
		return stringQuery;
	}

	private static String getDobQuery(List<Object> parms, TraceRequest request) {
		// build the dob sql
		String dobQuery = "";

		if (request.getDateOfBirthEnd()==null){
			dobQuery = " and dateofbirth = ? ";
			parms.add(getSqlDate(request.getDateOfBirthStart()));
		} else {
			dobQuery = " and dateofbirth between ? and ? ";
			parms.add(getSqlDate(request.getDateOfBirthStart()));
			parms.add(getSqlDate(request.getDateOfBirthEnd()));
		}
		return dobQuery;
	}
	
	private static String getSafeString(String value){
		if (value==null || value.equals("")){
			return "";
		} else {
			return value;
		}
	}
	
	private static java.sql.Date getSqlDate(java.util.Date date){
		if (date==null) return null;
		
		java.sql.Date sqlDate = new java.sql.Date(date.getTime());

		return sqlDate;
		
	}

	public static boolean getBoolean(Properties config, String name){
		boolean value = false;
		try { 
			String valueString = config.getProperty(name); 
			value = Boolean.parseBoolean(valueString);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return value;
	}
	
	public static void saveRequest(TraceRequest request) throws MpiException {

		logger.debug("Starting");

		// traceid is set by at database sequence
		
		String insertSQL = "Insert into jtrace.tracerequest "+
				"(traceid, tracetype, nameswap, localid, localidtype, originator, "+
	            "givenname, othergivennames, surname, gender, postcode, dateofbirthstart, dateofbirthend,"+ 
	            "street, longname, longaddress)"+
				" values (?,?,?,?,?,?,"+
	            " ?,?,?,?,?,?,?,"+
				" ?,?,?)";

		PreparedStatement preparedStatement = null;
		Connection conn = null;
		
		try {

			conn = SimpleConnectionManager.getDBConnection();
			
			preparedStatement = conn.prepareStatement(insertSQL);
			preparedStatement.setString(1, request.getTraceId());
			preparedStatement.setString(2, request.getTraceType());
			preparedStatement.setString(3, request.getNameSwap());
			preparedStatement.setString(4, request.getLocalId());
			preparedStatement.setString(5, request.getLocalIdType());
			preparedStatement.setString(6, request.getOriginator());

			preparedStatement.setString(7, request.getGivenName());
			preparedStatement.setString(8, request.getOtherGivenNames());
			preparedStatement.setString(9, request.getSurname());
			preparedStatement.setString(10, request.getGender());
			preparedStatement.setString(11, request.getPostcode());
			preparedStatement.setDate(12,getSqlDate(request.getDateOfBirthStart()));
			preparedStatement.setDate(13,getSqlDate(request.getDateOfBirthEnd()));

			preparedStatement.setString(14, request.getStreet());
			preparedStatement.setString(15, request.getLongName());
			preparedStatement.setString(16, request.getLongAddress());

			int affectedRows = preparedStatement.executeUpdate();
			logger.info("Affected Rows:"+affectedRows);
			
		} catch (Exception e) {
			logger.error("Failure inserting TraceRequest.",e);
			throw new MpiException("TraceRequest insert failed");
		} finally {

			if (preparedStatement != null) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					logger.error("Failure closing prepared statement.",e);
					throw new MpiException("TraceRequest insert failed");
				}
			}
			if(conn!= null) {
				try {
					conn.close();
				} catch (SQLException e) {
					logger.error("Failure closing Connection",e);
					throw new MpiException("Trace insert failed. "+e.getMessage());
				}
			}
		}

	}
	
	public static TraceResponse getResponse(String traceId) throws MpiException {

		logger.debug("Starting");

		TraceResponse response = new TraceResponse();
		response.setTraceId(traceId);
		
		String readSQL = "Select * from jtrace.traceresponse where traceid = ? ";

		PreparedStatement preparedStatement = null;
		Connection conn = null;
		ResultSet rs = null;
		
		try {

			conn = SimpleConnectionManager.getDBConnection();
			
			preparedStatement = conn.prepareStatement(readSQL);
			preparedStatement.setString(1, traceId);

			rs = preparedStatement.executeQuery();
			
			if (rs.next()){
				response.setTraceStartTime(rs.getString("tracestarttime"));
				response.setTraceEndTime(rs.getString("traceendtime"));
				response.setMessage(rs.getString("message"));
				response.setStatus(rs.getString("status"));
				response.setMaxWeight(rs.getDouble("maxweight"));
				response.setMatchCount(rs.getInt("matchcount"));
			}
			
			getResponseLines(response);
			
		} catch (SQLException e) {
			logger.error("Failure reading TraceResponse.",e);
			throw new MpiException("TraceResponse read failed");
		} finally {

			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					logger.error("Failure closing resultset.",e);
					throw new MpiException("TraceResponse read failed");
				}
			}
			
			if (preparedStatement != null) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					logger.error("Failure closing prepared statement.",e);
					throw new MpiException("TraceResponse read failed");
				}
			}
			if(conn!= null) {
				try {
					conn.close();
				} catch (SQLException e) {
					logger.error("Failure closing Connection",e);
					throw new MpiException("TraceResponse read failed. "+e.getMessage());
				}
			}
		}

		return response;
	}

	private static TraceResponse getResponseLines(TraceResponse response) throws MpiException {

		logger.debug("Starting");

		String readSQL = "Select * from jtrace.traceresponseline where traceid = ? ";

		PreparedStatement preparedStatement = null;
		Connection conn = null;
		ResultSet rs = null;
		
		try {

			conn = SimpleConnectionManager.getDBConnection();
			
			preparedStatement = conn.prepareStatement(readSQL);
			preparedStatement.setString(1, response.getTraceId());

			rs = preparedStatement.executeQuery();
			
			while (rs.next()){
				TraceResponseLine trl = new TraceResponseLine();
				trl.setPersonId(rs.getInt("personid"));
				trl.setWeight(rs.getDouble("weight"));
				trl.setGivenName(rs.getString("givenname"));
				trl.setOtherGivenNames(rs.getString("othergivennames"));
				trl.setSurname(rs.getString("surname"));
				trl.setPrevSurname(rs.getString("prevsurname"));
				trl.setGender(rs.getString("gender"));
				trl.setPostcode(rs.getString("postcode"));
				trl.setDateOfBirth(new java.util.Date(rs.getDate("dateofbirth").getTime()));
				trl.setStreet(rs.getString("street"));
				trl.setLongName(rs.getString("longname"));
				trl.setLongAddress(rs.getString("longaddress"));
				response.addResponseLines(trl);
			}
			
		} catch (SQLException e) {
			logger.error("Failure reading TraceResponseLine.",e);
			throw new MpiException("TraceResponseLine read failed");

		} finally {

			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					logger.error("Failure closing resultset.",e);
					throw new MpiException("TraceResponseLine read failed");
				}
			}
			
			if (preparedStatement != null) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					logger.error("Failure closing prepared statement.",e);
					throw new MpiException("TraceResponseLine read failed");
				}
			}
			if(conn!= null) {
				try {
					conn.close();
				} catch (SQLException e) {
					logger.error("Failure closing Connection",e);
					throw new MpiException("TraceResponseLine read failed. "+e.getMessage());
				}
			}
		}

		return response;
	}
	
	public static void saveResponse(TraceResponse response) throws MpiException {

		logger.debug("Starting");

		String insertSQL = "Insert into jtrace.traceresponse "+
				"(traceid, tracestarttime, traceendtime, message, status, maxweight, matchcount)"+
				" values (?,?,?,?,?,?,?)";

		PreparedStatement preparedStatement = null;
		Connection conn = null;
		
		try {

			conn = SimpleConnectionManager.getDBConnection();
			
			preparedStatement = conn.prepareStatement(insertSQL);
			preparedStatement.setString(1, response.getTraceId());
			preparedStatement.setString(2, response.getTraceStartTime());
			preparedStatement.setString(3, response.getTraceEndTime());
			preparedStatement.setString(4, response.getMessage());
			preparedStatement.setString(5, response.getStatus());
			preparedStatement.setDouble(6, response.getMaxWeight());
			preparedStatement.setInt(7, response.getMatchCount());

			int affectedRows = preparedStatement.executeUpdate();
			logger.info("Affected Rows:"+affectedRows);
			
			Iterator<TraceResponseLine> it = response.getResponseLines().iterator();
			while (it.hasNext()){
				TraceResponseLine trl = it.next();
				saveResponseLine(trl, response.getTraceId());
			}
			
		} catch (SQLException e) {
			logger.error("Failure inserting TraceResponse.",e);
			throw new MpiException("TraceResponse insert failed");
		} finally {

			if (preparedStatement != null) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					logger.error("Failure closing prepared statement.",e);
					throw new MpiException("TraceResponse insert failed");
				}
			}
			if(conn!= null) {
				try {
					conn.close();
				} catch (SQLException e) {
					logger.error("Failure closing Connection",e);
					throw new MpiException("TraceResponse insert failed. "+e.getMessage());
				}
			}
		}

	}
	public static void saveResponseLine(TraceResponseLine responseLine, String traceId) throws MpiException {

		logger.debug("Starting");

		String insertSQL = "Insert into jtrace.traceresponseline "+
				"(traceid, personid, weight, givenname, othergivennames, surname, "+
				"prevsurname, gender, postcode, dateofbirth, street, longname, "+
				"longaddress) "+
				" values (?,?,?,?,?,?,"+
				"         ?,?,?,?,?,?,"+
				"         ?)";
				
		PreparedStatement preparedStatement = null;
		Connection conn = null;
		
		try {

			conn = SimpleConnectionManager.getDBConnection();
			
			preparedStatement = conn.prepareStatement(insertSQL);
			preparedStatement.setString(1, traceId);
			preparedStatement.setInt(2, responseLine.getPersonId());
			preparedStatement.setDouble(3, responseLine.getWeight());
			preparedStatement.setString(4, responseLine.getGivenName());
			preparedStatement.setString(5, responseLine.getOtherGivenNames());
			preparedStatement.setString(6, responseLine.getSurname());
			preparedStatement.setString(7, responseLine.getPrevSurname());
			preparedStatement.setString(8, responseLine.getGender());
			preparedStatement.setString(9, responseLine.getPostcode());
			preparedStatement.setDate(10, getSqlDate(responseLine.getDateOfBirth()));
			preparedStatement.setString(11, responseLine.getStreet());
			preparedStatement.setString(12, responseLine.getLongName());
			preparedStatement.setString(13, responseLine.getLongAddress());

			int affectedRows = preparedStatement.executeUpdate();
			logger.info("Affected Rows:"+affectedRows);
			
		} catch (SQLException e) {
			logger.error("Failure inserting TraceResponseLine.",e);
			throw new MpiException("TraceResponseLine insert failed");
		} finally {

			if (preparedStatement != null) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					logger.error("Failure closing prepared statement.",e);
					throw new MpiException("TraceResponseLine insert failed");
				}
			}
			if(conn!= null) {
				try {
					conn.close();
				} catch (SQLException e) {
					logger.error("Failure closing Connection",e);
					throw new MpiException("TraceResponseLine insert failed. "+e.getMessage());
				}
			}
		}

	}
	
	public static void clearByTraceId(String traceId) throws MpiException {

		logger.debug("Starting");

		// traceid is set by at database sequence
		
		String deleteSQL1 = "delete from jtrace.tracerequest where traceid = ?";
		String deleteSQL2 = "delete from jtrace.traceresponse where traceid = ?";
		String deleteSQL3 = "delete from jtrace.traceresponseline where traceid = ?";

		PreparedStatement preparedStatement = null;
		Connection conn = null;
		
		try {

			conn = SimpleConnectionManager.getDBConnection();
			
			preparedStatement = conn.prepareStatement(deleteSQL1);
			preparedStatement.setString(1, traceId);
			int affectedRows = preparedStatement.executeUpdate();
			logger.debug("Affected TraceRequest Rows:"+affectedRows);
			preparedStatement.close();
					 
			preparedStatement = conn.prepareStatement(deleteSQL2);
			preparedStatement.setString(1, traceId);
			affectedRows = preparedStatement.executeUpdate();
			logger.debug("Affected TraceResponse Rows:"+affectedRows);
			preparedStatement.close();
					 
			preparedStatement = conn.prepareStatement(deleteSQL3);
			preparedStatement.setString(1, traceId);
			affectedRows = preparedStatement.executeUpdate();
			logger.debug("Affected TraceResponseLine Rows:"+affectedRows);
			preparedStatement.close();
					 
		} catch (SQLException e) {
			logger.error("Failure deleting trace:",e);
			throw new MpiException("trace delete failed");

		} finally {

			if (preparedStatement != null) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					logger.error("Failure closing Prepared Statement:",e);
					throw new MpiException("trace delete failed");
				}
			}
			if(conn!= null) {
				try {
					conn.close();
				} catch (SQLException e) {
					logger.error("Failure closing Connection",e);
					throw new MpiException("Trace delete failed. "+e.getMessage());
				}
			}
		}

	}	
	
	public static String getTraceId(String localId, String localIdType, String originator, String traceType) throws MpiException {

		logger.debug("Starting");

		String readSQL = "Select * from jtrace.tracerequest where tracetype = ? and localid = ? and localidtype = ? and originator = ? ";

		PreparedStatement preparedStatement = null;
		Connection conn = null;
		ResultSet rs = null;
		
		String traceId = null;
		
		try {

			conn = SimpleConnectionManager.getDBConnection();
			
			preparedStatement = conn.prepareStatement(readSQL);
			preparedStatement.setString(1, traceType);
			preparedStatement.setString(2, localId);
			preparedStatement.setString(3, localIdType);
			preparedStatement.setString(4, originator);

			rs = preparedStatement.executeQuery();
			
			
			if (rs.next()){
				traceId = rs.getString("traceid");
			}
			
		} catch (SQLException e) {
			logger.error("Failure reading TraceRequest.",e);
			throw new MpiException("TraceRequest read failed");
		} finally {

			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					logger.error("Failure closing resultset.",e);
					throw new MpiException("TraceRequest read failed");
				}
			}
			
			if (preparedStatement != null) {
				try {
					preparedStatement.close();
				} catch (SQLException e) {
					logger.error("Failure closing prepared statement.",e);
					throw new MpiException("TraceRequest read failed");
				}
			}
			if(conn!= null) {
				try {
					conn.close();
				} catch (SQLException e) {
					logger.error("Failure closing Connection",e);
					throw new MpiException("TraceRequest read failed. "+e.getMessage());
				}
			}
		}

		return traceId;
	}	
}
