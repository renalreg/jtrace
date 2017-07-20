package com.agiloak.mpi.normalization;

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.codec.language.Soundex;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.search.spell.JaroWinklerDistance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NormalizationManager {

	private final static Logger logger = LoggerFactory.getLogger(NormalizationManager.class);

	protected static final String givenNamesPropertiesLocation = "/normalization/givennames.properties";
	protected static final String surnamesPropertiesLocation = "/normalization/surnames.properties";
	private static Properties givenNames = new Properties();
	private static Properties surnames = new Properties();
	
	static {
		try {
			givenNames.load(NormalizationManager.class.getResourceAsStream(givenNamesPropertiesLocation));
			surnames.load(NormalizationManager.class.getResourceAsStream(surnamesPropertiesLocation));
		} catch (IOException e) {
			logger.error("Failed to set System Properties",e);
			throw new RuntimeException(e);
		}
	}

	public static String getStandardSurname(String value){
		if (value==null || value.equals("")) return "";

		String std = value.toUpperCase();
		String norm = surnames.getProperty(std,std);
		norm = norm.trim();
		Soundex a = new Soundex();
		String soundex = a.encode(norm);
		return soundex;
	}
	
	public static String getStandardGivenName(String value){
		if (value==null || value.equals("")) return "";
		
		String std = value.toUpperCase();
		String norm = givenNames.getProperty(std,std);
		norm = norm.trim();
		Soundex a = new Soundex();
		String soundex = a.encode(norm);
		return soundex;
	}

	public static String getStandardPostcode(String value){
		if (value==null || value.equals("")) return "";

		String std = value.toUpperCase();
		String norm = std.replaceAll(" ", "");
		return norm;
	}

	/*
	 * Apache Lucene implementation of Jaro Winkler - similar to Apache Commons but to a higher precision 
	 */
	public static double getSimilarity(String s1, String s2, boolean caseSensitive){
		// defensive - no match if either string is null
		if (s1==null || s2==null) {
			return 0.00;
		}
		
		if (!caseSensitive) {
			s1 = s1.toUpperCase();
			s2 = s2.toUpperCase();
		}

		JaroWinklerDistance jwd = new org.apache.lucene.search.spell.JaroWinklerDistance();
		double a = jwd.getDistance(StringUtils.stripEnd(s1,null), StringUtils.stripEnd(s2,null));
		return a;
	}

	/*
	 * Apache Commons implementation of the Jaro Winkler distance algorithm - returns double to 2Dec Places rather than float
	 */
	public static double getSimilarity2(String s1, String s2, boolean caseSensitive){
		// defensive - no match if either string is null
		if (s1==null || s2==null) {
			return 0.00;
		}
		
		if (!caseSensitive) {
			s1 = s1.toUpperCase();
			s2 = s2.toUpperCase();
		}
		
		double a = StringUtils.getJaroWinklerDistance(s1, s2);

		return a;
	}
	

}
