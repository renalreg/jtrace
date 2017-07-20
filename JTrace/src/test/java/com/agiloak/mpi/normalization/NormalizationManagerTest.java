package com.agiloak.mpi.normalization;

import org.apache.commons.codec.language.Soundex;

import junit.framework.TestCase;

public class NormalizationManagerTest extends TestCase {

	public void testNormalizationEdgeCases() {
		Soundex soundex = new Soundex();
		String std = NormalizationManager.getStandardGivenName(null);
		assert(std.equals(""));

		std = NormalizationManager.getStandardGivenName("");
		assert(std.equals(""));
	}

	public void testSimilarity() {
		Double s  = 0.00;
		s = NormalizationManager.getSimilarity("NICHOLAS","NICHOLAS",false);
		System.out.println("Exact:"+s);
		
		// 1 character switch
		s = NormalizationManager.getSimilarity("NICHOLAS","NICHOLSA",false);
		System.out.println("1C Swap:"+s);

		// 2 character switch
		s = NormalizationManager.getSimilarity("NICHOLAS","NICHOSLA",false);
		System.out.println("2C Swap:"+s);

		// 1 character error
		s = NormalizationManager.getSimilarity("NICHOLAS","N1CHOLAS",false);
		System.out.println("1C Error:"+s);

		// 2 character error
		s = NormalizationManager.getSimilarity("NICHOLAS","N1CH0LAS",false);
		System.out.println("2C Error:"+s);

		// 1 character missing
		s = NormalizationManager.getSimilarity("NICHOLAS","NICHOAS",false);
		System.out.println("1C Miss:"+s);

		// 2 character missing
		s = NormalizationManager.getSimilarity("NICHOLAS","NHOLAS",false);
		System.out.println("2C Miss:"+s);

	}
	
	public void testSimilarityCase() {
		Double s  = 0.00;
		Double s2  = 0.00;
		
		// 1 character error vs 1 case error
		s = NormalizationManager.getSimilarity("NICHOLAS","N1CHOLAS",true);
		s2 = NormalizationManager.getSimilarity("NICHOLAS","NiCHOLAS",true);
		assert(Double.compare(s, s2)==0);

		// 2 character error vs 2 case errors
		s = NormalizationManager.getSimilarity("NICHOLAS","N1CH0LAS",true);
		s2 = NormalizationManager.getSimilarity("NICHOLAS","NiCHoLAS",true);
		assert(Double.compare(s, s2)==0);

	}
	
	public void testStandardisation() {
		Soundex soundex = new Soundex();
		String std = NormalizationManager.getStandardGivenName("NICK");
		assert(std.equals(soundex.encode("NICHOLAS")));

		std = NormalizationManager.getStandardGivenName("NICKY");
		assert(std.equals(soundex.encode("NICKY")));

		// Use distant string for standardisation [ZZZZZ=TESTCASE]
		std = NormalizationManager.getStandardGivenName("ZZZZZ");
		assert(std.equals(soundex.encode("TESTCASE")));

		std = NormalizationManager.getStandardSurname("JONS");
		assert(std.equals(soundex.encode("JONES")));

		std = NormalizationManager.getStandardSurname("JOHNSON");
		assert(std.equals(soundex.encode("JOHNSON")));

		// Use distant string for standardisation [ZZZZZ=TESTCASE]
		std = NormalizationManager.getStandardSurname("ZZZZZ");
		assert(std.equals(soundex.encode("TESTCASE")));

		std = NormalizationManager.getStandardPostcode(" CH1  6LB ");
		assert(std.equals("CH16LB"));
	}

}
