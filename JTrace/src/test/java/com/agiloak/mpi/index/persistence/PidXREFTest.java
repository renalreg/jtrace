package com.agiloak.mpi.index.persistence;

import java.text.SimpleDateFormat;

import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.SimpleConnectionManager;

public class PidXREFTest {
	
	public static SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

	@Rule
	public final ExpectedException exception = ExpectedException.none();

	@BeforeClass
	public static void setup()  throws MpiException {
		SimpleConnectionManager.configure("postgres", "postgres","localhost", "5432", "JTRACE");
		// delete test data
		//PidXREFDAO.deleteByNationalId("NHS0000001","NHS");
		
	}
	
	@Test
	public void testGetSequence() throws MpiException {
		String patientId = PidXREFDAO.allocate();
		assert(patientId!=null);
		assert(patientId.length()==9);
	}

}
