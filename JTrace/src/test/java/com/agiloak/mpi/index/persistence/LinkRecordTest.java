package com.agiloak.mpi.index.persistence;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.index.LinkRecord;

public class LinkRecordTest {

	@Rule
	public final ExpectedException exception = ExpectedException.none();

	@Before
	public void setup()  throws MpiException {
		// delete test data
		//LinkRecord.DeleteByPersonalId("NHS0000001");
	}
	
	@Test
	public void testCreate() throws MpiException {
		LinkRecord lr = new LinkRecord(1,1);
		LinkRecordDAO.create(lr);
		assert(true);
	}

}
