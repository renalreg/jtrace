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
	public void setup()  {
		// delete test data if it exists
		try {
			LinkRecord lr = new LinkRecord(1,1);
			LinkRecordDAO.delete(lr);
		} catch (MpiException e) {
			// test data may not be there at this point 
		}
		
	}
	
	@Test
	public void testCreate() throws MpiException {
		LinkRecord lr = new LinkRecord(1,1);
		LinkRecordDAO.create(lr);
		assert(true);
	}
	
	@Test
	public void testFind() throws MpiException {
		LinkRecord lr = new LinkRecord(1,1);
		LinkRecordDAO.create(lr);
		assert(true);
		LinkRecord lr2 =LinkRecordDAO.find(1,1);
		assert(lr.getId()==lr2.getId());
	}
	
	@Test
	public void testNoFind() throws MpiException {
		LinkRecord lr = new LinkRecord(1,1);
		LinkRecordDAO.create(lr);
		assert(true);
		LinkRecord lr2 =LinkRecordDAO.find(2,1);
		assert(lr2==null);
	}

	@Test
	public void testDelete() throws MpiException {
		LinkRecord lr = new LinkRecord(1,1);
		LinkRecordDAO.create(lr);
		assert(true);
		LinkRecordDAO.delete(lr);
		assert(true);
	}

}
