package com.agiloak.mpi.index.persistence;

import org.junit.Test;

import com.agiloak.mpi.index.Person;
import com.agiloak.mpi.index.persistence.PersonDAO;

public class PersonReadTest {
	
	@Test
	public void testRead() {
		Person person = PersonDAO.findPerson("MR", "100001", "MY");
		assert(person != null);
	}

	@Test
	public void testReadNTF() {
		Person person = PersonDAO.findPerson("MR", "9100001", "MY");
		assert(person == null);
	}
}
