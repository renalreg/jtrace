package com.agiloak.mpi.index;

import java.util.Date;
import java.util.Random;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.SimpleConnectionManager;

public class UKRDCIndexManagerLoad extends UKRDCIndexManagerBaseTest {
	
	private Date d1 = getDate("1962-08-31");
	private UKRDCIndexManager im = new UKRDCIndexManager();
	private Random rand = new Random();

	private String[] extractTypes = {"UKRDC", "MIRTH", "RADAR", "PV", "ANO"};

	public static void main(String[] args)  throws MpiException {
		SimpleConnectionManager.configure("postgres", "postgres","localhost", "5432", "JTRACE");
		long startTime = System.currentTimeMillis();
		int loadCount = 1000;
		int offsetCount = 100000;
		System.out.println("EMPI Loader starting:"+startTime);
		
		UKRDCIndexManagerLoad loader = new UKRDCIndexManagerLoad();
		for (int i= offsetCount; i < (offsetCount+loadCount); i++) {
			int exTypeIdx = loader.rand.nextInt(5);
			int facilityIdx = loader.rand.nextInt(89)+10;

			loader.loadPerson("T0"+i+1, "RXX"+facilityIdx, loader.extractTypes[exTypeIdx]);
		}
		long endTime = System.currentTimeMillis();
		System.out.println("EMPI Loader ending. Processed "+loadCount+" records in: "+(endTime-startTime)+"ms");
	}

	public void loadPerson(String localId, String sendingFacility, String sendingExtract) throws MpiException {

		Person p1 = new Person().setDateOfBirth(d1).setSurname("UPDATE").setGivenName("PAT").setGender("1");
		p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
		p1.setLocalId(localId);

		UKRDCIndexManagerResponse setPidResp = im.setLocalPID(p1, sendingFacility, sendingExtract);
		if (setPidResp.getStatus()==UKRDCIndexManagerResponse.FAIL) {
			throw new MpiException(setPidResp.getMessage());
		}
		
		p1.setLocalId(setPidResp.getPid());
		p1.setOriginator("UKRDC");
		p1.setLocalIdType("CLPID");

		UKRDCIndexManagerResponse empiResp = im.store(p1);
		if (empiResp.getStatus()==UKRDCIndexManagerResponse.FAIL) {
			throw new MpiException(empiResp.getMessage());
		}
	}
		
}
