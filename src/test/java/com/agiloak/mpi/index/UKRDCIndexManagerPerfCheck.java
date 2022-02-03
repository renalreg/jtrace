package com.agiloak.mpi.index;

import java.sql.Connection;
import java.util.Random;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.SimpleConnectionManager;
import com.agiloak.mpi.index.persistence.PidXREFDAO;

public class UKRDCIndexManagerPerfCheck extends UKRDCIndexManagerBaseTest {
	
	private Random rand = new Random();

	private String[] extractTypes = {"UKRDC", "MIRTH", "RADAR", "PV", "ANO"};

	public static void main(String[] args)  throws MpiException {
		SimpleConnectionManager.configure("postgres", "postgres","localhost", "5432", "JTRACE");
		Connection conn = SimpleConnectionManager.getDBConnection();

		long startTime = System.currentTimeMillis();
		int readCount = 1000;
		System.out.println("EMPI PerfCheck starting:"+startTime);
		
		UKRDCIndexManagerPerfCheck loader = new UKRDCIndexManagerPerfCheck();
		for (int i= 0; i < readCount; i++) {
			int exTypeIdx = loader.rand.nextInt(5);
			int facilityIdx = loader.rand.nextInt(89)+10;

			loader.readPidXREF(conn, "T0"+i+1, "RXX"+facilityIdx, loader.extractTypes[exTypeIdx]);
		}
		long endTime = System.currentTimeMillis();
		System.out.println("EMPI PerfCheck PIDXREF-1 ending. Processed "+readCount+" records in: "+(endTime-startTime)+"ms");
		System.out.println((endTime-startTime)/readCount+" ms");
		
		startTime = System.currentTimeMillis();

		for (int i= 0; i < readCount; i++) {
			int exTypeIdx = loader.rand.nextInt(5);
			int facilityIdx = loader.rand.nextInt(89)+10;

			loader.readPidXREF2(conn, "T0"+i+1, "RXX"+facilityIdx, loader.extractTypes[exTypeIdx]);
		}
		endTime = System.currentTimeMillis();
		System.out.println("EMPI PerfCheck PIDXREF-2 ending. Processed "+readCount+" records in: "+(endTime-startTime)+"ms");
		System.out.println((endTime-startTime)/readCount+" ms");
		
	}

	public void readPidXREF(Connection connection, String localId, String sendingFacility, String sendingExtract) throws MpiException {
		PidXREFDAO.findByLocalId(connection, sendingFacility, sendingExtract, localId);
	}
	public void readPidXREF2(Connection connection, String localId, String sendingFacility, String sendingExtract) throws MpiException {
		PidXREFDAO.FindByNationalIdAndFacility(connection, sendingFacility, sendingExtract, "NHS", "1111122222");
	}
		
}
