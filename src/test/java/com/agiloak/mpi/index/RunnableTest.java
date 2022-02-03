package com.agiloak.mpi.index;

import java.util.Date;

public class RunnableTest implements Runnable {

	private Date d1 = new Date();
	private int idSeq;
	
    public RunnableTest(int seq) {
    	idSeq = seq;
    }

    public void run() {
    	try {
			// T1-1 NationalId for P1. New Person, New Master and new link to the master
			Person p1 = new Person().setDateOfBirth(d1).setSurname(" JONES").setGivenName("NICHOLAS ").setGender("1");
			p1.setPostcode("CH1 6LB").setStreet("Townfield Lane");
			p1.setLocalId("NSYS10000"+idSeq).setLocalIdType("MR").setOriginator("THRDTST");
			
			UKRDCIndexManager im = new UKRDCIndexManager();
			UKRDCIndexManagerResponse resp = im.store(p1);
			NationalIdentity natId = resp.getNationalIdentity();
			System.out.println("National Id:"+natId.getId());
    	} catch(Exception e) {
			e.printStackTrace();
    		
    	}
  }

}


