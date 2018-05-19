package com.agiloak.mpi.index;

import com.agiloak.mpi.MpiException;
import com.agiloak.mpi.SimpleConnectionManager;

public class ThreadRunner {
    public static void main(String args[]) {
        
		try {
			SimpleConnectionManager.configure("postgres", "postgres","localhost", "5432", "JTRACE",10);
		} catch (MpiException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
        for (int i=0; i < 100; i++) {
            RunnableTest runnableTest = new RunnableTest(i);
            Thread t = new Thread(runnableTest);
            t.start();
        }
        System.out.println("FINISHED");
    }    
}