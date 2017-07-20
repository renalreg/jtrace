package com.agiloak.mpi.trace;

import java.lang.reflect.Type;
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;



public class JsonTest {

        public static void main(String[] args) {
        	String req = "{\"traceType\":\"REST\",\"nameSwap\":\"N\",\"localIdType\":\"TST\","+
                         "\"localIdOriginator\":\"FHIR\",\"localId\":\"hca-pat-43\","+
        			     "\"givenName\":\"Beryl\",\"surname\":\"Bachmann\",\"gender\":\"e\","+
                         "\"postcode\":\"37203\",\"dateOfBirthStart\":\"1974-02-13\",\"street\":\"One Park Plaza\"}";

            Gson gson = new Gson();
            Type type = new TypeToken<TraceRequest>() {}.getType();
            TraceRequest treq = gson.fromJson(req, type);
            System.out.println(treq.getDateOfBirthStart());
            

            /**
        	    List<TraceRequest> list = new ArrayList<TraceRequest>();
                for (int i = 0; i < 20; i++) {
                	TraceRequest tr = new TraceRequest();
                	tr.setGender("1");
                	tr.setSurname("JONES");
                    list.add(tr);
                }
                Gson gson = new Gson();
                Type type = new TypeToken<List<TraceRequest>>() {}.getType();
                String json = gson.toJson(list, type);
                System.out.println(json);
                List<TraceRequest> fromJson = gson.fromJson(json, type);

                for (TraceRequest req : fromJson) {
                        System.out.println(req.getTraceId()+":"+req.getSurname());
                }
               **/ 
                
        }
}