package main.java;

import java.util.Date;
import java.util.Random;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.VoltBulkLoader.BulkLoaderFailureCallBack;
import org.voltdb.client.VoltBulkLoader.VoltBulkLoader;
import org.voltdb.VoltTable;

public class Application {
/*
USING TTL 1 MINUTES ON COLUMN CREATED;
 USING TTL 25 MINUTES ON COLUMN CREATED;

CREATE TABLE T1 (
   IMSI varchar(15) NOT NULL,
   C1 varchar(15) ,
   C2 varchar(20),
   CREATED timestamp NOT NULL,
   PRIMARY KEY (IMSI)
);PARTITION TABLE T1 ON COLUMN IMSI;


CREATE TABLE T2 (
   IMSI varchar(15) NOT NULL,
   AOA integer NOT NULL,
   CREATED timestamp NOT NULL
);PARTITION TABLE T2 ON COLUMN IMSI;
CREATE INDEX t2Index ON T2(imsi);

CREATE TABLE T3 (
   IMSI varchar(15) NOT NULL,
   TA integer NOT NULL,
   CREATED timestamp NOT NULL
);PARTITION TABLE T3 ON COLUMN IMSI;
CREATE INDEX t3Index ON T3(imsi);

CREATE TABLE T4 (
   IMSI varchar(15) NOT NULL,
   TB integer NOT NULL,
   CREATED timestamp NOT NULL
);PARTITION TABLE T4 ON COLUMN IMSI;
CREATE INDEX t4Index ON T4(imsi);

CREATE TABLE T5 (
   IMSI varchar(15) NOT NULL,
   TC integer NOT NULL,
   CREATED timestamp NOT NULL
); PARTITION TABLE T5 ON COLUMN IMSI;
CREATE INDEX t5Index ON T5(imsi);

CREATE TABLE T6 (
   IMSI varchar(15) NOT NULL,
   TD integer NOT NULL,
   CREATED timestamp NOT NULL
);PARTITION TABLE T6 ON COLUMN IMSI;
CREATE INDEX t6Index ON T6(imsi);

CREATE TABLE T7 (
   IMSI varchar(15) NOT NULL,
   TE integer NOT NULL,
   CREATED timestamp NOT NULL
);PARTITION TABLE T7 ON COLUMN IMSI;
CREATE INDEX t7Index ON T7(imsi);

CREATE TABLE SUPER (
   IMSI varchar(15) NOT NULL,
   C1 varchar(15) ,
   C2 varchar(20),
    AOA integer DEFAULT NULL,
    TA integer DEFAULT NULL,
    TB integer  DEFAULT NULL,
    TC integer DEFAULT  NULL,
    TD integer DEFAULT NULL,
    TE integer DEFAULT NULL,
   CREATED timestamp NOT NULL,
   PRIMARY KEY (IMSI)
);PARTITION TABLE SUPER ON COLUMN IMSI;

SELECT Table1.IMSI, Table1.c1,Table1.c2,Table2.aoa,Table2.Created FROM T1 Table1, T2 Table2 Where Table1.IMSI=Table2.IMSI;

 */
	public static void main(String[] args) throws Exception {
        Random rand= new Random();
        String[] c1=new String[11];
        String[] c2=new String[11];
        String[] columns=new String[4];
        columns[0]="IMSI";
        columns[1]="C1";
        columns[2]="C2";
        columns[3]="CREATED";


        c1[0]="Doug";
        c1[1]="Min";
        c1[2]="Simon";
        c1[3]="Seeta";
        c1[4]="Dheeraj";
        c1[5]="Sarah";
        c1[6]="Alan";
        c1[7]="David";
        c1[8]="Bob";
        c1[9]="Dude";
        c1[10]="Doug1";

        c2[0]="Dougs";
        c2[1]="Mins";
        c2[2]="Simons";
        c2[3]="Seetas";
        c2[4]="Dheerajs";
        c2[5]="Sarahs";
        c2[6]="Alans";
        c2[7]="Davids";
        c2[8]="Bobs";
        c2[9]="Dudes";
        c2[10]="Dougs2";
        String column1="";
        String column2="";
        String column3="";
        int aoa;
        int ta;
		Client client = ClientFactory.createClient();
	//	client.createConnection("54.90.179.48");
    //    client.createConnection("54.165.216.194");
    //    client.createConnection("18.208.228.192");
       client.createConnection("192.168.17.138");
			//VoltBulkLoader bulkLoader = client.getNewBulkLoader("TEST_TABLE", 5000, new TestCallback());
        VoltBulkLoader bulkLoader1 = client.getNewBulkLoader("SUPER", 100,true,columns,new TestCallback());

/*        VoltBulkLoader bulkLoader2 = client.getNewBulkLoader("T2", 100,true, new TestCallback());
        VoltBulkLoader bulkLoader3 = client.getNewBulkLoader("T3", 100,true, new TestCallback());
        VoltBulkLoader bulkLoader4 = client.getNewBulkLoader("T4", 100,true, new TestCallback());
       VoltBulkLoader bulkLoader5 = client.getNewBulkLoader("T5", 100,true, new TestCallback());
       VoltBulkLoader bulkLoader6 = client.getNewBulkLoader("T6", 100,true, new TestCallback());
        VoltBulkLoader bulkLoader7 = client.getNewBulkLoader("T7", 100,true, new TestCallback());
*/

        VoltTable T1;
        VoltTable T2;
        VoltTable T3;
        VoltTable T4;
        VoltTable T5;
        VoltTable T6;
        VoltTable T7;


        //Just plain bulk insert values.
		//for(int i=0; i<Integer.MAX_VALUE; i++) {
	//		bulkLoader.insertRow(i, i, i, new Date());
	//	}
        // For T1
        for(int i=0; i<1000001; i++) {
            column1 = String.valueOf(i);
            column2 = c1[rand.nextInt(10)];
            column3 = c2[rand.nextInt(10)];
            //bulkLoader1
            bulkLoader1.insertRow(i, column1, column2, column3, new Date().getTime());

   //         client.callProcedure("@AdHoc", "UPSERT INTO SUPER (imsi,c1,c2,created) values ('" + column1 + "','" + column2 + "','" + column3 + "',now());");
        }
        // For T2
   /*     for(int i=0; i<1000001; i++) {
            column1=String.valueOf(i);
          aoa=rand.nextInt(10);
            bulkLoader2.insertRow(i,column1,aoa, new Date());
           client.callProcedure("@AdHoc", "UPSERT INTO SUPER (imsi,aoa,created) values ('"+column1+"'," +aoa + ",now());");

        }
        // For T3
        for(int i=0; i<1000001; i++) {
            column1=String.valueOf(i);
            ta=rand.nextInt(10);
            bulkLoader3.insertRow(i,column1,ta, new Date());
            client.callProcedure("@AdHoc", "UPSERT INTO SUPER (imsi,ta,created) values ('"+column1+"'," +ta + ",now());");

        }
        // For T4
        for(int i=0; i<1000001; i++) {
            column1=String.valueOf(i);
            ta=rand.nextInt(10);
            bulkLoader4.insertRow(i,column1,ta, new Date());
            client.callProcedure("@AdHoc", "UPSERT INTO SUPER (imsi,ta,created) values ('"+column1+"'," +ta + ",now());");

        }
        // For T5
        for(int i=0; i<1000001; i++) {
            column1=String.valueOf(i);
            ta=rand.nextInt(10);
            bulkLoader5.insertRow(i,column1,ta, new Date());
            client.callProcedure("@AdHoc", "UPSERT INTO SUPER (imsi,tb,created) values ('"+column1+"'," +ta + ",now());");

        }
        // For T6
        for(int i=0; i<1000001; i++) {
            column1=String.valueOf(i);
            ta=rand.nextInt(10);
            bulkLoader6.insertRow(i,column1,ta, new Date());
           client.callProcedure("@AdHoc", "UPSERT INTO SUPER (imsi,tc,created) values ("+column1+"," +ta + ",now());");

        }
        // For T7
        for(int i=0; i<1000001; i++) {
            column1=String.valueOf(i);
            ta=rand.nextInt(10);
            bulkLoader7.insertRow(i,column1,ta, new Date());
           client.callProcedure("@AdHoc", "UPSERT INTO SUPER (imsi,tb,created) values ("+column1+"," +ta + ",now());");

        }
*/	}
	
	private static class TestCallback implements BulkLoaderFailureCallBack {

		@Override
		public void failureCallback(Object arg0, Object[] arg1, ClientResponse arg2) {
			System.out.println("Bulkloader failure " + arg0 + " " + arg1 + " " + arg2.getStatusString());
		}
		
	}
}
