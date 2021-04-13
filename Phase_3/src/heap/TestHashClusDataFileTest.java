package heap;


import java.io.File;
import java.util.function.Function;

import global.AttrType;
import global.GlobalConst;
import global.RID;
import global.SystemDefs;
import hashindex.ClusHIndex;
import hashindex.ClusHIndexScan;
import hashindex.HashBucket;
import hashindex.HashKey;
import hashindex.HashUtils;

public class TestHashClusDataFileTest implements GlobalConst {

	public static void main(String[] args) {
		
		System.out.println("Start");
		
		TestHashClusDataFileTest thiss = new TestHashClusDataFileTest();
		try {
			thiss.testInsert();
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		System.out.println("End");
	}
	
	private void testInsert() throws Exception {
		String INDEX_NAME = "huehuehue";
		ClusHIndex index = new ClusHIndex("clhdf"+INDEX_NAME, INDEX_NAME,AttrType.attrString, 4,80);
		System.out.println("\n\nNOW INSERTING SOME DATA IN INDEX \n");
		for (int k = 10; k < 10; k++) {
			HashUtils.log("------------------------------------------------------");
			for (int i = 0; i < 10; i++) {
				HashUtils.log("\nooooooooooooooo");
				
				HashKey key= new HashKey(generateRandomChars(5));
				//HashKey key = new HashKey(k);
				byte[] arr = new byte[key.size()];
				key.writeToByteArray(arr, 0);
				Tuple tup = new Tuple(arr, 0, arr.length);
				RID loc = index.insert(key, tup);
//				printPinnedPages();
			}
			HashUtils.log("------------------------------------------------------\n");
		}
		//System.out.println(index.getHeaderPage().get_EntriesCount());
		Function<byte[],String> mapper= (a)->{
			try {
				return new HashKey(a, 0).toString();
			}catch (Exception e) {
				e.printStackTrace();
				return e.getMessage();
			}
		};
		System.out.println("\n\nPRINTING ALL BUCKETS OF THE INDEX \n");
		for(int i =0;i<index.getHeaderPage().get_NumberOfBuckets();i++) {
			System.out.println("BUCKET: "+i);
			HashBucket bucket = new HashBucket(index.getHeaderPage().get_NthBucketName(i));
			//bucket.printToConsole();
		}
		System.out.println("\n\nPRINTING THE DATA FILE \n");
		//index.getDataFile().printToConsole(mapper);
		index.close();
		index = new ClusHIndex("clhdf"+INDEX_NAME, INDEX_NAME);
		System.out.println("\n\nNOW TESTING SCAN OF INDEX WITH SEARCH KEY \n");
		ClusHIndexScan scan = index.new_scan(new HashKey(13));
		Tuple tup= null;
		do {
			tup = scan.get_next();
			if(tup == null) {
				System.out.println("breaking from scan loop");
				break;
			}
			System.out.println("scan.get_next(): "+mapper.apply(tup.getTupleByteArray()));
		} while (tup!=null);
		index.close();
	}
	public static String generateRandomChars( int length) {
		String candidateChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
	    StringBuilder sb = new StringBuilder();
	    java.util.Random random = new java.util.Random();
	    for (int i = 0; i < length; i++) {
	        sb.append(candidateChars.charAt(random.nextInt(candidateChars
	                .length())));
	    }

	    return sb.toString();
	}
	
	private void testFile() throws Exception {
		ClusHIndexDataFile file = new ClusHIndexDataFile("whateveer");
		for (int i = 10; i < 100; i++) {
			//HashKey key = new HashKey(i+"laskdhlaskdlaskhdlaskdhlaskdlaskhdalskdhlaskdhalskdhlaskdhlahdlaskdhalksaskdhaskdhaskdhlaskdhlahsd"+i);
			HashKey key = new HashKey(i);
			byte[] arr= new byte[key.size()];
			key.writeToByteArray(arr, 0);
			RID loc  = file.insertRecordToNewPage(arr);
			
		}
		
		Scan scan = file.openScan();
		RID rid = new RID();
		Tuple tup;
		boolean done = false;
		while (!done) {
			tup = scan.getNext(rid);

			if (tup == null) {
				done = true;
				break;
			}
			HashKey scannedHashKey = new HashKey(tup.returnTupleByteArray(), 0);
			HashUtils.log("Location: "+rid+" - "+scannedHashKey);
				
		}
		scan.closescan();
		
	}




	public TestHashClusDataFileTest() {
		long time=System.currentTimeMillis();
		time=10;
		String dbpath = "HASHTEST" + time + ".minibase-db";
		File file = new File(dbpath);
		System.out.println("file.delete(): "+file.delete());;
		//SystemDefs.MINIBASE_RESTART_FLAG=true;
		SystemDefs sysdef = new SystemDefs(dbpath, 5000, 100, "Clock");

	}



	
	public static void printPinnedPages() {
		System.out.println("pin: "+(SystemDefs.JavabaseBM.getNumBuffers()- SystemDefs.JavabaseBM.getNumUnpinnedBuffers()));

	}

}

