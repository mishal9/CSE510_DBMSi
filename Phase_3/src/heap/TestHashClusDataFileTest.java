package heap;


import java.io.File;

import global.GlobalConst;
import global.RID;
import global.SystemDefs;
import hashindex.HashKey;
import hashindex.HashUtils;

public class TestHashClusDataFileTest implements GlobalConst {

	public static void main(String[] args) {
		
		System.out.println("Start");
		
		TestHashClusDataFileTest thiss = new TestHashClusDataFileTest();
		try {
			thiss.testFile();
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		System.out.println("End");
	}
	

	
	
	private void testFile() throws Exception {
		HashClustDataFile file = new HashClustDataFile("whateveer");
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

