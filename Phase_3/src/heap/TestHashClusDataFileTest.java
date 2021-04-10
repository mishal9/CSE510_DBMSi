package heap;


import java.io.File;
import java.io.IOException;
import java.util.function.Consumer;
import java.util.function.Function;

import btree.KeyNotMatchException;
import global.AttrType;
import global.GlobalConst;
import global.RID;
import global.SystemDefs;
import hashindex.ClusHIndex;
import hashindex.ClusHIndexScan;
import hashindex.HashBucket;
import hashindex.HashEntry;
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
		ClusHIndex index = new ClusHIndex("huehuehue",AttrType.attrInteger, 4,80);
		for (int k = 10; k < 15; k++) {
			for (int i = 0; i < 10; i++) {
				// HashKey key= new HashKey(i+"laskdhlaskdlaskhdlaskdhlaskdlaskhdalskdhlaskdhalskdhlaskdhlahdlaskdhalksaskdhaskdhaskdhlaskdhlahsd"+i);
				HashKey key = new HashKey(k);
				byte[] arr = new byte[key.size()];
				key.writeToByteArray(arr, 0);
				Tuple tup = new Tuple(arr, 0, arr.length);
				RID loc = index.insert(key, tup);

			}
		}
		Function<byte[],String> mapper= (a)->{
			try {
				return new HashKey(a, 0).toString();
			}catch (Exception e) {
				e.printStackTrace();
				return e.getMessage();
			}
		};
		for(int i =0;i<index.getHeaderPage().get_NumberOfBuckets();i++) {
			System.out.println("BUCKET: "+i);
			HashBucket bucket = new HashBucket(index.getHeaderPage().get_NthBucketName(i));
			bucket.printToConsole();
		}
		index.getDataFile().printToConsole(mapper);
		ClusHIndexScan scan = index.new_scan(new HashKey(23));
		Tuple tup= null;
		do {
			tup = scan.get_next();
			if(tup == null) {
				System.out.println("breaking from scan loop");
				break;
			}
			System.out.println("scan.get_next(): "+tup);
		} while (tup!=null);

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

