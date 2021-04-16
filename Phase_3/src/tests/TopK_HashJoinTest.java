package tests;

import java.io.IOException;

import bufmgr.*;
import diskmgr.*;
import global.*;
import heap.*;
import index.*;
import iterator.*;
import java.util.*;
import btree.*;

class TopK_HashJoinDriver extends TestDriver
implements GlobalConst {
	
	public TopK_HashJoinDriver() {
	     super("TopKNraJoinDriver");
	}
	
	public boolean runTests () throws HFDiskMgrException, HFException, HFBufMgrException, IOException {

        System.out.println ("\n" + "Running " + testName() + " tests...." + "\n");
        SystemDefs sysdef = new SystemDefs( dbpath, 8000, 100, "Clock" );

        String newdbpath;
        String newlogpath;
        String remove_logcmd;
        String remove_dbcmd;
        String remove_cmd = "/bin/rm -rf ";

        newdbpath = dbpath;
        newlogpath = logpath;

        remove_logcmd = remove_cmd + logpath;
        remove_dbcmd = remove_cmd + dbpath;

        remove_logcmd = remove_cmd + newlogpath;
        remove_dbcmd = remove_cmd + newdbpath;

        boolean _pass = runAllTests();

        System.out.println ("\n" + "..." + testName() + " tests ");
        System.out.println (_pass==OK ? "completely successfully" : "failed");
        System.out.println (".\n\n");

        return _pass;
    }
	
	protected boolean test1() {
        System.out.println("------------------------ TEST 2 --------------------------");
        
        BTFileScan scan1;
        KeyDataEntry entry1;
        System.out.println("CombinedBTreeIndex scanning");
        
        int [] pref_list1 = new int[] {0,1};
        int pref_list_length1 = 2;
        

        GenerateIndexFiles obj = new GenerateIndexFiles();
        IndexFile indexFile1 = null;
        IndexFile indexFile2 = null;

		try {
			indexFile1 = obj.createCombinedBTreeIndex("/Users/kunjpatel/Desktop/CSE510_DBMSi/Phase_3/data/nra1.txt",pref_list1, pref_list_length1);
			indexFile2 = obj.createCombinedBTreeIndex("/Users/kunjpatel/Desktop/CSE510_DBMSi/Phase_3/data/nra2.txt",pref_list1, pref_list_length1);

		} catch (Exception e) {
			e.printStackTrace();
		} 
		
        System.out.println("Index created! ");
        
        AttrType[] attrType = new AttrType[2];
        attrType[0] = new AttrType(AttrType.attrReal);
        attrType[1] = new AttrType(AttrType.attrReal);
        
        short[] attrSize = null;
        
        
        FldSpec joinAttr = new FldSpec(new RelSpec(RelSpec.outer), 1);
        FldSpec mergeAttr = new FldSpec(new RelSpec(RelSpec.outer), 2);
 
        try {
			TopK_HashJoin tkhj = new TopK_HashJoin(
					attrType, attrType.length, attrSize,
					joinAttr,
					mergeAttr,
					attrType, attrType.length, attrSize,
					joinAttr,
					mergeAttr,
					"heap_AAA1",
					"heap_AAA2",
					2,
					100
					);
		}  catch (Exception e) {
			e.printStackTrace();
		}
        
        boolean status = OK;
		return status;
    }
}

public class TopK_HashJoinTest  {
	
	public static void main(String[] argv) throws HFDiskMgrException, HFException, HFBufMgrException, IOException {
		TopK_HashJoinDriver tknj = new TopK_HashJoinDriver();
		tknj.runTests();
	}

}