package driver;


import java.io.*;
import java.util.*;

import btree.*;
import bufmgr.*;
import chainexception.ChainException;
import diskmgr.DiskMgrException;
import diskmgr.FileIOException;
import diskmgr.InvalidPageNumberException;
import diskmgr.PCounter;
import heap.*;
import global.*;

import index.IndexException;
import iterator.*;
import iterator.Iterator;
import tests.TestDriver;


//watching point: RID rid, some of them may not have to be newed.

class SkylineQueryDriver extends TestDriver implements GlobalConst
{
    /* list of skyline attributes */
	private int[] pref_list;
	
	/* tablename on which skyline is to be calculated */
	private String tablename;
	
	/* number of buffer pages available */
	private int n_pages;
	
	/* table to store the output of skyline */
	private String outtablename;
	
	/* skyline algorithm to be used */
	private String skyline_algo;
    
    public SkylineQueryDriver(int[] pref_list, String tablename, int n_pages, String outtablename, String skyline_algo){
        this.pref_list = pref_list;
        this.tablename = tablename;
        this.n_pages = n_pages;
        this.outtablename = outtablename;
        this.skyline_algo = skyline_algo;
        print_attr();
        run_skyline();
    }
    
    private void run_skyline() {
    	switch ( skyline_algo ) {
			case "NLS":
				//TBD run NLS with proper params
				runNestedLoopSky();
				break;
			case "BNLS":
				//TBD run BNLS with proper params
				break;
			case "SFS":
				//TBD run SFS with proper params
				break;
			case "BTS":
				//TBD run btree sky with proper params
				break;
			case "BTSS":
				//TBD run btree sorted sky with proper params
				break;
			default:
				break;
    	}
    }
    
    private void runNestedLoopSky(){
        NestedLoopsSky nestedLoopsSky = null;
        int numSkyEle =0;
        try {
        	Table table = SystemDefs.JavabaseDB.get_relation(tablename);
        	System.out.println("Printint the attrtype in table "+Arrays.toString(table.getTable_attr_type()));
            nestedLoopsSky = new NestedLoopsSky(table.getTable_attr_type(),
							                    (short)table.getTable_num_attr(),
							                    table.getTable_attr_size(),
							                    null,
							                    table.getTable_heapfile(),
							                    this.pref_list,
							                    this.pref_list.length,
							                    this.n_pages);

            System.out.println("Printing the Nested Loop Skyline");
            Tuple temp = nestedLoopsSky.get_next();
            while (temp!=null) {
            	numSkyEle++;
                temp.print(table.getTable_attr_type());
                temp = nestedLoopsSky.get_next();
            }
        
		} catch (IOException | JoinsException | InvalidTupleSizeException | InvalidTypeException | PageNotReadException
				| PredEvalException | UnknowAttrType | FieldNumberOutOfBoundException | WrongPermat
				| TupleUtilsException | FileScanException | InvalidRelation e) {
			e.printStackTrace();
		} finally {
            // clean up
            try {
                nestedLoopsSky.close();
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
        System.out.println("Skyline Length: "+numSkyEle);
        System.out.println("Number of Disk reads: "+ PCounter.get_rcounter());
        System.out.println("Number of Disk writes: "+ PCounter.get_wcounter());
        PCounter.initialize();
    }

    
    public void print_attr() {
    	System.out.println("****************************skyline*****************************************");
    	System.out.println("Will run skyline with params: ");
    	System.out.println("SKyline algo: "+skyline_algo);
        System.out.println("N pages: "+ n_pages);
        System.out.println("Pref list: "+Arrays.toString(pref_list));
        System.out.println("Pref list length: "+pref_list.length);
        System.out.println("Tablename: "+tablename);
        System.out.println("Outtablename: "+ outtablename);
    }
    
    /* calculates the NLS on tablename */
    private String calculate_NLS() {
    	return null;
    }
    
    /* calculates the skyline as per params */
    public String calculate_skyline() {
    	switch ( skyline_algo ) {
	    	case "NLS":
				//TBD run NLS with proper params
	    		return calculate_NLS();
			case "BNLS":
				//TBD run BNLS with proper params
				break;
			case "SFS":
				//TBD run SFS with proper params
				break;
			case "BTS":
				//TBD run btree sky with proper params
				break;
			case "BTSS":
				//TBD run btree sorted sky with proper params
				break;
			default:
				return null;
    	}
    	return null;
    }
}