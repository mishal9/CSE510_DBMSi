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
				runblockNestedSky();
				break;
			case "SFS":
				//TBD run SFS with proper params
				runSortFirstSky();
				break;
			case "BTS":
				//TBD run btree sky with proper params
				runBtreeSky();
				break;
			case "BTSS":
				//TBD run btree sorted sky with proper params
				//TBD modify btree sort sky to handle input as a heap file
				break;
			default:
				break;
    	}
    }

    private void runBtreeSky() {
    	try {
	    	Table table = SystemDefs.JavabaseDB.get_relation(tablename);
	    	Heapfile f = new Heapfile(table.getTable_heapfile());
			BtreeGeneratorUtil.generateAllBtreesForHeapfile( table.getTable_heapfile(), f, table.getTable_attr_type(), table.getTable_attr_size());
			System.out.println("Btree DATABASE CREATED");
			
			//limiting buffer pages in BufMgr
			//SystemDefs.JavabaseBM.limit_memory_usage(true, this._n_pages);
			
			int amt_of_mem = 100; // TODO what should this be?
			Iterator am1 = null;
			String relationName = table.getTable_heapfile();
			
			//get only the btree indexes specified by the the pref_list array
			IndexFile[] index_file_list = BtreeGeneratorUtil.getBtreeSubset(this.pref_list);
			PCounter.initialize();
			BTreeSky btreesky = new BTreeSky(table.getTable_attr_type(), table.getTable_num_attr(), table.getTable_attr_size(), amt_of_mem, am1, relationName, this.pref_list,
											 this.pref_list.length, index_file_list, this.n_pages);
			btreesky.debug = false;
			int numSkyEle = 0;
			Tuple skyEle = btreesky.get_next(); // first sky element
			System.out.print("First Sky element is: ");
			skyEle.print(table.getTable_attr_type());
			numSkyEle++;
			while (skyEle != null) {
				skyEle = btreesky.get_next(); // subsequent sky elements
				if (skyEle == null) {
					System.out.println("No more sky elements");
					break;
				}
				numSkyEle++;
				System.out.print("Sky element is: ");
				skyEle.print(table.getTable_attr_type());
			}
			System.out.println("Skyline Length: "+numSkyEle);
			btreesky.close();
			System.out.println("End of runBtreeSky");
			System.out.println("Number of Disk reads: "+ PCounter.get_rcounter());
			System.out.println("Number of Disk writes: "+ PCounter.get_wcounter());
			PCounter.initialize();
    	} catch ( Exception e ) {
    		e.printStackTrace();
    	}
	}

    private void runSortFirstSky() {
    	Table table = SystemDefs.JavabaseDB.get_relation(tablename);
        int numSkyEle = 0;
    	int COLS = table.getTable_num_attr();
        FldSpec[] projlist = new FldSpec[COLS + 1];
        RelSpec rel = new RelSpec(RelSpec.outer);
        for( int i=0; i<COLS; i++ ) {
            projlist[i] = new FldSpec(rel, i+1);
        }

        projlist[COLS] = new FldSpec(rel, 1);

        AttrType[] attrType_for_proj = new AttrType[COLS];

        for(int i=0;i<COLS;i++)
            attrType_for_proj[i] = table.getTable_attr_type()[i];

        OurFileScan fscan = null;

        try {
            fscan = new OurFileScan(table.getTable_heapfile(), attrType_for_proj, table.getTable_attr_size(), (short) COLS, COLS, projlist, null, this.pref_list);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        // Sort "test1sortPref.in"

        AttrType[] attrType_for_sort = new AttrType[COLS+1];

        for(int i=0;i<COLS;i++) {
            attrType_for_sort[i] = table.getTable_attr_type()[i];
        }
        attrType_for_sort[COLS] = new AttrType(AttrType.attrReal);

        //SystemDefs.JavabaseBM.limit_memory_usage(true, _n_pages);

        Sort sort = null;
        try {
            sort = new Sort(attrType_for_sort, (short) (COLS+1), table.getTable_attr_size(), fscan, (COLS+1), new TupleOrder(TupleOrder.Descending), 32, this.n_pages);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        // pass this sort object to the sortfirstsky

        SortFirstSky sortFirstSky = null;
        try {
            sortFirstSky = new SortFirstSky(attrType_for_sort,
						                    (short) COLS,
						                    null,
						                    sort,
						                    (short)table.getTable_tuple_size(),
						                    table.getTable_heapfile(),
						                    this.pref_list,
						                    this.pref_list.length,
						                    this.n_pages);
            fscan.close();
            sort.close();
            System.out.println("Skyline object: ");
            Tuple temp;
            try {
                temp = sortFirstSky.get_next();
                while (temp!=null) {
                    temp.printTuple(attrType_for_proj);
                    numSkyEle++;
                    temp = sortFirstSky.get_next();
                }

            } catch (Exception e) {
                e.printStackTrace();
            }


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // clean up
            try {
                sortFirstSky.close();
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

    private void runblockNestedSky(){
    	Table table = SystemDefs.JavabaseDB.get_relation(tablename);
        BlockNestedLoopsSky blockNestedLoopsSky = null;
        int numSkyEle = 0;
        try {
            blockNestedLoopsSky = new BlockNestedLoopsSky(table.getTable_attr_type(),
									                    (short)table.getTable_num_attr(),
									                    table.getTable_attr_size(),
									                    null,
									                    table.getTable_heapfile(),
									                    this.pref_list,
									                    this.pref_list.length,
									                    this.n_pages);

            System.out.println("Printing the Block Nested Loop Skyline");
            Tuple temp;
            try {
                temp = blockNestedLoopsSky.get_next();
                while (temp!=null) {
                    temp.print(table.getTable_attr_type());
                    numSkyEle++;
                    temp = blockNestedLoopsSky.get_next();
                }
               
            } catch (Exception e) {
                e.printStackTrace();
            }
       
        } catch (IOException | FileScanException | TupleUtilsException | InvalidRelation e) {
            e.printStackTrace();
        } finally {
            blockNestedLoopsSky.close();
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
        System.out.println("\n");
    }
}