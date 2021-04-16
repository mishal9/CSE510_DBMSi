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
	
	/* table for index */
	private Table skytable;
	
	/* table for output */
	private Table skyouttable;
    
    public SkylineQueryDriver(int[] pref_list, String tablename, int n_pages, String outtablename, String skyline_algo){
        this.pref_list = pref_list;
        this.tablename = tablename;
        this.n_pages = n_pages;
        this.outtablename = outtablename;
        this.skyline_algo = skyline_algo;
        this.skytable = SystemDefs.JavabaseDB.get_relation(tablename);
        
        if ( outtablename.length() > 0 ) {
        	skyouttable = new Table(outtablename, "MATER");
        	skyouttable.setTable_data_file(skytable.getTable_data_file());
        	skyouttable.setTable_attr_name(skytable.getTable_attr_name());
        	skyouttable.setTable_attr_size(skytable.getTable_attr_size());
        	skyouttable.setTable_num_attr(skytable.getTable_num_attr());
        	skyouttable.setTable_tuple_size(skytable.getTable_tuple_size());
        	skyouttable.setTable_attr_type(skytable.getTable_attr_type());
        }
        else {
        	skyouttable = null;
        }
        
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
    	close();
    }

    private void close() {
    	if ( skyouttable != null ) {
    		boolean[] bunc = new boolean[skyouttable.getTable_num_attr()];
    		boolean[] hunc = new boolean[skyouttable.getTable_num_attr()];
    		for ( int i=0; i<bunc.length; i++ ) {
    			hunc[i] = false;
    			bunc[i] = false;
    		}
    		skyouttable.setBtree_unclustered_attr(bunc);
    		skyouttable.setHash_unclustered_attr(hunc);
    		skyouttable.add_table_to_global_queue();
    	}
    }
    
    private void runBtreeSky() {
    	try {
	    	//Heapfile f = new Heapfile(skytable.getTable_heapfile());
			//BtreeGeneratorUtil.generateAllBtreesForHeapfile( skytable.getTable_heapfile(), f, skytable.getTable_attr_type(), skytable.getTable_attr_size());
			//System.out.println("Btree DATABASE CREATED");
			
			//limiting buffer pages in BufMgr
			//SystemDefs.JavabaseBM.limit_memory_usage(true, this._n_pages);
			
			int amt_of_mem = 100; // TODO what should this be?
			Iterator am1 = null;
			String relationName = skytable.getTable_heapfile();
			
			//get only the btree indexes specified by the the pref_list array
			//IndexFile[] index_file_list = BtreeGeneratorUtil.getBtreeSubset(this.pref_list);
			IndexFile[] index_file_list = get_btree_index_files();
			System.out.println("Btree DATABASE CREATED");
			PCounter.initialize();
			BTreeSky btreesky = new BTreeSky(skytable.getTable_attr_type(), skytable.getTable_num_attr(), skytable.getTable_attr_size(), amt_of_mem, am1, relationName, this.pref_list,
											 this.pref_list.length, index_file_list, this.n_pages);
			btreesky.debug = false;
			int numSkyEle = 0;
			Tuple skyEle = btreesky.get_next(); // first sky element
			System.out.print("First Sky element is: ");
			skyEle.print(skytable.getTable_attr_type());
			add_to_mater_table(skyEle);
			numSkyEle++;
			while (skyEle != null) {
				skyEle = btreesky.get_next(); // subsequent sky elements
				if (skyEle == null) {
					System.out.println("No more sky elements");
					break;
				}
				add_to_mater_table(skyEle);
				numSkyEle++;
				System.out.print("Sky element is: ");
				skyEle.print(skytable.getTable_attr_type());
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
        int numSkyEle = 0;
    	int COLS = skytable.getTable_num_attr();
        FldSpec[] projlist = new FldSpec[COLS + 1];
        RelSpec rel = new RelSpec(RelSpec.outer);
        for( int i=0; i<COLS; i++ ) {
            projlist[i] = new FldSpec(rel, i+1);
        }

        projlist[COLS] = new FldSpec(rel, 1);

        AttrType[] attrType_for_proj = new AttrType[COLS];

        for(int i=0;i<COLS;i++)
            attrType_for_proj[i] = skytable.getTable_attr_type()[i];

        OurFileScan fscan = null;

        try {
            fscan = new OurFileScan(skytable.getTable_heapfile(), attrType_for_proj, skytable.getTable_attr_size(), (short) COLS, COLS, projlist, null, this.pref_list);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        // Sort "test1sortPref.in"

        AttrType[] attrType_for_sort = new AttrType[COLS+1];

        for(int i=0;i<COLS;i++) {
            attrType_for_sort[i] = skytable.getTable_attr_type()[i];
        }
        attrType_for_sort[COLS] = new AttrType(AttrType.attrReal);

        //SystemDefs.JavabaseBM.limit_memory_usage(true, _n_pages);

        Sort sort = null;
        try {
            sort = new Sort(attrType_for_sort, (short) (COLS+1), skytable.getTable_attr_size(), fscan, (COLS+1), new TupleOrder(TupleOrder.Descending), 32, this.n_pages);
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
						                    (short)skytable.getTable_tuple_size(),
						                    skytable.getTable_heapfile(),
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
                add_to_mater_table(temp);
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
                	add_to_mater_table(temp);
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
    
    private BTreeFile[] get_btree_index_files() throws GetFileEntryException, PinPageException, ConstructPageException {
    	BTreeFile[] btreeFileArray = new BTreeFile[this.pref_list.length];
    	
    	for ( int i=0; i<this.pref_list.length; i++ ) {
    		if ( skytable.unclustered_index_exist(this.pref_list[i], "btree") ) {
    			BTreeFile btf  = new BTreeFile(skytable.get_unclustered_index_filename(this.pref_list[i], "btree") );
    			btreeFileArray[i] = btf;
    		}
    		else {
    			skytable.create_unclustered_index(this.pref_list[i], "btree");
    			BTreeFile btf  = new BTreeFile(skytable.get_unclustered_index_filename(this.pref_list[i], "btree") );
    			btreeFileArray[i] = btf;
    		}
    	}
    	
    	return btreeFileArray;
    }
    
    private void add_to_mater_table(Tuple temp) {
    	try {
    		if ( skyouttable == null ) {
    			return;
    		}
			Tuple t = TupleUtils.getEmptyTuple(skyouttable.getTable_attr_type(), skyouttable.getTable_attr_size());
			t.tupleCopy(temp);
			Heapfile outheapfile = new Heapfile(skyouttable.getTable_heapfile());
			RID newrid = outheapfile.insertRecord(t.getTupleByteArray());
		} catch (InvalidTypeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidTupleSizeException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (HFException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (HFBufMgrException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (HFDiskMgrException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidSlotNumberException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SpaceNotAvailableException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    }
}