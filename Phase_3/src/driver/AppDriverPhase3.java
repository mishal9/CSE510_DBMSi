package driver;


import java.io.*;
import java.util.*;

import btree.*;
import bufmgr.*;
import chainexception.ChainException;
import diskmgr.PCounter;
import heap.*;
import global.*;

import index.IndexException;
import iterator.*;
import iterator.Iterator;
import tests.TestDriver;


//watching point: RID rid, some of them may not have to be newed.

class DriverPhase3 extends TestDriver implements GlobalConst
{
    protected String dbpath;
    protected String logpath;

    private static RID   rid;
    private static Heapfile  f = null;
    private boolean status = OK;
    private static String _fileName;
    private static int[] _pref_list;
    private static int _n_pages;
    private static int COLS;
    private static final String hFile = "hFile.in";
    private static AttrType[] attrType;
    private short[] attrSize;
    // create an iterator by open a file scan
    private static FldSpec[] projlist;
    private static RelSpec rel = new RelSpec(RelSpec.outer);

    private static boolean individualBTreeIndexesCreated;
    private static int _t_size;
    static String dataFile = "";
    private int numberOfDimensions = 0;

    public DriverPhase3(){
        super("main");
    }
    
    /* This function runs the main query interface which reads and processes all the queries */
    public void startQueryInterface() {
    	boolean exit_interface = false;
    	while (!exit_interface) {
    		String[] tokens = readNextCommand();
    		switch (tokens[0]) {
    			case "exit":
    				System.out.println("exiting query interface");
    				exit_interface = true;
    				break;
    			case "open_database":
    				if ( tokens.length != 2 ) {
    					System.out.println("Command not recognized");
    					printQueryHelper("open_database");
    					break;
    				}
    				System.out.println("Loading database "+ tokens[1]);
    				break;
    			case "create_table":
    				//need to create a simple or a clustered index table here
    				break;
    			case "close_database":
    				//need to close the existing db and dump all the file on the disk 
    				break;
    			case "create_index":
    				//need to create an unclustered index as specified
    				break;
    			case "insert_data":
    				//insert data to the file
    				break;
    			case "delete_data":
    				//delete the elements from the given file 
    				break;
    			case "output_table":
    				//print the entire table specified
    				break;
    			case "output_index":
    				//outputs the index if available
    				break;
    			case "skyline":
    				//computes the skyline mentioned
    				break;
    			case "groupby":
    				//calculate the group by accordingly
    				break;
    			case "join":
    				//calculate the join as specified
    				break;
    			case "topkjoin":
    				//perform the topkjoin as specified
    				break;
    			default:
    				System.out.println("Query command not recognized "+tokens[0]);
    				printQueryHelper("all");
    				break;
    		}
    	}
    }
    
    /* This function will print all the available functions and their structures incase a wrong query is entered */
    public void printQueryHelper(String s) {
    	switch (s) {
    		case "open_database":
    			System.out.println("Command: open_database dbname--> opens a database with the given name. If the db does not exist, reates the db from scratch\n\n");
    			break;
    		case "close_database":
    			System.out.println("Command: close_database--> Closes the current database and make all the files in the db persistent\n\n");
    			break;
    		case "create_table":
    			System.out.println("Command: create_table [CLUSTERED BTREE/HASH ATT_NO] FILENAME--> Create a new table and populte it with data from the given file"
    					+ ", with the following format");
    			System.out.println("\t\t1. First row of the file contains the number, n, of attributes");
    			System.out.println("\t\t2. The next rows containshe attribute names and types (INT/STR)");
    			System.out.println("\t\t3. The rest of the rows contains the data tuples");
    			System.out.println("If CLUSTERED BTREE or CLUSTERED HASH is specified, then create a  clustered index on the file on the attribute specified"
    					+ " by ATT_NO ( first attribute at index 1 and second at indexx 2 and so on.\n\n");
    			break;
    		case "create_index":
    			System.out.println("Command: create_index BTREE/HASH ATT_NO TABLENAME--> Create and unclustered index on the file on the attribute specified by"
    					+ " ATT_NO. If an index already exists, then the operation returns without any index creation. once the index is created, it needs to"
    					+ " be maintained with the insertions and deletions\n\n");
    			break;
    		case "insert_data":
    			System.out.println("Command: insert_data TABLENAME FILENAME--> Insert data to the given table from the given file\n\n");
    			break;
    		case "delete_data":
    			System.out.println("Command: delete_data TABLENAME FILENAME--> Delete data from the given table those data that appear in the give file.\n\n");
    			break;
    		case "output_table":
    			System.out.println("Command: output_table TABLENAME--> Output all the tuples in the given table.\n\n");
    			break;
    		case "output_index":
    			System.out.println("Command: output_index TABLENAME ATT_NO--> Output the keys in the (clustered or unclustered) index of the table for the given"
    					+ " attribute. If there is no index at the given attribute, then the operation outputs N/A.\n\n");
    			break;
    		case "skyline":
    			System.out.println("Command: skyline NLS/BNLS/SFS/BTS/BTSS {ATT_NO1, ...ATT_NOh} TABLENAME NPAGES [MATER OUTTABLENAME]--> Output the skyline"
    					+ " of the given table  for the given h attributes. If MATER is specified, then materialize the results by creating a new table with "
    					+ " specified outputtable name.\n\n");
    			break;
    		case "groupby":
    			System.out.println("Command: GROUPBY SORT/HASH MAX/MIN/AGG/SKY G_ATT_NO{ATT_NO1,...ATT_NOh} TABLENAME NPAGES [MATER OUTTABLENAME]--> Output the "
    					+ " result of the groupby/aggregation operation. G_ATT_NO specifies the groupby attributes. ATT_NO1 through ATT_NOh  specify the"
    					+ " aggregation attributes. If MATER is specified, then materialize the results by creating a new table with the specified output table name\n\n");
    			break;
    		case "join":
    			System.out.println("Command: JOIN NLJ/SMJ/INLJ/HJ OTABLENAME O_ATT_NO ITABLENAME I_ATT_NO OP NPAGES [MATER OUTTABLENAME]--> Output the "
    					+ " result of specified join operation on the given out and inner relations. The join condition is specified by two given attributes and "
    					+ "operator OP which belongs to  {=, <=, <, >, >=}. If MATER is specified, then materialize the results by creating a new table with"
    					+ " the specified output table name.\n\n");
    			break;
    		case "topkjoin":
    			System.out.println("Command: TOPKJOIN HASH/NRA K OTABLENAME O_J_ATT_NO O_M_ATT_NO ITABLENAME I_J_ATT_NO I_M_ATT_NO NPAGES [MATER OUTTABLENAME"
    					+ "]--> Output the result of the specified top-K-join operation on the given out and inner relations. The join condtion  is specified by "
    					+ "the two given attributes and the selection of the top-K results are performed based in the specified merge attributes. If MATER is"
    					+ " specified, then materialize the results by creating a new table with the specified output table name.\n\n");
    			break;
    			
    		case "all":
    			printQueryHelper("open_database");
    			printQueryHelper("close_database");
    			printQueryHelper("create_table");
    			printQueryHelper("create_index");
    			printQueryHelper("insert_data");
    			printQueryHelper("delete_data");
    			printQueryHelper("output_table");
    			printQueryHelper("output_index");
    			printQueryHelper("skyline");
    			printQueryHelper("groupby");
    			printQueryHelper("join");
    			printQueryHelper("topkjoin");
    			break;
    	}
    }
    
    /* This function is used to read and parse the next command in the query interface */
    public String[] readNextCommand() {
    	System.out.print(">>");
    	Scanner sc = new Scanner(System.in);
    	String input_command = sc.nextLine();
    	String[] tokens = input_command.split(" ");
    	return tokens;
    }
    
    public boolean runTests () {
        System.out.println ("\n" + "Running " + testName() + " tests...." + "\n");
        dbpath = "MINIBASE.minibase-db";
		logpath = "MINIBASE.minibase-log";
        // Each page can handle at most 25 tuples on original data => 7308 / 25 = 292
        SystemDefs sysdef = new SystemDefs(dbpath,160000, 3000,"Clock");

        // Kill anything that might be hanging around
        String newdbpath;
        String newlogpath;
        String remove_logcmd;
        String remove_dbcmd;
        String remove_cmd = "/bin/rm -rf ";

        newdbpath = dbpath;
        newlogpath = logpath;

        remove_logcmd = remove_cmd + logpath;
        remove_dbcmd = remove_cmd + dbpath;

        // Commands here is very machine dependent.  We assume
        // user are on UNIX system here
        /*try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        }
        catch (IOException e) {
            System.err.println ("IO error: "+e);
        }*/

        remove_logcmd = remove_cmd + newlogpath;
        remove_dbcmd = remove_cmd + newdbpath;

        /*try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        }
        catch (IOException e) {
            System.err.println ("IO error: "+e);
        }*/

        //Run the tests. Return type different from C++
        boolean _pass = runAllTests();

        //Clean up again
        /*try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        }
        catch (IOException e) {
            System.err.println ("IO error: "+e);
        }*/

        System.out.print ("\n" + "..." + testName() + " tests ");
        System.out.print (_pass==OK ? "completely successfully" : "failed");
        System.out.print (".\n\n");
        return _pass;
    }

    private void menu() {
        System.out.println("------------SKYLINE PROCESSING MENU ------------------");
        System.out.println("[101]   Set pref = [1]");
        System.out.println("[102]   Set pref = [1,3]");
        System.out.println("[103]   Set pref = [1,2]");
        System.out.println("[104]   Set pref = [1,3,5]");
        System.out.println("[105]   Set pref = [1,2,3,4,5]");
        System.out.println("[106]   Set n_page = 5");
        System.out.println("[107]   Set n_page = 10");
        System.out.println("[108]   Set n_page = <your_wish>");
        System.out.println("[1]  Run Nested Loop skyline on data with parameters ");
        System.out.println("[2]  Run Block Nested Loop on data with parameters ");
        System.out.println("[3]  Run Sort First Sky on data with parameters ");
        System.out.println("[4]  Run Btree Sky on data with parameters ");
        System.out.println("[5]  Run Btree Sort Sky on data with parameters ");
        System.out.println("\n[0]  Quit!");
        System.out.print("Hi, make your choice :");
    }
    
    private void dbcreationmenu() {
    	 System.out.println("------------------DB CREATION MENU ------------------");
         System.out.println("[1]   Read input data data2.txt");
         System.out.println("[2]   Read input data data3.txt");
         System.out.println("[3]   Read input data data_large_skyline.txt");
         System.out.print("Hi, make your choice :");

        String OS = System.getProperty("os.name").toLowerCase();

        int choice= GetStuffPhase3.getChoice();
         switch(choice) {
         case 1:
        	 dataFile = OS.indexOf("mac") >= 0 ? "data/demo_data/nc_2_3000_single.txt" : "data/data2.txt";
        	 numberOfDimensions = 2;
        	 break;
         case 2:
        	 dataFile = OS.indexOf("mac") >= 0 ? "data/data3.txt" : "data/data3.txt";
        	 numberOfDimensions = 5;
        	 break;
         case 3:
        	 dataFile = OS.indexOf("mac") >= 0 ? "data/data_large_skyline.txt" : "data/data_large_skyline.txt";
        	 numberOfDimensions = 2;
        	 break;
         default:
        	 System.err.println("Invalid Choice");
        	 System.exit(-1);
        	 break;        	 
         }
		try {
			System.out.println("Reading file: "+dataFile);
			readDataIntoHeap(dataFile);
			BtreeGeneratorUtil.generateAllBtreesForHeapfile(hFile, f, attrType, attrSize);
			individualBTreeIndexesCreated = true;
			System.out.println("DATABASE CREATED");
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
    }
    
    private void readDataIntoHeap(String fileName) throws IOException, InvalidTupleSizeException, InvalidTypeException, InvalidSlotNumberException, HFDiskMgrException, HFBufMgrException, HFException, HashOperationException, PageUnpinnedException, PagePinnedException, PageNotFoundException, BufMgrException {

        // Create the heap file object
        try {
            f = new Heapfile(hFile);
        }
		catch (HFException | HFBufMgrException | HFDiskMgrException | IOException e) {
			status = FAIL;
			System.err.println("*** Could not create heap file\n");
			e.printStackTrace();
			throw e;
		}
       

        if ( status == OK ) {

            // Read data and construct tuples

            File file = new File(fileName);
            Scanner sc = new Scanner(file);

            COLS = sc.nextInt();
            sc.nextLine(); // skipping the whole first line from the file as that has only 5 in it

            attrType = new AttrType[COLS];
            attrSize = new short[COLS];

            for(int i=0; i<attrType.length; i++){
                attrType[i] = new AttrType(AttrType.attrReal);
            }

            for(int i=0; i<attrSize.length; i++){
                attrSize[i] = 32;
            }

            projlist = new FldSpec[COLS];

            for(int i=0; i<attrType.length; i++){
                projlist[i] = new FldSpec(rel, i+1);;
            }

            Tuple t = new Tuple();
            try {
                t.setHdr((short) COLS,attrType, attrSize);
            }
            catch (Exception e) {
                System.err.println("*** error in Tuple.setHdr() ***");
                status = FAIL;
                e.printStackTrace();
            }

            int size = t.size();
            _t_size = t.size();
            //System.out.println("Size: "+size);

            t = new Tuple(size);
            try {
                t.setHdr((short) COLS, attrType, attrSize);
            }
            catch (Exception e) {
                System.err.println("*** error in Tuple.setHdr() ***");
                status = FAIL;
                e.printStackTrace();
            }

            while (sc.hasNextLine()) {
                // create a tuple of appropriate size

                double[] doubleArray = Arrays.stream(Arrays.stream(sc.nextLine().trim()
                        .split("\\s+"))
                        .filter(s -> !s.isEmpty())
                        .toArray(String[]::new))
                        .mapToDouble(Double::parseDouble)
                        .toArray();
                
                for(int i=0; i<doubleArray.length; i++) {
                    try {
                        t.setFloFld(i+1, (float) doubleArray[i]);
                    } catch (Exception e) {
                        status = FAIL;
                        e.printStackTrace();
                    }
                }

                try {
                    rid = f.insertRecord(t.returnTupleByteArray());
                }
                catch (Exception e) {
                    status = FAIL;
                    e.printStackTrace();
                }

                //System.out.println("RID: "+rid);
            }
            System.out.println("Number of records in Database: "+f.getRecCnt());
            sc.close();
        }
       
		SystemDefs.JavabaseBM.flushAllPages();
		
    }

    protected String testName () {
        return "Main Driver";
    }

	protected boolean runAllTests (){
        int choice=100;
        
        dbcreationmenu();
       
        while(choice!=0) {
            menu();
           
            try{
            	
                choice= GetStuffPhase3.getChoice();

                switch(choice) {
                    case 101:
                        _pref_list = new int[]{1};
                        break;

                    case 102:
                        _pref_list = new int[]{1,3};
                        break;
                    case 103:
                        _pref_list = new int[]{1,2};
                        break;
                    case 104:
                        _pref_list = new int[]{1,3,5};
                        break;

                    case 105:
                        _pref_list = new int[]{1,2,3,4,5};
                        break;

                    case 106:
                        _n_pages = 5;
                        System.out.println("n_pages set to :" + _n_pages);
                        break;

                    case 107:
                        _n_pages = 10;
                        System.out.println("n_pages set to :" + _n_pages);
                        break;

                    case 108:
                        System.out.println("Enter n_pages of your choice: ");
                        _n_pages = GetStuffPhase3.getChoice();
                        if(_n_pages<0)
                            break;
                        System.out.println("n_pages set to :" + _n_pages);
                        break;

                    case 1:
                        // call nested loop sky
                        runNestedLoopSky();
                        break;

                    case 2:
                        // call block nested loop sky
                        blockNestedSky();
                        break;

                    case 3:
                        // call sort first sky
                        runSortFirstSky();
                        break;

                    case 4:
                        // call btree sky
                    	runBtreeSky();
                        break;

                    case 5:
                        // call btree sort sky
                        runBtreeSortSky();
                        break;

                    case 0:
                    	SystemDefs.JavabaseDB.DBDestroy();
                        break;
                }


            }
            catch (Exception e) {
            	//checking for buffer full exception, then dont print full stack trace
            	boolean caught = false;
            	if (e instanceof ChainException) {
            		ChainException temp = (ChainException) e;
            		while (caught == false && temp != null) {
            			if (temp instanceof bufmgr.BufferPoolExceededException) {
            				caught = true;
            				System.err.println(temp.getMessage());
            				System.err.println(
            						"BufferPoolExceededException, insufficient buffer memory for this operation ");
            			}
            			System.err.println(temp.getMessage());
            			temp = (ChainException) temp.prev;
            		}
            	}

            	if (!caught) {
            		System.out.println("       !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
            		System.out.println("       !!         Something is wrong                    !!");
            		System.out.println("       !!     Is your DB full? then exit. rerun it!     !!");
            		System.out.println("       !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
            		e.printStackTrace();
            	}
            }
        }

        return true;
    }

    private void runNestedLoopSky(){
        System.out.println("Will run nested loop skyline with params: ");
        System.out.println("N pages: "+_n_pages);
        System.out.println("Pref list: "+Arrays.toString(_pref_list));
        System.out.println("Pref list length: "+_pref_list.length);
        PCounter.initialize();
        NestedLoopsSky nestedLoopsSky = null;
        int numSkyEle =0;
        try {
            nestedLoopsSky = new NestedLoopsSky(attrType,
                    (short)COLS,
                    attrSize,
                    null,
                    hFile,
                    _pref_list,
                    _pref_list.length,
                    _n_pages);

            System.out.println("Printing the Nested Loop Skyline");
            Tuple temp = nestedLoopsSky.get_next();
            while (temp!=null) {
            	numSkyEle++;
                temp.print(attrType);
                temp = nestedLoopsSky.get_next();
            }
        
		} catch (IOException | JoinsException | InvalidTupleSizeException | InvalidTypeException | PageNotReadException
				| PredEvalException | UnknowAttrType | FieldNumberOutOfBoundException | WrongPermat
				| TupleUtilsException | FileScanException | InvalidRelation e) {
			e.printStackTrace();
		} finally {
            status = OK;
            // clean up
            try {
                nestedLoopsSky.close();
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }
        System.out.println("Skyline Length: "+numSkyEle);
        System.out.println("Number of Disk reads: "+ PCounter.get_rcounter());
        System.out.println("Number of Disk writes: "+ PCounter.get_wcounter());
        PCounter.initialize();
    }

    private void blockNestedSky(){
        System.out.println("Will run block nested loop skyline with params: ");
        System.out.println("N pages: "+_n_pages);
        System.out.println("Pref list: "+Arrays.toString(_pref_list));
        System.out.println("Pref list length: "+_pref_list.length);

        BlockNestedLoopsSky blockNestedLoopsSky = null;
        PCounter.initialize();
        int numSkyEle = 0;
        try {
            blockNestedLoopsSky = new BlockNestedLoopsSky(attrType,
                    (short)COLS,
                    attrSize,
                    null,
                    hFile,
                    _pref_list,
                    _pref_list.length,
                    _n_pages);

            System.out.println("Printing the Block Nested Loop Skyline");
            Tuple temp;
            try {
                temp = blockNestedLoopsSky.get_next();
                while (temp!=null) {
                    temp.print(attrType);
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

    private void runSortFirstSky() {

        System.out.println("Will run sort first sky with params: ");
        System.out.println("N pages: "+_n_pages);
        System.out.println("Pref list: "+Arrays.toString(_pref_list));
        System.out.println("Pref list length: "+_pref_list.length);

        PCounter.initialize();

        /*
        try {
            fscan = new FileScan(hFile, attrType, attrSize, (short) COLS, COLS, projlist, null);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        SortFirstSky sortFirstSky = null;
        try {
            sortFirstSky = new SortFirstSky(attrType,
                (short) COLS,
                attrSize,
                fscan,
                (short)_t_size,
                hFile,
                _pref_list,
                _pref_list.length,
                _n_pages);


            while(sortFirstSky.hasNext()) {
                System.out.println("Skyline object: ");
                sortFirstSky.get_next().print(attrType);
            }


            
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            status = OK;
            // clean up
            try {
                sortFirstSky.close();
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }

         */

        FldSpec[] projlist = new FldSpec[COLS+1];
        RelSpec rel = new RelSpec(RelSpec.outer);
        for(int i=0;i<COLS;i++)
            projlist[i] = new FldSpec(rel, i+1);

        projlist[COLS] = new FldSpec(rel, 1);

        AttrType[] attrType_for_proj = new AttrType[COLS];

        for(int i=0;i<COLS;i++)
            attrType_for_proj[i] = new AttrType(AttrType.attrReal);

        OurFileScan fscan = null;

        try {
            fscan = new OurFileScan(hFile, attrType_for_proj, null, (short) COLS, COLS, projlist, null, _pref_list);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        // Sort "test1sortPref.in"

        AttrType[] attrType_for_sort = new AttrType[COLS+1];

        for(int i=0;i<COLS;i++) {
            attrType_for_sort[i] = new AttrType(AttrType.attrReal);
        }
        attrType_for_sort[COLS] = new AttrType(AttrType.attrReal);

        SystemDefs.JavabaseBM.limit_memory_usage(true, _n_pages);

        Sort sort = null;
        try {
            sort = new Sort(attrType_for_sort, (short) (COLS+1), attrSize, fscan, (COLS+1), new TupleOrder(TupleOrder.Descending), 32, _n_pages/2);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }
        System.out.println("Number of Disk reads: "+ PCounter.get_rcounter());
        System.out.println("Number of Disk writes: "+ PCounter.get_wcounter());
        PCounter.initialize();
        // pass this sort object to the sortfirstsky

        SortFirstSky sortFirstSky = null;
        try {
            sortFirstSky = new SortFirstSky(attrType_for_sort,
                    (short) COLS,
                    null,
                    sort,
                    (short)_t_size,
                    hFile,
                    _pref_list,
                    _pref_list.length,
                    _n_pages);

            System.out.println("Skyline object: ");
            Tuple temp;
            int numSkyEle = 0;
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
            status = OK;
            // clean up
            try {
                sortFirstSky.close();
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }

        System.out.println("Number of Disk reads: "+ PCounter.get_rcounter());
        System.out.println("Number of Disk writes: "+ PCounter.get_wcounter());
        PCounter.initialize();

    }

	private void runBtreeSky() throws Exception {
		System.out.println("Will run b tree sky with params: ");
		System.out.println("N pages: " + _n_pages);
		System.out.println("Pref list: " + Arrays.toString(_pref_list));
		System.out.println("Pref list length: " + _pref_list.length);
		
		if (individualBTreeIndexesCreated == false) {
			BtreeGeneratorUtil.generateAllBtreesForHeapfile(hFile, f, attrType, attrSize);
			individualBTreeIndexesCreated = true;
		}
		
		//limiting buffer pages in BufMgr
		SystemDefs.JavabaseBM.limit_memory_usage(true, this._n_pages);
		
		int len_in1 = 4;
		int amt_of_mem = 100; // TODO what should this be?
		Iterator am1 = null;
		String relationName = hFile;
		
		//get only the btree indexes specified by the the pref_list array
		IndexFile[] index_file_list = BtreeGeneratorUtil.getBtreeSubset(_pref_list);
		PCounter.initialize();
		BTreeSky btreesky = new BTreeSky(attrType, len_in1, attrSize, amt_of_mem, am1, relationName, _pref_list,
				_pref_list.length, index_file_list, _n_pages);
		btreesky.debug = false;
		int numSkyEle = 0;
		Tuple skyEle = btreesky.get_next(); // first sky element
		System.out.print("First Sky element is: ");
		skyEle.print(attrType);
		numSkyEle++;
		while (skyEle != null) {
			skyEle = btreesky.get_next(); // subsequent sky elements
			if (skyEle == null) {
				System.out.println("No more sky elements");
				break;
			}
			numSkyEle++;
			System.out.print("Sky element is: ");
			skyEle.print(attrType);
		}
		System.out.println("Skyline Length: "+numSkyEle);
		btreesky.close();
		System.out.println("End of runBtreeSky");
		 System.out.println("Number of Disk reads: "+ PCounter.get_rcounter());
         System.out.println("Number of Disk writes: "+ PCounter.get_wcounter());
         PCounter.initialize();
	}

	private void runBtreeSortSky() throws Exception {
        System.out.println("Will run btree sort sky with params: ");
        System.out.println("N pages: "+_n_pages);

        int [] pref_list = new int[numberOfDimensions];
        
        for(int i = 0; i < _pref_list.length; i++) {
        	pref_list[ _pref_list[i] - 1 ] = 1;
        }
        
        for(int i = 0; i < numberOfDimensions; i++) {
        	if(pref_list[i] != 1) pref_list[i] = 0;
        }
        
        System.out.println("Pref list: "+Arrays.toString(pref_list));
        System.out.println("Pref list length: "+numberOfDimensions);

        //limiting buffer pages in BufMgr
        System.out.println("No of buffers "+SystemDefs.JavabaseBM.getNumBuffers());
        System.out.println("No of unpinned buffers "+SystemDefs.JavabaseBM.getNumUnpinnedBuffers());

        SystemDefs.JavabaseBM.limit_memory_usage(true, this._n_pages);


        GenerateIndexFiles obj = new GenerateIndexFiles();
        IndexFile indexFile = obj.createCombinedBTreeIndex(dataFile,pref_list, pref_list.length);
        System.out.println("Index created! ");
        Tuple t = new Tuple();
        short [] Ssizes = null;

        AttrType [] attrType = new AttrType[pref_list.length];
        for(int i=0;i<pref_list.length;i++){
            attrType[i] = new AttrType (AttrType.attrReal);
        }

        t.setHdr((short)pref_list.length, attrType, Ssizes);
        int size = t.size();

        t = new Tuple(size);
        t.setHdr((short)pref_list.length, attrType, Ssizes);

        PCounter.initialize();
        int numSkyEle = 0;
        BTreeSortedSky btree = new BTreeSortedSky(attrType, pref_list.length, Ssizes, 0, null, "heap_AAA", _pref_list, _pref_list.length, indexFile, _n_pages );
        btree.computeSkylines();

        System.out.println("Printing the Btree sorted Skyline");
        Tuple temp;
        temp = btree.get_next();
        while (temp!=null) {
        	temp.print(attrType);
        	numSkyEle++;
        	temp = btree.get_next();
        }
        System.out.println("Skyline Length: "+numSkyEle);
        System.out.println("Number of Disk reads: "+ PCounter.get_rcounter());
        System.out.println("Number of Disk writes: "+ PCounter.get_wcounter());
        PCounter.initialize();
        btree.close();
        
    }
}


/**
 * To get the integer off the command line
 */
class GetStuffPhase3 {
	GetStuffPhase3() {}

    public static int getChoice () {
        BufferedReader in = new BufferedReader (new InputStreamReader(System.in));
        int choice = -1;

        try {
            choice = Integer.parseInt(in.readLine());
        }
        catch (NumberFormatException e) {
            return -1;
        }
        catch (IOException e) {
            return -1;
        }

        return choice;
    }

    public static void getReturn () {

        BufferedReader in = new BufferedReader (new InputStreamReader(System.in));

        try {
            String ret = in.readLine();
        }
        catch (IOException e) {}
    }
}

public class AppDriverPhase3 implements  GlobalConst{

    public static void main(String [] argvs) {

        try{
        	DriverPhase3 driver = new DriverPhase3();
            driver.startQueryInterface();
        }
        catch (Exception e) {
            System.err.println ("Error encountered during running main driver:\n");
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }finally {

        }
    }

}