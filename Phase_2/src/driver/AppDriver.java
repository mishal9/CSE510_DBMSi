package driver;


import java.io.*;
import java.util.*;

import btree.BTreeSky;
import btree.IndexFile;

import bufmgr.PageNotReadException;
import diskmgr.PCounter;
import heap.*;
import global.*;

import index.IndexException;
import iterator.*;
import iterator.Iterator;
import tests.TestDriver;


//watching point: RID rid, some of them may not have to be newed.

class Driver extends TestDriver implements GlobalConst
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

    public Driver(){
        super("main");
    }

    public boolean runTests () {
        System.out.println ("\n" + "Running " + testName() + " tests...." + "\n");
        dbpath = "MINIBASE.minibase-db";
		logpath = "MINIBASE.minibase-log";
        // Each page can handle at most 25 tuples on original data => 7308 / 25 = 292
        SystemDefs sysdef = new SystemDefs(dbpath,8000, 3000,"Clock");

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
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        }
        catch (IOException e) {
            System.err.println ("IO error: "+e);
        }

        remove_logcmd = remove_cmd + newlogpath;
        remove_dbcmd = remove_cmd + newdbpath;

        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        }
        catch (IOException e) {
            System.err.println ("IO error: "+e);
        }

        //Run the tests. Return type different from C++
        boolean _pass = runAllTests();

        //Clean up again
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        }
        catch (IOException e) {
            System.err.println ("IO error: "+e);
        }

        System.out.print ("\n" + "..." + testName() + " tests ");
        System.out.print (_pass==OK ? "completely successfully" : "failed");
        System.out.print (".\n\n");

        return _pass;
    }

    private void menu() {
        System.out.println("-------------------------- MENU ------------------");
        System.out.println("[102]   Read input data 2");
        System.out.println("[103]   Read input data 3");
        System.out.println("[104]   Set pref = [1]");
        System.out.println("[105]   Set pref = [1,3]");
        System.out.println("[106]   Set pref = [1,3,5]");
        System.out.println("[107]   Set pref = [1,2,3,4,5]");
        System.out.println("[108]   Set n_page = 5");
        System.out.println("[109]   Set n_page = 10");
        System.out.println("[110]   Set n_page = <your_wish>");
        System.out.println("[1]  Run Nested Loop skyline on data with parameters ");
        System.out.println("[2]  Run Block Nested Loop on data with parameters ");
        System.out.println("[3]  Run Sort First Sky on data with parameters ");
        System.out.println("[4]  Run Btree Sky on data with parameters ");
        System.out.println("[5]  Run Btree Sort Sky on data with parameters ");
        System.out.println("\n[0]  Quit!");
        System.out.print("Hi, make your choice :");
    }
    
    private void readDataIntoHeap(String fileName) throws IOException, InvalidTupleSizeException, InvalidTypeException, InvalidSlotNumberException, HFDiskMgrException, HFBufMgrException, HFException {

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

            File file = new File("../../data/"+fileName+".txt");
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
            System.out.println("Size: "+size);

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
            System.out.println("record count "+f.getRecCnt());
            sc.close();
        }
    }

    protected String testName () {
        return "Main Driver";
    }

	protected boolean runAllTests (){
        int choice=100;
               
        while(choice!=0) {
            menu();

            try{
                choice= GetStuff.getChoice();

                switch(choice) {

                    case 102:
                        readDataIntoHeap("data2");
                        BtreeGeneratorUtil.generateAllBtreesForHeapfile(hFile, f, attrType, attrSize);
                        individualBTreeIndexesCreated = true;
                        break;

                    case 103:
                        readDataIntoHeap("data3");
                        BtreeGeneratorUtil.generateAllBtreesForHeapfile(hFile, f, attrType, attrSize);
                        individualBTreeIndexesCreated = true;
                        break;

                    case 104:
                        _pref_list = new int[]{1};
                        break;

                    case 105:
                        _pref_list = new int[]{1,3};
                        break;

                    case 106:
                        _pref_list = new int[]{1,3,5};
                        break;

                    case 107:
                        _pref_list = new int[]{1,2,3,4,5};
                        break;

                    case 108:
                        _n_pages = 5;
                        break;

                    case 109:
                        _n_pages = 10;
                        break;

                    case 110:
                        System.out.println("Enter n_pages of your choice: ");
                        _n_pages = GetStuff.getChoice();
                        if(_n_pages<0)
                            break;
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
                        break;
                }


            }
            catch(Exception e) {
                e.printStackTrace();
                System.out.println("       !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                System.out.println("       !!         Something is wrong                    !!");
                System.out.println("       !!     Is your DB full? then exit. rerun it!     !!");
                System.out.println("       !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");

            }
        }

        return true;
    }

    private void runNestedLoopSky(){
        System.out.println("Will run nested loop skyline with params: ");
        System.out.println("N pages: "+_n_pages);
        System.out.println("Pref list: "+Arrays.toString(_pref_list));
        System.out.println("Pref list length: "+_pref_list.length);

        NestedLoopsSky nestedLoopsSky = null;

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
                temp.print(attrType);
                temp = nestedLoopsSky.get_next();
            }
        } catch (FileScanException e) {
            e.printStackTrace();
        } catch (TupleUtilsException e) {
            e.printStackTrace();
        } catch (InvalidRelation invalidRelation) {
            invalidRelation.printStackTrace();
        } catch (WrongPermat wrongPermat) {
            wrongPermat.printStackTrace();
        } catch (InvalidTypeException e) {
            e.printStackTrace();
        } catch (JoinsException e) {
            e.printStackTrace();
        } catch (PageNotReadException e) {
            e.printStackTrace();
        } catch (FieldNumberOutOfBoundException e) {
            e.printStackTrace();
        } catch (PredEvalException e) {
            e.printStackTrace();
        } catch (UnknowAttrType unknowAttrType) {
            unknowAttrType.printStackTrace();
        } catch (InvalidTupleSizeException e) {
            e.printStackTrace();
        } catch (IOException e) {
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
    }

    private void blockNestedSky(){
        System.out.println("Will run block nested loop skyline with params: ");
        System.out.println("N pages: "+_n_pages);
        System.out.println("Pref list: "+Arrays.toString(_pref_list));
        System.out.println("Pref list length: "+_pref_list.length);

        BlockNestedLoopsSky blockNestedLoopsSky = null;

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
                    temp = blockNestedLoopsSky.get_next();
                }
            } catch (IndexException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (SortException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (LowMemException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (UnknownKeyTypeException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

        } catch (FileScanException e) {
            e.printStackTrace();
        } catch (TupleUtilsException e) {
            e.printStackTrace();
        } catch (InvalidRelation invalidRelation) {
            invalidRelation.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            blockNestedLoopsSky.close();
        }
    }

    private void runSortFirstSky() {

        System.out.println("Will run sort first sky with params: ");
        System.out.println("N pages: "+_n_pages);
        System.out.println("Pref list: "+Arrays.toString(_pref_list));
        System.out.println("Pref list length: "+_pref_list.length);

        FileScan fscan = null;

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

        } catch (IOException e) {
            e.printStackTrace();
        } catch (IndexException e) {
            e.printStackTrace();
        } catch (PredEvalException e) {
            e.printStackTrace();
        } catch (UnknowAttrType unknowAttrType) {
            unknowAttrType.printStackTrace();
        } catch (JoinsException e) {
            e.printStackTrace();
        } catch (InvalidTupleSizeException e) {
            e.printStackTrace();
        } catch (PageNotReadException e) {
            e.printStackTrace();
        } catch (UnknownKeyTypeException e) {
            e.printStackTrace();
        } catch (LowMemException e) {
            e.printStackTrace();
        } catch (InvalidTypeException e) {
            e.printStackTrace();
        } catch (SortException e) {
            e.printStackTrace();
        } catch (TupleUtilsException e) {
            e.printStackTrace();
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
		
		int len_in1 = 4;
		int amt_of_mem = 100; // TODO what should this be?
		Iterator am1 = null;
		String relationName = hFile;
		//get only the btree indexes specified by the the pref_list array
		IndexFile[] index_file_list = BtreeGeneratorUtil.getBtreeSubset(_pref_list);

		BTreeSky btreesky = new BTreeSky(attrType, len_in1, attrSize, amt_of_mem, am1, relationName, _pref_list,
				_pref_list.length, index_file_list, _n_pages);
		btreesky.debug =false;
		Tuple skyEle = btreesky.get_next(); // first sky element
		System.out.print("First Sky element is: ");
		skyEle.print(attrType);

		while (skyEle != null) { // TODO check this after integration with BNL skyline
			skyEle = btreesky.get_next(); // subsequent sky elements
			if (skyEle == null) {
				System.out.println("No more sky elements");
				break;
			}
			System.out.print("Sky element is: ");
			skyEle.print(attrType);
		}

		btreesky.close();
		System.out.println("End of runBtreeSky");
	}

	private void runBtreeSortSky() {
        System.out.println("Will run btree sort sky with params: ");
        System.out.println("N pages: "+_n_pages);
        System.out.println("Pref list: "+Arrays.toString(_pref_list));
        System.out.println("Pref list length: "+_pref_list.length);
    }
}


/**
 * To get the integer off the command line
 */
class GetStuff {
    GetStuff() {}

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

public class AppDriver implements  GlobalConst{

    public static void main(String [] argvs) {

        try{
            Driver driver = new Driver();
            driver.runTests();
        }
        catch (Exception e) {
            System.err.println ("Error encountered during running main driver:\n");
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }finally {

        }
    }

}
