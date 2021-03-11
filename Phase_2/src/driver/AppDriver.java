package driver;


import java.io.*;
import java.util.*;

import btree.BTreeSky;
import btree.IndexFile;

import diskmgr.PCounter;
import heap.*;
import global.*;
import iterator.FileScan;
import iterator.FldSpec;
import iterator.Iterator;
import iterator.RelSpec;
import skylines.SortFirstSky;
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
    private static final String hFile = "hFile100.in";
    private static AttrType[] attrType;
    private short[] attrSize;
    // create an iterator by open a file scan
    private static FldSpec[] projlist;
    private static RelSpec rel = new RelSpec(RelSpec.outer);

    private static boolean individualBTreeIndexesCreated;

    public Driver(){
        super("main");
    }

    public boolean runTests () {
        System.out.println ("\n" + "Running " + testName() + " tests...." + "\n");
        dbpath = "MINIBASE.minibase-db";
		logpath = "MINIBASE.minibase-log";
        // Each page can handle at most 25 tuples on original data => 7308 / 25 = 292
        SystemDefs sysdef = new SystemDefs(dbpath,10000, 3000,"Clock");

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
                t.setHdr((short) attrType.length,attrType, attrSize);
            }
            catch (Exception e) {
                System.err.println("*** error in Tuple.setHdr() ***");
                status = FAIL;
                e.printStackTrace();
            }

            int size = t.size();
            System.out.println("Size: "+size);

            t = new Tuple(size);
            try {
                t.setHdr((short) attrType.length, attrType, attrSize);
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
                        System.out.println("Will run nested loop skyline with params: ");
                        System.out.println("N pages: "+_n_pages);
                        System.out.println("Pref list: "+Arrays.toString(_pref_list));
                        System.out.println("Pref list length: "+_pref_list.length);
                        break;

                    case 2:
                        // call block nested loop sky
                        System.out.println("Will run block nested loop skyline with params: ");
                        System.out.println("N pages: "+_n_pages);
                        System.out.println("Pref list: "+Arrays.toString(_pref_list));
                        System.out.println("Pref list length: "+_pref_list.length);
                        break;

                    case 3:
                        // call sort first sky
                        System.out.println("Will run sort first sky with params: ");
                        System.out.println("N pages: "+_n_pages);
                        System.out.println("Pref list: "+Arrays.toString(_pref_list));
                        System.out.println("Pref list length: "+_pref_list.length);


                        // create an iterator by open a file scan

                        FileScan fscan = null;

                        try {
                            fscan = new FileScan(hFile, attrType, attrSize, (short) COLS, COLS, projlist, null);
                        }
                        catch (Exception e) {
                            status = FAIL;
                            e.printStackTrace();
                        }

                        try {
                            SortFirstSky sortFirstSky = new SortFirstSky(attrType,
                                                            (short) COLS,
                                                            attrSize,
                                                            fscan,
                                                            hFile,
                                                            _pref_list,
                                                            _pref_list.length,
                                                            _n_pages);
                        } catch (IOException e) {
                            e.printStackTrace();
                        } catch (HFException e) {
                            e.printStackTrace();
                        } catch (HFBufMgrException e) {
                            e.printStackTrace();
                        } catch (HFDiskMgrException e) {
                            e.printStackTrace();
                        } finally {
                            status = OK;
                            // clean up
                            try {
                                fscan.close();
                            }
                            catch (Exception e) {
                                status = FAIL;
                                e.printStackTrace();
                            }
                        }

                        break;

                    case 4:
                        // call btree sky
                    	runBtreeSky();
                        break;

                    case 5:
                        // call btree sort sky
                        System.out.println("Will run btree sort sky with params: ");
                        System.out.println("N pages: "+_n_pages);
                        System.out.println("Pref list: "+Arrays.toString(_pref_list));
                        System.out.println("Pref list length: "+_pref_list.length);
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

        PCounter.initialize();

        try{
            Driver driver = new Driver();
            driver.runTests();

            System.out.println("Read statistics "+PCounter.rcounter);
            System.out.println("Write statistics "+PCounter.wcounter);
        }
        catch (Exception e) {
            System.err.println ("Error encountered during running main driver:\n");
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }finally {

        }
    }

}
