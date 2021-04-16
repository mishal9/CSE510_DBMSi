package tests;

import java.io.*;

import bufmgr.*;
import diskmgr.PCounter;
import global.*;
import heap.*;
import index.IndexException;
import iterator.*;

import java.util.Arrays;
import java.util.Scanner;

class GroupByHashDriver extends TestDriver
        implements GlobalConst {

    // groupBy_attr: 1
    // agg_list: 2
    // agg_types: MIN | MAX | AVG | SKYLINE

    private static float[][] data1 = {
            {1, 6, 8},
            {1, 4, 5},
            {1, 4, 3},
            {1, 2, 3},
            {2, 7, 8},
            {2, 3, 4},
            {3, 5, 10},
            {3, 100, 20},
            {4, 5, 8},
            {4, 8, 9}
    };

    private static float[][] data2 = {              // AVG
            {1, 10/3, 13/3},
            {2, 10/2, 12/2},
            {5, 5, 8},
            {9, 6, 8}
    };

    private static float[][] data3 = {              // SKYLINE
            {1, 4, 5},
            {2, 7, 8},
            {5, 5, 8},
            {9, 6, 8}
    };

    private static float[][] data4 = {              // MIN
            {1, 2, 3},
            {2, 3, 4},
            {5, 5, 8},
            {9, 6, 8}
    };

    private static float[][] data5 = {              // MAX
            {1, 4, 5},
            {2, 7, 8},
            {5, 5, 8},
            {9, 6, 8}
    };

    private static int COLS;
    private static final String hFile = "hFile.in";
    private static AttrType[] attrType;
    private short[] attrSize;
    private static Heapfile  f = null;
    private static RID   rid;

    private static int   NUM_RECORDS = data1.length;
    private static short REC_LEN1 = 32;
    private static short REC_LEN2 = 32;
    private static short REC_LEN3 = 32;

    private static FldSpec[] projlist;
    private static RelSpec rel = new RelSpec(RelSpec.outer);
    private static int _t_size;
    boolean status = false;

    TupleOrder[] order = new TupleOrder[2];
    AggType[] aggType = new AggType[4];

    public GroupByHashDriver() {
        super("groupByHashTest");
        order[0] = new TupleOrder(TupleOrder.Ascending);
        order[1] = new TupleOrder(TupleOrder.Descending);

        aggType[0] = new AggType(AggType.MIN);
        aggType[1] = new AggType(AggType.MAX);
        aggType[2] = new AggType(AggType.AVG);
        aggType[3] = new AggType(AggType.SKYLINE);
    }

    private void readDataIntoHeap(String fileName)
            throws IOException, InvalidTupleSizeException, InvalidTypeException, InvalidSlotNumberException, HFDiskMgrException, HFBufMgrException, HFException, HashOperationException, PageUnpinnedException, PagePinnedException, PageNotFoundException, BufMgrException, SpaceNotAvailableException {

        // Create the heap file object
        f = new Heapfile(hFile);
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
            projlist[i] = new FldSpec(rel, i+1);
        }

        Tuple t = new Tuple();
        t.setHdr((short) COLS,attrType, attrSize);
        _t_size = t.size();

        t = new Tuple(_t_size);
        t.setHdr((short) COLS, attrType, attrSize);

        while (sc.hasNextLine()) {
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
                    e.printStackTrace();
                }
            }
            rid = f.insertRecord(t.returnTupleByteArray());
        }
        System.out.println("Number of records in Database: "+f.getRecCnt());
        sc.close();
        SystemDefs.JavabaseBM.flushAllPages();
    }

    public boolean runTests () throws HFDiskMgrException, HFException, HFBufMgrException, IOException {

        System.out.println ("\n" + "Running " + testName() + " tests...." + "\n");

        SystemDefs sysdef = new SystemDefs( dbpath, 3000, 100, "Clock" );

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
            System.err.println (""+e);
        }

        remove_logcmd = remove_cmd + newlogpath;
        remove_dbcmd = remove_cmd + newdbpath;

        //This step seems redundant for me.  But it's in the original
        //C++ code.  So I am keeping it as of now, just in case I
        //I missed something
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        }
        catch (IOException e) {
            System.err.println (""+e);
        }

        //Run the tests. Return type different from C++
        boolean _pass = runAllTests();

        //Clean up again
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        }
        catch (IOException e) {
            System.err.println (""+e);
        }

        System.out.println ("\n" + "..." + testName() + " tests ");
        System.out.println (_pass==OK ? "completely successfully" : "failed");
        System.out.println (".\n\n");

        return _pass;
    }

    protected boolean test1()
    {
        System.out.println("------------------------ TEST 1 : MIN --------------------------");

        boolean status = OK;

        AttrType[] attrType = new AttrType[3];
        attrType[0] = new AttrType(AttrType.attrInteger);
        attrType[1] = new AttrType(AttrType.attrInteger);
        attrType[2] = new AttrType(AttrType.attrInteger);

        short[] attrSize = new short[3];
        attrSize[0] = REC_LEN1;
        attrSize[1] = REC_LEN2;
        attrSize[2] = REC_LEN3;

        // create a tuple of appropriate size
        Tuple t = new Tuple();
        try {
            t.setHdr((short) 3, attrType, attrSize);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        int size = t.size();

        // Create unsorted data file "test1.in"
        RID             rid;
        Heapfile        f = null;
        try {
            f = new Heapfile("test1GroupByHash.in");
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        t = new Tuple(size);
        try {
            t.setHdr((short) 3, attrType, attrSize);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        for (int i=0; i<NUM_RECORDS; i++) {
            try {
                for(int j=0; j<3; j++)
                    t.setFloFld(j+1, data1[i][j]);
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            try {
                rid = f.insertRecord(t.returnTupleByteArray());
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }

        FldSpec groupByAttr = new FldSpec(new RelSpec(RelSpec.outer), 1);

        FldSpec[] aggList = new FldSpec[1];
        rel = new RelSpec(RelSpec.outer);
        aggList[0] = new FldSpec(rel, 2);

        // create an iterator by open a file scan
        FldSpec[] projlist = new FldSpec[3];
        rel = new RelSpec(RelSpec.outer);
        projlist[0] = new FldSpec(rel, 1);
        projlist[1] = new FldSpec(rel, 2);
        projlist[2] = new FldSpec(rel, 3);

        FileScan fscan = null;

        try {
            fscan = new FileScan("test1GroupByHash.in", attrType, attrSize, (short) 3, 3, projlist, null);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        // Sort operator working verified till here

        GroupByWithHash grpHash = null;
        PCounter.initialize();
        try {
            grpHash = new GroupByWithHash(attrType,
                    3,
                    attrSize,
                    fscan,
                    groupByAttr,
                    aggList,
                    aggType[0],
                    projlist,
                    3,
                    20);

            /*
            System.out.println("Printing the Group By Sort Results");
            Tuple temp;
            try {
                temp = grpSort.get_next();
                while (temp!=null) {
                    temp.print(attrType);
                    temp = grpSort.get_next();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            */

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                grpHash.close();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (SortException e) {
                e.printStackTrace();
            }
        }

        System.out.println("Number of Disk reads: "+ PCounter.get_rcounter());
        System.out.println("Number of Disk writes: "+ PCounter.get_wcounter());
        PCounter.initialize();

        System.err.println("------------------- TEST 1 completed ---------------------\n");

        return status;
    }


    protected boolean test2()
    {
        System.out.println("------------------------ TEST 2 : MAX --------------------------");

        boolean status = OK;

        AttrType[] attrType = new AttrType[3];
        attrType[0] = new AttrType(AttrType.attrInteger);
        attrType[1] = new AttrType(AttrType.attrInteger);
        attrType[2] = new AttrType(AttrType.attrInteger);

        short[] attrSize = new short[3];
        attrSize[0] = REC_LEN1;
        attrSize[1] = REC_LEN2;
        attrSize[2] = REC_LEN3;

        // create a tuple of appropriate size
        Tuple t = new Tuple();
        try {
            t.setHdr((short) 3, attrType, attrSize);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        int size = t.size();

        // Create unsorted data file "test1.in"
        RID             rid;
        Heapfile        f = null;
        try {
            f = new Heapfile("test2GroupByHash.in");
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        t = new Tuple(size);
        try {
            t.setHdr((short) 3, attrType, attrSize);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        for (int i=0; i<NUM_RECORDS; i++) {
            try {
                for(int j=0; j<3; j++)
                    t.setFloFld(j+1, data1[i][j]);
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            try {
                rid = f.insertRecord(t.returnTupleByteArray());
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }

        FldSpec groupByAttr = new FldSpec(new RelSpec(RelSpec.outer), 1);

        FldSpec[] aggList = new FldSpec[2];
        rel = new RelSpec(RelSpec.outer);
        aggList[0] = new FldSpec(rel, 2);
        aggList[1] = new FldSpec(rel, 3);

        // create an iterator by open a file scan
        FldSpec[] projlist = new FldSpec[3];
        rel = new RelSpec(RelSpec.outer);
        projlist[0] = new FldSpec(rel, 1);
        projlist[1] = new FldSpec(rel, 2);
        projlist[2] = new FldSpec(rel, 3);


        FileScan fscan = null;

        try {
            fscan = new FileScan("test2GroupByHash.in", attrType, attrSize, (short) 3, 3, projlist, null);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        // Sort operator working verified till here

        GroupByWithHash grpHash = null;
        PCounter.initialize();
        try {
            grpHash = new GroupByWithHash(attrType,
                    3,
                    attrSize,
                    fscan,
                    groupByAttr,
                    aggList,
                    aggType[1],
                    projlist,
                    3,
                    20);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                grpHash.close();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (SortException e) {
                e.printStackTrace();
            }
        }

        System.out.println("Number of Disk reads: "+ PCounter.get_rcounter());
        System.out.println("Number of Disk writes: "+ PCounter.get_wcounter());
        PCounter.initialize();

        System.err.println("------------------- TEST 2 completed ---------------------\n");

        return status;
    }

    protected  boolean test3(){
        System.out.println("------------------------ TEST 3 : AVG --------------------------");

        boolean status = OK;

        AttrType[] attrType = new AttrType[3];
        attrType[0] = new AttrType(AttrType.attrInteger);
        attrType[1] = new AttrType(AttrType.attrInteger);
        attrType[2] = new AttrType(AttrType.attrInteger);

        short[] attrSize = new short[3];
        attrSize[0] = REC_LEN1;
        attrSize[1] = REC_LEN2;
        attrSize[2] = REC_LEN3;

        // create a tuple of appropriate size
        Tuple t = new Tuple();
        try {
            t.setHdr((short) 3, attrType, attrSize);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        int size = t.size();

        // Create unsorted data file "test1.in"
        RID             rid;
        Heapfile        f = null;
        try {
            f = new Heapfile("test3GroupByHash.in");
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        t = new Tuple(size);
        try {
            t.setHdr((short) 3, attrType, attrSize);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        for (int i=0; i<NUM_RECORDS; i++) {
            try {
                for(int j=0; j<3; j++)
                    t.setFloFld(j+1, data1[i][j]);
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            try {
                rid = f.insertRecord(t.returnTupleByteArray());
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }

        FldSpec groupByAttr = new FldSpec(new RelSpec(RelSpec.outer), 1);

        FldSpec[] aggList = new FldSpec[2];
        rel = new RelSpec(RelSpec.outer);
        aggList[0] = new FldSpec(rel, 2);
        aggList[1] = new FldSpec(rel, 3);

        // create an iterator by open a file scan
        FldSpec[] projlist = new FldSpec[3];
        rel = new RelSpec(RelSpec.outer);
        projlist[0] = new FldSpec(rel, 1);
        projlist[1] = new FldSpec(rel, 2);
        projlist[2] = new FldSpec(rel, 3);


        FileScan fscan = null;

        try {
            fscan = new FileScan("test3GroupByHash.in", attrType, attrSize, (short) 3, 3, projlist, null);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        // Sort operator working verified till here

        GroupByWithHash grpHash = null;
        PCounter.initialize();
        try {
            grpHash = new GroupByWithHash(attrType,
                    3,
                    attrSize,
                    fscan,
                    groupByAttr,
                    aggList,
                    aggType[2],
                    projlist,
                    3,
                    20);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                grpHash.close();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (SortException e) {
                e.printStackTrace();
            }
        }

        System.out.println("Number of Disk reads: "+ PCounter.get_rcounter());
        System.out.println("Number of Disk writes: "+ PCounter.get_wcounter());
        PCounter.initialize();

        System.err.println("------------------- TEST 3 completed ---------------------\n");

        return status;
    }

    protected boolean test4(){
        System.out.println("------------------------ TEST 4 : SKYLINE --------------------------");

        boolean status = OK;

        AttrType[] attrType = new AttrType[3];
        attrType[0] = new AttrType(AttrType.attrInteger);
        attrType[1] = new AttrType(AttrType.attrInteger);
        attrType[2] = new AttrType(AttrType.attrInteger);

        short[] attrSize = new short[3];
        attrSize[0] = REC_LEN1;
        attrSize[1] = REC_LEN2;
        attrSize[2] = REC_LEN3;

        // create a tuple of appropriate size
        Tuple t = new Tuple();
        try {
            t.setHdr((short) 3, attrType, attrSize);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        int size = t.size();

        // Create unsorted data file "test1.in"
        RID             rid;
        Heapfile        f = null;
        try {
            f = new Heapfile("test4GroupByHash.in");
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        t = new Tuple(size);
        try {
            t.setHdr((short) 3, attrType, attrSize);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        for (int i=0; i<NUM_RECORDS; i++) {
            try {
                for(int j=0; j<3; j++)
                    t.setFloFld(j+1, data1[i][j]);
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            try {
                rid = f.insertRecord(t.returnTupleByteArray());
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }

        FldSpec groupByAttr = new FldSpec(new RelSpec(RelSpec.outer), 1);

        FldSpec[] aggList = new FldSpec[2];
        rel = new RelSpec(RelSpec.outer);
        aggList[0] = new FldSpec(rel, 2);
        aggList[1] = new FldSpec(rel, 3);

        // create an iterator by open a file scan
        FldSpec[] projlist = new FldSpec[3];
        rel = new RelSpec(RelSpec.outer);
        projlist[0] = new FldSpec(rel, 1);
        projlist[1] = new FldSpec(rel, 2);
        projlist[2] = new FldSpec(rel, 3);


        FileScan fscan = null;

        try {
            fscan = new FileScan("test4GroupByHash.in", attrType, attrSize, (short) 3, 3, projlist, null);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        // Sort operator working verified till here

        GroupByWithHash grpHash = null;
        PCounter.initialize();
        try {
            try {
                grpHash = new GroupByWithHash(attrType,
                        3,
                        attrSize,
                        fscan,
                        groupByAttr,
                        aggList,
                        aggType[3],
                        projlist,
                        3,
                        20);
            } catch (Exception e) {
                e.printStackTrace();
            }

        } finally {
            try {
                grpHash.close();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (SortException e) {
                e.printStackTrace();
            }
        }

        System.out.println("Number of Disk reads: "+ PCounter.get_rcounter());
        System.out.println("Number of Disk writes: "+ PCounter.get_wcounter());
        PCounter.initialize();

        System.err.println("------------------- TEST 4 completed ---------------------\n");

        return status;
    }

    protected  boolean test5()
    {
        /*
        System.out.println("------------------------ TEST 5 --------------------------");
        try{
            readDataIntoHeap("../../data/data3.txt");
        }catch (Exception e){
            e.printStackTrace();
        }
        Tuple t = null;
        int num_attributes = COLS;
        int actual_pref_list[] = {1, 2};

        FldSpec[] projlist = new FldSpec[num_attributes+1];
        RelSpec rel = new RelSpec(RelSpec.outer);
        for(int i=0;i<num_attributes;i++) projlist[i] = new FldSpec(rel, i+1);
        projlist[num_attributes] = new FldSpec(rel, 1);;

        AttrType[] attrType_for_proj = new AttrType[num_attributes];
        for(int i=0;i<num_attributes;i++) attrType_for_proj[i] = new AttrType(AttrType.attrReal);

        OurFileScan fscan = null;

        try {
            fscan = new OurFileScan(hFile, attrType_for_proj, null, (short) num_attributes, num_attributes, projlist, null, actual_pref_list);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        // Sort "test1sortPref.in"

        AttrType[] attrType_for_sort = new AttrType[num_attributes+1];
        for(int i=0;i<num_attributes;i++) {
            attrType_for_sort[i] = new AttrType(AttrType.attrReal);
        }
        attrType_for_sort[num_attributes] = new AttrType(AttrType.attrReal);

        Sort sort = null;
        try {
            sort = new Sort(attrType_for_sort, (short) (num_attributes+1), attrSize, fscan, (num_attributes+1), order[1], REC_LEN1, SORTPGNUM);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        int count = 0;
        float[] outval = new float[5];

        try {
            t = sort.get_next();
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        boolean flag = true;
        while (t != null) {
            try {
                outval[0] = t.getFloFld(1);
                outval[1] = t.getFloFld(2);
                outval[2] = t.getFloFld(3);
                outval[3] = t.getFloFld(4);
                outval[4] = 0;//t.getFloFld(3);

                System.out.println("Got row: "+outval[0]+" "+outval[1]+" "+outval[2]+" "+outval[3]+" "+outval[4]+" "+" | "+(outval[0]+outval[1]));
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            count++;

            try {
                t = sort.get_next();
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }
        }

         */
        return true;
    }

    protected String testName()
    {
        return "GroupByHash";
    }
}

public class GroupByHashTest
{
    public static void main(String argv[]) throws IOException, HFException, HFBufMgrException, HFDiskMgrException {
        boolean grpstatus;

        GroupByHashDriver hash = new GroupByHashDriver();

        grpstatus = hash.runTests();
        if (grpstatus != true) {
            System.out.println("Error ocurred during sorting tests");
        }
        else {
            System.out.println("Sorting tests completed successfully");
        }
    }
}

