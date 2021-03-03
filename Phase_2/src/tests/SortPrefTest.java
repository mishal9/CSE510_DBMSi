package tests;

import java.io.*;
import global.*;
import bufmgr.*;
import diskmgr.*;
import heap.*;
import iterator.*;
import index.*;

import java.util.Arrays;
import java.util.Random;


class SORTPrefDriver extends TestDriver
        implements GlobalConst {

    private static float[][] data1 = {
            {0.825f,   0.823f,    0.453f,    0.122f,    0.356f}, // -> 1.648
            {0.855f,   0.316f,    0.782f,    0.478f,    0.758f}, // -> 1.171
            {0.011f,   0.268f,    0.348f,    0.646f,    0.161f}, // -> 0.279
            {0.896f,   0.572f,    0.281f,    0.592f,    0.166f}, // -> 1.468
            {0.852f,   0.194f,    0.613f,    0.846f,    0.846f}  // -> 1.046
    };
    // [0,1]
    private static float[][] data2 = {
            {0.011f,   0.268f,    0.348f,    0.646f,    0.161f}, // -> 0.279
            {0.852f,   0.194f,    0.613f,    0.846f,    0.846f}, // -> 1.046
            {0.855f,   0.316f,    0.782f,    0.478f,    0.758f}, // -> 1.171
            {0.896f,   0.572f,    0.281f,    0.592f,    0.166f}, // -> 1.468
            {0.825f,   0.823f,    0.453f,    0.122f,    0.356f}, // -> 1.648
    };

    private static int   NUM_RECORDS = data2.length;
    private static short REC_LEN1 = 160;
    private static short REC_LEN2 = 160;
    private static short REC_LEN3 = 160;
    private static short REC_LEN4 = 160;
    private static short REC_LEN5 = 160;
    private static int   SORTPGNUM = 12;

    TupleOrder[] order = new TupleOrder[2];

    public SORTPrefDriver() {
        super("sorttest");
        order[0] = new TupleOrder(TupleOrder.Ascending);
        order[1] = new TupleOrder(TupleOrder.Descending);
    }

    public boolean runTests ()  {

        System.out.println ("\n" + "Running " + testName() + " tests...." + "\n");

        SystemDefs sysdef = new SystemDefs( dbpath, 300, NUMBUF, "Clock" );

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
        System.out.println("------------------------ TEST 1 --------------------------");

        boolean status = OK;

        AttrType[] attrType = new AttrType[5];
        attrType[0] = new AttrType(AttrType.attrReal);
        attrType[1] = new AttrType(AttrType.attrReal);
        attrType[2] = new AttrType(AttrType.attrReal);
        attrType[3] = new AttrType(AttrType.attrReal);
        attrType[4] = new AttrType(AttrType.attrReal);

        short[] attrSize = new short[5];
        attrSize[0] = REC_LEN1;
        attrSize[1] = REC_LEN2;
        attrSize[2] = REC_LEN3;
        attrSize[3] = REC_LEN4;
        attrSize[4] = REC_LEN5;



        // create a tuple of appropriate size
        Tuple t = new Tuple();
        try {
            t.setHdr((short) 5, attrType, attrSize);
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
            f = new Heapfile("test1sortPref.in");
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        t = new Tuple(size);
        try {
            t.setHdr((short) 5, attrType, attrSize);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        for (int i=0; i<NUM_RECORDS; i++) {
            try {
                for(int j=0; j<5; j++)
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

        // create an iterator by open a file scan
        FldSpec[] projlist = new FldSpec[5];
        RelSpec rel = new RelSpec(RelSpec.outer);
        projlist[0] = new FldSpec(rel, 1);
        projlist[1] = new FldSpec(rel, 2);
        projlist[2] = new FldSpec(rel, 3);
        projlist[3] = new FldSpec(rel, 4);
        projlist[4] = new FldSpec(rel, 5);

        FileScan fscan = null;

        try {
            fscan = new FileScan("test1sortPref.in", attrType, attrSize, (short) 5, 5, projlist, null);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        // Sort "test1sortPref.in"
        Sort sort = null;
        try {
            sort = new Sort(attrType, (short) 5, attrSize, fscan, order[0], new int[]{1,2}, 2, SORTPGNUM);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }


        int count = 0;
        t = null;
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
            if (count >= NUM_RECORDS) {
                System.err.println("Test1 -- OOPS! too many records");
                status = FAIL;
                flag = false;
                break;
            }

            try {
                outval[0] = t.getFloFld(1);
                outval[1] = t.getFloFld(2);
                outval[2] = t.getFloFld(3);
                outval[3] = t.getFloFld(4);
                outval[4] = t.getFloFld(5);

                System.out.println("Got row: ");
                System.out.println(outval[0]+" "+outval[1]+" "+outval[2]+" "+outval[3]+" "+outval[4]+" ");
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            if (!Arrays.equals(outval, data2[count])) {
                System.err.println("outval = " + outval[0] + "\tdata2[count] = " + data2[count][0]);

                System.err.println("Test1 -- OOPS! test1.out not sorted");
                status = FAIL;
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
        if (count < NUM_RECORDS) {
            System.err.println("Test1 -- OOPS! too few records");
            status = FAIL;
        }
        else if (flag && status) {
            System.err.println("Test1 -- Sorting OK");
        }

        // clean up
        try {
            sort.close();
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        System.err.println("------------------- TEST 1 completed ---------------------\n");

        return status;
    }


    protected boolean test2()
    {
        System.out.println("------------------------ TEST 2 --------------------------");

        boolean status = OK;
        /*
        AttrType[] attrType = new AttrType[1];
        attrType[0] = new AttrType(AttrType.attrString);
        short[] attrSize = new short[1];
        attrSize[0] = REC_LEN1;
        TupleOrder[] order = new TupleOrder[2];
        order[0] = new TupleOrder(TupleOrder.Ascending);
        order[1] = new TupleOrder(TupleOrder.Descending);

        // create a tuple of appropriate size
        Tuple t = new Tuple();
        try {
            t.setHdr((short) 1, attrType, attrSize);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }
        int size = t.size();

        // Create unsorted data file "test2.in"
        RID             rid;
        Heapfile        f = null;
        try {
            f = new Heapfile("test2.in");
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        t = new Tuple(size);
        try {
            t.setHdr((short) 1, attrType, attrSize);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        for (int i=0; i<NUM_RECORDS; i++) {
            try {
                t.setStrFld(1, data1[i]);
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

        // create an iterator by open a file scan
        FldSpec[] projlist = new FldSpec[1];
        RelSpec rel = new RelSpec(RelSpec.outer);
        projlist[0] = new FldSpec(rel, 1);

        FileScan fscan = null;

        try {
            fscan = new FileScan("test2.in", attrType, attrSize, (short) 1, 1, projlist, null);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        // Sort "test2.in"
        Sort sort = null;
        try {
            sort = new Sort(attrType, (short) 1, attrSize, fscan, 1, order[1], REC_LEN1, SORTPGNUM);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }


        int count = 0;
        t = null;
        String outval = null;

        try {
            t = sort.get_next();
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        boolean flag = true;

        while (t != null) {
            if (count >= NUM_RECORDS) {
                System.err.println("Test2 -- OOPS! too many records");
                status = FAIL;
                flag = false;
                break;
            }

            try {
                outval = t.getStrFld(1);
            }
            catch (Exception e) {
                status = FAIL;
                e.printStackTrace();
            }

            if (outval.compareTo(data2[NUM_RECORDS - count - 1]) != 0) {
                System.err.println("Test2 -- OOPS! test2.out not sorted");
                status = FAIL;
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
        if (count < NUM_RECORDS) {
            System.err.println("Test2 -- OOPS! too few records");
            status = FAIL;
        }
        else if (flag && status) {
            System.err.println("Test2 -- Sorting OK");
        }

        // clean up
        try {
            sort.close();
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        System.err.println("------------------- TEST 2 completed ---------------------\n");
        */
        return status;
    }


    protected String testName()
    {
        return "SortPref";
    }
}

public class SortPrefTest
{
    public static void main(String argv[])
    {
        boolean sortstatus;

        SORTPrefDriver sortt = new SORTPrefDriver();

        sortstatus = sortt.runTests();
        if (sortstatus != true) {
            System.out.println("Error ocurred during sorting tests");
        }
        else {
            System.out.println("Sorting tests completed successfully");
        }
    }
}


