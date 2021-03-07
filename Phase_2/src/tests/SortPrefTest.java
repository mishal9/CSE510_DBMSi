package tests;

import java.io.*;
import global.*;
import heap.*;
import iterator.*;

import java.util.Arrays;


class SORTPrefDriver extends TestDriver
        implements GlobalConst {

    private static float[][] data1 = {
            {0.825f, 0.823f, 0.453f, 0.122f, 0.356f},   // 1.648
            {0.855f, 0.316f, 0.782f, 0.478f, 0.758f},   // 1.171
            {0.011f, 0.268f, 0.348f, 0.646f, 0.161f},   // 0.279
            {0.896f, 0.572f, 0.281f, 0.592f, 0.166f},   // 1.468
            {0.852f, 0.194f, 0.613f, 0.846f, 0.846f},   // 1.046
            {0.110f, 0.758f, 0.221f, 0.234f, 0.169f},   // 0.868
            {0.259f, 0.550f, 0.837f, 0.138f, 0.960f},   // 0.809
            {0.821f, 0.814f, 0.104f, 0.106f, 0.475f},   // 1.635
            {0.192f, 0.975f, 0.761f, 0.157f, 0.899f},   // 1.167
            {0.627f, 0.043f, 0.133f, 0.690f, 0.272f}    // 0.67
    };
    // [0,1]
    private static float[][] data2 = {
            {0.011f, 0.268f, 0.348f, 0.646f, 0.161f},   // 0.279
            {0.627f, 0.043f, 0.133f, 0.690f, 0.272f},   // 0.67
            {0.259f, 0.550f, 0.837f, 0.138f, 0.960f},   // 0.809
            {0.110f, 0.758f, 0.221f, 0.234f, 0.169f},   // 0.868
            {0.852f, 0.194f, 0.613f, 0.846f, 0.846f},   // 1.046
            {0.192f, 0.975f, 0.761f, 0.157f, 0.899f},   // 1.167
            {0.855f, 0.316f, 0.782f, 0.478f, 0.758f},   // 1.171
            {0.896f, 0.572f, 0.281f, 0.592f, 0.166f},   // 1.468
            {0.821f, 0.814f, 0.104f, 0.106f, 0.475f},   // 1.635
            {0.825f, 0.823f, 0.453f, 0.122f, 0.356f},   // 1.648
    };

    private static float[][] data3 = {
            {0.825f, 0.823f, 0.453f, 0.122f, 0.356f},   // 1.648
            {0.821f, 0.814f, 0.104f, 0.106f, 0.475f},   // 1.635
            {0.896f, 0.572f, 0.281f, 0.592f, 0.166f},   // 1.468
            {0.855f, 0.316f, 0.782f, 0.478f, 0.758f},   // 1.171
            {0.192f, 0.975f, 0.761f, 0.157f, 0.899f},   // 1.167
            {0.852f, 0.194f, 0.613f, 0.846f, 0.846f},   // 1.046
            {0.110f, 0.758f, 0.221f, 0.234f, 0.169f},   // 0.868
            {0.259f, 0.550f, 0.837f, 0.138f, 0.960f},   // 0.809
            {0.627f, 0.043f, 0.133f, 0.690f, 0.272f},   // 0.67
            {0.011f, 0.268f, 0.348f, 0.646f, 0.161f},   // 0.279
    };

    /*
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

    private static float[][] data3 = {
            {0.825f,   0.823f,    0.453f,    0.122f,    0.356f}, // -> 1.648
            {0.896f,   0.572f,    0.281f,    0.592f,    0.166f}, // -> 1.468
            {0.855f,   0.316f,    0.782f,    0.478f,    0.758f}, // -> 1.171
            {0.852f,   0.194f,    0.613f,    0.846f,    0.846f}, // -> 1.046
            {0.011f,   0.268f,    0.348f,    0.646f,    0.161f}, // -> 0.279
    };
    */

    private static int   NUM_RECORDS = data2.length;
    private static short REC_LEN1 = 32;
    private static short REC_LEN2 = 32;
    private static short REC_LEN3 = 32;
    private static short REC_LEN4 = 32;
    private static short REC_LEN5 = 32;
    private static int   SORTPGNUM = 20;

    TupleOrder[] order = new TupleOrder[2];

    public SORTPrefDriver() {
        super("sorttest");
        order[0] = new TupleOrder(TupleOrder.Ascending);
        order[1] = new TupleOrder(TupleOrder.Descending);
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
        System.out.println("------------------------ TEST 1 --------------------------");

        boolean status = OK;

        AttrType[] attrType = new AttrType[6];
        attrType[0] = new AttrType(AttrType.attrReal);
        attrType[1] = new AttrType(AttrType.attrReal);
        attrType[2] = new AttrType(AttrType.attrReal);
        attrType[3] = new AttrType(AttrType.attrReal);
        attrType[4] = new AttrType(AttrType.attrReal);
        attrType[5] = new AttrType(AttrType.attrReal);

        short[] attrSize = new short[6];
        attrSize[0] = REC_LEN1;
        attrSize[1] = REC_LEN2;
        attrSize[2] = REC_LEN3;
        attrSize[3] = REC_LEN4;
        attrSize[4] = REC_LEN5;
        attrSize[5] = REC_LEN5;



        // create a tuple of appropriate size
        Tuple t = new Tuple();
        try {
            t.setHdr((short) 6, attrType, attrSize);
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
            f = new Heapfile("testnsortPref.in");
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        t = new Tuple(size);
        try {
            t.setHdr((short) 6, attrType, attrSize);
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
        FldSpec[] projlist = new FldSpec[6];
        RelSpec rel = new RelSpec(RelSpec.outer);
        projlist[0] = new FldSpec(rel, 1);
        projlist[1] = new FldSpec(rel, 2);
        projlist[2] = new FldSpec(rel, 3);
        projlist[3] = new FldSpec(rel, 4);
        projlist[4] = new FldSpec(rel, 5);
        projlist[5] = new FldSpec(rel, 6);

        FileScan fscan = null;

        try {
            fscan = new FileScan("testnsortPref.in", attrType, attrSize, (short) 6, 6, projlist, null);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        // Sort "test1sortPref.in"
        SortPref sort = null;
        try {
            sort = new SortPref(attrType, (short) 6, attrSize, fscan, order[1], new int[]{1,5}, 2, SORTPGNUM);
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

                System.out.println("Got row: "+outval[0]+" "+outval[1]+" "+outval[2]+" "+outval[3]+" "+outval[4]+" ");
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

        return status;
    }


    protected String testName()
    {
        return "SortPref";
    }
}

public class SortPrefTest
{
    public static void main(String argv[]) throws IOException, HFException, HFBufMgrException, HFDiskMgrException {
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


