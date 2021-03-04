package tests;

import java.io.*;
import global.*;
import bufmgr.*;
import diskmgr.*;
import heap.*;
import iterator.*;
import index.*;
import skylines.SortFirstSky;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;


class SortFirstSkyDriver extends TestDriver
        implements GlobalConst {

    private static float[][] data1 = {
            {0.825f, 0.823f, 0.453f, 0.122f, 0.356f},
            {0.855f, 0.316f, 0.782f, 0.478f, 0.758f},
            {0.011f, 0.268f, 0.348f, 0.646f, 0.161f},
            {0.896f, 0.572f, 0.281f, 0.592f, 0.166f},
            {0.852f, 0.194f, 0.613f, 0.846f, 0.846f},
            {0.110f, 0.758f, 0.221f, 0.234f, 0.169f},
            {0.259f, 0.550f, 0.837f, 0.138f, 0.960f},
            {0.821f, 0.814f, 0.104f, 0.106f, 0.475f},
            {0.192f, 0.975f, 0.761f, 0.157f, 0.899f},
            {0.627f, 0.043f, 0.133f, 0.690f, 0.272f},
            {0.626f, 0.095f, 0.048f, 0.173f, 0.181f},
            {0.086f, 0.366f, 0.073f, 0.169f, 0.791f},
            {0.955f, 0.379f, 0.100f ,0.170f, 0.297f},
            {0.287f, 0.298f, 0.602f, 0.381f, 0.262f},
            {0.397f, 0.472f, 0.163f, 0.626f, 0.745f},
            {0.540f, 0.443f, 0.621f, 0.804f, 0.149f},
            {0.801f, 0.804f, 0.761f, 0.441f, 0.352f},
            {0.797f, 0.139f, 0.808f, 0.493f, 0.244f},
            {0.324f, 0.257f, 0.609f, 0.529f, 0.349f}
    };


    private static int   NUM_RECORDS = data1.length;
    private static short REC_LEN1 = 160;
    private static short REC_LEN2 = 160;
    private static short REC_LEN3 = 160;
    private static short REC_LEN4 = 160;
    private static short REC_LEN5 = 160;
    private static int   SORTPGNUM = 12;

    TupleOrder[] order = new TupleOrder[2];

    public SortFirstSkyDriver() {
        super("sortfirstskytest");
        order[0] = new TupleOrder(TupleOrder.Ascending);
        order[1] = new TupleOrder(TupleOrder.Descending);
    }

    public boolean runTests () throws HFDiskMgrException, HFException, HFBufMgrException, IOException {

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

        // Create unsorted data file "test1sortFirstSky.in"
        RID             rid;
        Heapfile        f = null;
        try {
            f = new Heapfile("test1sortFirstSky.in");
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
            fscan = new FileScan("test1sortFirstSky.in", attrType, attrSize, (short) 5, 5, projlist, null);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        // Sort "test1sortFirstSky.in"
        Sort sort = null;
        try {
            sort = new Sort(attrType, (short) 5, attrSize, fscan, order[1], new int[]{1,2}, 2, SORTPGNUM);
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

            if (!Arrays.equals(outval, data1[count])) {
                System.err.println("outval = " + outval[0] + "\tdata2[count] = " + data1[count][0]);

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


    protected boolean test2()  {
        System.out.println("------------------------ TEST 2 --------------------------");

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

        // Create unsorted data file "test1sortFirstSky.in"
        RID             rid;
        Heapfile        f = null;
        try {
            f = new Heapfile("test1sortFirstSky.in");
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

        String relation = "test1sortFirstSky.in";

        try {
            fscan = new FileScan(relation, attrType, attrSize, (short) 5, 5, projlist, null);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        // Sort "test1sortFirstSky.in"
        Sort sort = null;
        try {
            sort = new Sort(attrType, (short) 5, attrSize, fscan, order[1], new int[]{1,2}, 2, SORTPGNUM);
        }
        catch (Exception e) {
            status = FAIL;
            e.printStackTrace();
        }

        try {
            SortFirstSky sortFirstSky = new SortFirstSky(attrType,
                                                        (short) 5,
                                                        attrSize,
                                                        sort,
                                                        relation,
                                                        new int[]{1,2},
                                                       2,
                                                        SORTPGNUM);
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
        }

        return status;
    }


    protected String testName()
    {
        return "SortFirstSky";
    }
}

public class SortFirstSkyTest
{
    public static void main(String argv[]) throws IOException, HFException, HFBufMgrException, HFDiskMgrException {
        boolean sortstatus;

        SortFirstSkyDriver driver = new SortFirstSkyDriver();

        sortstatus = driver.runTests();
        if (sortstatus != true) {
            System.out.println("Error occurred during sort first sky tests");
        }
        else {
            System.out.println("Sort first sky tests completed successfully");
        }
    }
}


