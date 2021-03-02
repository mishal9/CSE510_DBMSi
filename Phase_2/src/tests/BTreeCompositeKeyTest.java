package tests;

import java.io.*;
import java.util.*;
import java.lang.*;

import heap.*;
import bufmgr.*;
import diskmgr.*;
import global.*;
import btree.*;
import org.w3c.dom.Attr;

class BTCompositeDriver implements GlobalConst {
    public BTreeFile file;
    public int postfix = 0;
    public int keyType;
    public BTFileScan scan;

    protected String dbpath;
    protected String logpath;
    public int deleteFashion;

    int ROW, COL;
    float[][] records;

    public void runTests() {
        Random random = new Random();
        dbpath = "BTREE" + random.nextInt() + ".minibase-db";
        logpath = "BTREE" + random.nextInt() + ".minibase-log";


        SystemDefs sysdef = new SystemDefs(dbpath, 5000, 5000, "Clock");
        System.out.println("\n" + "Running " + " tests...." + "\n");

        keyType = AttrType.attrInteger;

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
        } catch (IOException e) {
            System.err.println("IO error: " + e);
        }

        remove_logcmd = remove_cmd + newlogpath;
        remove_dbcmd = remove_cmd + newdbpath;

        //This step seems redundant for me.  But it's in the original
        //C++ code.  So I am keeping it as of now, just in case I
        //I missed something
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        } catch (IOException e) {
            System.err.println("IO error: " + e);
        }

        //Run the tests. Return type different from C++
        runAllTests();

        //Clean up again
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        } catch (IOException e) {
            System.err.println("IO error: " + e);
        }

        System.out.print("\n" + "..." + " Finished ");
        System.out.println(".\n\n");


    }

    protected void runAllTests() {
        PageId pageno = new PageId();
        int key, n, m, num, choice, lowkeyInt, hikeyInt;
        KeyClass lowkey, hikey;
        KeyDataEntry entry;
        RID rid;
        choice = 1;
        deleteFashion = 1; //full delete
        // creating records of size 4
        ROW = 3;
        COL = 4;
//        records = new float[ROW][COL];
        records = new float[][]{
                {(float) 0.1, (float) 0.2, (float) 0.3, (float) 0.4},
                {(float) 0.1, (float) 0.2, (float) 0.3, (float) 0.2},
                {(float) 0.1, (float) 0.3, (float) 0.4, (float) 0.5}
        };
        keyType = AttrType.attrString;

        Random r = new Random();
        int k = 0;
//        for (int i = 0; i < ROW; i++) {
//            for (int j = 0; j < COL; j++) {
//                records[i][j] = (   (float)(k++)/10%ROW);//r.nextFloat();
//            }
//        }

        test1();
    }

    public static String padRight(String s, int n) {
        return String.format("%-" + n + "s", s).replace(' ', '0');
    }

    String create_key(float[] values) {
        String s = new String();

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < values.length; i++) {
//            s = String.format("%1$" + 10 + "s", String.valueOf(values[i])).replace(' ', '0');
            s = padRight(String.valueOf(values[i]), 10);
            sb.append(s);
            sb.append("|");
        }
        return sb.toString();
    }

    /*
    * For composite key, one option is to combine the keys that we are trying to index into a string formulation and then use it as string index.
    * For example, we have [0.1, 0.2, 0.3, 0.04] as attributes, we combine them and use "0.10|0.20|0.30|0.04|" as key
    *   each attribute must be converted to its maximum precission ex. 0.2 -> 0.20000000.
    *   Use "|" as seperator to make sure no two different rows have same index.
     */
    void test1() {
        try {
            System.out.println(" ***************** The file name is: " + "AAA" + postfix + "  **********");
            file = new BTreeFile("AAA" + postfix, keyType, 1 + 12 * COL, deleteFashion);
            file.traceFilename("TRACE");

            KeyClass key;
            RID rid = new RID();
            PageId pageno = new PageId();
            String s;
            for (int i = 0; i < ROW; i++) {
                s = create_key(records[i]);
                key = new StringKey(s);
                pageno.pid = i;
                rid = new RID(pageno, i);
                System.out.println("Inserted: " + key + " for record id: " + rid);
                file.insert(key, rid);

            }
            BT.printBTree(file.getHeaderPage());
            BT.printAllLeafPages(file.getHeaderPage());

        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("       !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
            System.out.println("       !!         Something is wrong                    !!");
            System.out.println("       !!     Is your DB full? then exit. rerun it!     !!");
            System.out.println("       !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");

        }


    }

    void test5(int n, int m)
            throws Exception {
        try {

            System.out.println(" ***************** The file name is: " + "AAA" + postfix + "  **********");
            file = new BTreeFile("AAA" + postfix, keyType, 20, deleteFashion);
            file.traceFilename("TRACE");

            int[] k = new int[n];
            for (int i = 0; i < n; i++) {
                k[i] = i;
            }

            Random ran = new Random();
            int random;
            int tmp;
            for (int i = 0; i < n; i++) {
                random = (ran.nextInt()) % n;
                if (random < 0) random = -random;
                tmp = k[i];
                k[i] = k[random];
                k[random] = tmp;
            }
            for (int i = 0; i < n; i++) {
                random = (ran.nextInt()) % n;
                if (random < 0) random = -random;
                tmp = k[i];
                k[i] = k[random];
                k[random] = tmp;
            }


            KeyClass key;
            RID rid = new RID();
            PageId pageno = new PageId();
            for (int i = 0; i < n; i++) {
                key = new StringKey("**" + k[i]);
                pageno.pid = k[i];
                rid = new RID(pageno, k[i]);

                file.insert(key, rid);

            }

            for (int i = 0; i < n; i++) {
                random = (ran.nextInt()) % n;
                if (random < 0) random = -random;
                tmp = k[i];
                k[i] = k[random];
                k[random] = tmp;
            }
            for (int i = 0; i < n; i++) {
                random = (ran.nextInt()) % n;
                if (random < 0) random = -random;
                tmp = k[i];
                k[i] = k[random];
                k[random] = tmp;
            }

            for (int i = 0; i < m; i++) {
                key = new StringKey("**" + k[i]);
                pageno.pid = k[i];
                rid = new RID(pageno, k[i]);

                if (file.Delete(key, rid) == false) {
                    System.out.println("*********************************************************");
                    System.out.println("*     Your delete method has bug!!!                     *");
                    System.out.println("*     You insert a record, But you failed to delete it. *");
                    System.out.println("*********************************************************");
                }

            }


        } catch (Exception e) {
            throw e;
        }
    }
}

public class BTreeCompositeKeyTest implements GlobalConst {
    public static void main(String[] argvs) {
        try {
            BTCompositeDriver bttest = new BTCompositeDriver();
            bttest.runTests();

        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Error encountered during buffer manager tests:\n");
            Runtime.getRuntime().exit(1);
        }
    }
}
