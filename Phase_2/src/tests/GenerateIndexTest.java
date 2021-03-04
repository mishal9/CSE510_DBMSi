package tests;

import java.io.*;
import java.util.*;
import java.lang.*;
import global.*;
import btree.*;


/**
 * Note that in JAVA, methods can't be overridden to be more private.
 * Therefore, the declaration of all private functions are now declared
 * protected as opposed to the private type in C++.
 */

//watching point: RID rid, some of them may not have to be newed.

class GenerateIndexDriver implements GlobalConst {

    public BTreeFile file;
    public int keyType;

    protected String dbpath;
    protected String logpath;

    public void runTests() {
        Random random = new Random();
        dbpath = "BTREE" + random.nextInt() + ".minibase-db";
        logpath = "BTREE" + random.nextInt() + ".minibase-log";


        SystemDefs sysdef = new SystemDefs(dbpath, 200, NUMBUF, "Clock");
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

        try{
            test1();
            test2();
            test3();
            test4();
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    // creating combined BTreeIndex
    void test1()
            throws Exception {
        try {
            System.out.println(" Combined BTreeIndex creation");

            GenerateIndexFiles obj = new GenerateIndexFiles();
            IndexFile hf = obj.createCombinedBTreeIndex("../../data/subset.txt");

        } catch (Exception e) {
            throw e;
        }


    }

    // creating individual BTreeIndex
    void test2()
            throws Exception {
        try {
            System.out.println("Individual BTreeIndex creation");

            GenerateIndexFiles obj = new GenerateIndexFiles();
            IndexFile[] hf = obj.createBTreeIndex("../../data/subset.txt");

        } catch (Exception e) {
            throw e;
        }
    }

    // scanning in combined BTree
    void test3()
            throws Exception {
        try {
            BTFileScan scan;
            KeyDataEntry entry;
            System.out.println("CombinedBTreeIndex on all attributes scanning");

            GenerateIndexFiles obj = new GenerateIndexFiles();
            IndexFile hf = obj.createCombinedBTreeIndex("../../data/subset.txt");

            scan = ((BTreeFile)hf).new_scan(null, null);

            entry = scan.get_next();
            while(entry!=null) {
                System.out.println("SCAN RESULT: " + entry.key + " " + entry.data);
                entry = scan.get_next();
            }
            System.out.println("AT THE END OF SCAN!");

        } catch (Exception e) {
            throw e;
        }
    }

    // scanning in BTree on each index
    void test4()
            throws Exception {
        try {
            KeyDataEntry entry;
            System.out.println("BTreeIndex on individual attributes scanning");

            GenerateIndexFiles obj = new GenerateIndexFiles();
            IndexFile[] hf = obj.createBTreeIndex("../../data/subset.txt");

            BTFileScan[] scans = new BTFileScan[hf.length];
            for(int i=0;i<hf.length;i++){
                scans[i] = ((BTreeFile)hf[i]).new_scan(null, null);
            }
            for(BTFileScan scan: scans) {
                entry = scan.get_next();
                while (entry != null) {
                    System.out.println("SCAN RESULT: " + entry.key + " " + entry.data);
                    entry = scan.get_next();
                }
                System.out.println("AT THE END OF SCAN!");
            }
        } catch (Exception e) {
            throw e;
        }
    }

}


/**
 * To get the integer off the command line
 */

public class GenerateIndexTest implements GlobalConst {

    public static void main(String[] argvs) {

        try {
            GenerateIndexDriver btdriver = new GenerateIndexDriver();
            btdriver.runTests();
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Error encountered during buffer manager tests:\n");
            Runtime.getRuntime().exit(1);
        }
    }

}
