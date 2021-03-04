package tests;

import java.io.*;
import java.util.*;
import java.lang.*;

import driver.GenerateIndexFiles;
import heap.*;
import bufmgr.*;
import diskmgr.*;
import global.*;
import btree.*;
import index.IndexException;
import index.IndexScan;
import index.UnknownIndexTypeException;
import iterator.FldSpec;
import iterator.RelSpec;
import iterator.UnknownKeyTypeException;

/**
 * Note that in JAVA, methods can't be overridden to be more private.
 * Therefore, the declaration of all private functions are now declared
 * protected as opposed to the private type in C++.
 */

//watching point: RID rid, some of them may not have to be newed.

class GenerateIndexDriver implements GlobalConst {

    public BTreeFile file;
    public int postfix = 0;
    public int keyType;

    protected String dbpath;
    protected String logpath;
    public int deleteFashion;

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

    private void menu() {
        System.out.println("-------------------------- MENU ------------------");
        System.out.println("\n\n[0]   Naive delete (new file)");
        System.out.println("[1]   Full delete(Default) (new file)");

        System.out.println("\n[2]   Print the B+ Tree Structure");
        System.out.println("[3]   Print All Leaf Pages");
        System.out.println("[4]   Choose a Page to Print");

        System.out.println("\n           ---Integer Key (for choices [6]-[14]) ---");
        System.out.println("\n[5]   Insert a Record");
        System.out.println("[6]   Delete a Record");
        System.out.println("[7]   Test1 (new file): insert n records in order");
        System.out.println("[8]   Test2 (new file): insert n records in reverse order");
        System.out.println("[9]   Test3 (new file): insert n records in random order");
        System.out.println("[10]  Test4 (new file): insert n records in random order");
        System.out.println("      and delete m records randomly");
        System.out.println("[11]  Delete some records");

        System.out.println("\n[12]  Initialize a Scan");
        System.out.println("[13]  Scan the next Record");
        System.out.println("[14]  Delete the just-scanned record");
        System.out.println("\n           ---String Key (for choice [15]) ---");
        System.out.println("\n[15]  Test5 (new file): insert n records in random order  ");
        System.out.println("        and delete m records randomly.");

        System.out.println("\n[16]  Close the file");
        System.out.println("[17]  Open which file (input an integer for the file name): ");
        System.out.println("[18]  Destroy which file (input an integer for the file name): ");


        System.out.println("\n[19]  Quit!");
        System.out.print("Hi, make your choice :");
    }


    protected void runAllTests() {
        deleteFashion = 1; //full delete

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
            IndexFile hf = obj.createCombinedBTreeIndex("driver/data/subset.txt");

        } catch (Exception e) {
            throw e;
        }


    }

    // creating individual BTreeIndex
    void test2()
            throws Exception {
        try {
            System.out.println(" BTreeIndex creation");

            GenerateIndexFiles obj = new GenerateIndexFiles();
            IndexFile[] hf = obj.createBTreeIndex("driver/data/subset.txt");

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
            System.out.println("CombinedBTreeIndex scanning");

            GenerateIndexFiles obj = new GenerateIndexFiles();
            IndexFile indexFile = obj.createCombinedBTreeIndex("driver/data/subset2.txt");
            System.out.println("Index created! ");

            scan = ((BTreeFile) indexFile).new_scan(null, null);
            Heapfile hf = new Heapfile("heap_" + "AAA" + obj.prefix);
            Scan heap_scan = new Scan(hf);

            RID rid;
            entry = scan.get_next();
            while (entry != null) {
//                System.out.println("SCAN RESULT: " + entry.key + " > " + entry.data);
                entry = scan.get_next();
            }
//            System.out.println("AT THE END OF SCAN!");

//      Have to add code to SCAN the heap file using the BTree Index
//            FldSpec[] projlist = new FldSpec[3];
//            RelSpec rel = new RelSpec(RelSpec.outer);
//            projlist[0] = new FldSpec(rel, 1);
//            projlist[1] = new FldSpec(rel, 2);
//            projlist[1] = new FldSpec(rel, 3);
//            int COLS = 3;
//            AttrType[] Stypes = new AttrType[COLS];
//            for (int i = 0; i < COLS; i++) {
//                Stypes[i] = new AttrType(AttrType.attrReal);
//            }
//
//            IndexScan iscan = new IndexScan(new IndexType(IndexType.B_Index), "AAA" + (obj.prefix - 1), "BTreeIndex", Stypes, null, 3, 2, projlist, null, 3, true);
//            int count = 0;
//            Tuple t = null;
//            String outval = null;
//            t = iscan.get_next();
//            boolean flag = true;
//
//            while (t != null) {
//                t = iscan.get_next();
//                System.out.println(t.noOfFlds());
//            }
        } catch (Exception e){
            e.printStackTrace();
        }
    }

    // scanning in BTree on each index
    void test4()
            throws Exception {
        try {
            KeyDataEntry entry;
            System.out.println("BTreeIndex scanning");

            GenerateIndexFiles obj = new GenerateIndexFiles();
            IndexFile[] hf = obj.createBTreeIndex("driver/data/subset2.txt");

            BTFileScan[] scans = new BTFileScan[hf.length];
            for(int i=0;i<hf.length;i++){
                scans[i] = ((BTreeFile)hf[i]).new_scan(null, null);
            }
            for(BTFileScan scan: scans) {
                entry = scan.get_next();
                while (entry != null) {
                    // System.out.println("SCAN RESULT: " + entry.key + " " + entry.data);
                    entry = scan.get_next();
                }
                // System.out.println("AT THE END OF SCAN!");
            }
        } catch (Exception e) {
            throw e;
        }
    }

    void test5(int n, int m)
            throws Exception {}


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
