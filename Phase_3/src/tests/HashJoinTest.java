package tests;
//originally from : joins.C

import iterator.*;
import heap.*;
import global.*;
import index.*;

import java.io.*;
import java.util.*;
import java.lang.*;

import btree.*;

/**
 * Here is the implementation for the tests. There are N tests performed.
 * We start off by showing that each operator works on its own.
 * Then more complicated trees are constructed.
 * As a nice feature, we allow the user to specify a selection condition.
 * We also allow the user to hardwire trees together.
 */

//Define the Sailor schema
class Sailor {
    public int sid;
    public String sname;
    public int rating;
    public double age;

    public Sailor(int _sid, String _sname, int _rating, double _age) {
        sid = _sid;
        sname = _sname;
        rating = _rating;
        age = _age;
    }
}

//Define the Boat schema
class Boats {
    public int bid;
    public String bname;
    public String color;

    public Boats(int _bid, String _bname, String _color) {
        bid = _bid;
        bname = _bname;
        color = _color;
    }
}

//Define the Reserves schema
class Reserves {
    public int sid;
    public int bid;
    public String date;

    public Reserves(int _sid, int _bid, String _date) {
        sid = _sid;
        bid = _bid;
        date = _date;
    }
}

class HashJoinsDriver extends TestDriver
implements GlobalConst {

    private boolean OK = true;
    private boolean FAIL = false;
    private Vector sailors;
    private Vector boats;
    private Vector reserves;
    private BTreeFile btree_index;
    /**
     * Constructor
     */
    private static int   LARGE = 1000;
    private static short REC_LEN1 = 32;
    private static short REC_LEN2 = 160;
    private static int   SORTPGNUM = 12;

    protected String testName()
    {
        return "HashJoin";
    }

    public HashJoinsDriver() {

        String dbpath = "/tmp/" + System.getProperty("user.name") + ".minibase.jointestdb";
        String logpath = "/tmp/" + System.getProperty("user.name") + ".joinlog";

        String remove_cmd = "/bin/rm -rf ";
        String remove_logcmd = remove_cmd + logpath;
        String remove_dbcmd = remove_cmd + dbpath;
        String remove_joincmd = remove_cmd + dbpath;

        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
            Runtime.getRuntime().exec(remove_joincmd);
        } catch (IOException e) {
            System.err.println("" + e);
        }
        SystemDefs sysdef = new SystemDefs(dbpath, 1000, NUMBUF, "Clock");
    }

    public boolean runAllTests() {

        Query7();
        System.out.print("Finished joins testing" + "\n");


        return true;
    }

    private void Query7_CondExpr_int(CondExpr[] expr) {
        expr[0] = new CondExpr();
        expr[0].next = null;
        expr[0].op = new AttrOperator(AttrOperator.aopEQ);

        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 1);

        expr[0].type2 = new AttrType(AttrType.attrSymbol);
        expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 1);

        expr[1] = null;
    }
    private void Query7_CondExpr_str(CondExpr[] expr) {
        expr[0] = new CondExpr();
        expr[0].next = null;
        expr[0].op = new AttrOperator(AttrOperator.aopEQ);

        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer), 2);

        expr[0].type2 = new AttrType(AttrType.attrSymbol);
        expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel), 3);

        expr[1] = null;
    }

    public void Query7() {
        System.out.print("**********************Query7 strating *********************\n");
        boolean status = OK;
        System.out.print("Query: Join sailors with reserves on id\n"
                + "  SELECT   S.sname, S.sid\n"
                + "  JOIN Sailors'S' 1  Reserves'R' 1 = \n\n\n");

        CondExpr[] outFilter = new CondExpr[2];

        Tuple t = new Tuple();

        String outer_relation_name = "test1_o.in";
        String inner_relation_name = "test1_i.in";


        AttrType[] Stypes = {
                new AttrType(AttrType.attrInteger),
                new AttrType(AttrType.attrString),
                new AttrType(AttrType.attrInteger),
                new AttrType(AttrType.attrReal)
        };
        short[] Ssizes = new short[1];
        Ssizes[0] = 30;

        // creating outer relation's heap file
        try{
            Heapfile hf = new Heapfile(outer_relation_name);
            t.setHdr((short)Stypes.length, Stypes, Ssizes);
            int size = t.size();
            t = new Tuple(size);
            t.setHdr((short)Stypes.length, Stypes, Ssizes);
            for(int i=0;i<20;i++){
                int j = i%20;
                t.setIntFld(1, j);
                t.setStrFld(2, "f:"+String.valueOf((float)j));
                t.setIntFld(3, i);
                t.setFloFld(4, (float)j);

                hf.insertRecord(t.getTupleByteArray());
            }
            System.out.println("Outer Heap file created.");
        }catch (Exception e){
            e.printStackTrace();
        }


        AttrType[] Rtypes = {
                new AttrType(AttrType.attrInteger),
                new AttrType(AttrType.attrInteger),
                new AttrType(AttrType.attrString),
        };
        short[] Rsizes = new short[1];
        Rsizes[0] = 15;
        // creating inner relation's heap file
        try{
            Heapfile hf2 = new Heapfile(inner_relation_name);
            t.setHdr((short)Rtypes.length, Rtypes, Rsizes);
            int size = t.size();
            t = new Tuple(size);
            t.setHdr((short)Rtypes.length, Rtypes, Rsizes);
            for(int i=10;i<20;i++){
                t.setIntFld(1, i);
                t.setIntFld(2, i);
                t.setStrFld(3, "f:"+String.valueOf((float)i));

                hf2.insertRecord(t.getTupleByteArray());
            }
            System.out.println("Inner Heap file created.");
        }catch (Exception e){
            e.printStackTrace();
        }

        short[] JJsize = new short[1];
        JJsize[0] = 30;

        FldSpec[] proj1 = {
                new FldSpec(new RelSpec(RelSpec.outer), 1),
                new FldSpec(new RelSpec(RelSpec.outer), 2),
                new FldSpec(new RelSpec(RelSpec.outer), 3),
                new FldSpec(new RelSpec(RelSpec.outer), 4),
                new FldSpec(new RelSpec(RelSpec.innerRel), 1),
                new FldSpec(new RelSpec(RelSpec.innerRel), 2),
                new FldSpec(new RelSpec(RelSpec.innerRel), 3)
        };

        FldSpec[] Sprojection = {
                new FldSpec(new RelSpec(RelSpec.outer), 1),
                new FldSpec(new RelSpec(RelSpec.outer), 2),
                new FldSpec(new RelSpec(RelSpec.outer), 3),
                new FldSpec(new RelSpec(RelSpec.outer), 4)
        };

        try {
            int count = 0;
            System.out.println("\nJoin on integer attribute.");
            Query7_CondExpr_int(outFilter);
            FileScan am = null;
            am = new FileScan(outer_relation_name, Stypes, Ssizes,
                    (short) Stypes.length, (short) Sprojection.length, Sprojection, null);
            HashJoin inl = null;

            inl = new HashJoin(Stypes, Stypes.length, Ssizes,
                    Rtypes, Rtypes.length, Rsizes,
                    50,
                    am, inner_relation_name,
                    outFilter, null, proj1, proj1.length);
            AttrType [] JJtype = {
                    new AttrType(AttrType.attrInteger),
                    new AttrType(AttrType.attrString),
                    new AttrType(AttrType.attrInteger),
                    new AttrType(AttrType.attrReal),
                    new AttrType(AttrType.attrInteger),
                    new AttrType(AttrType.attrInteger),
                    new AttrType(AttrType.attrString)
            };
            int c = 0;
            while ((t = inl.get_next()) != null) {
                count++;
                t.print(JJtype);
            }
            am.close();
            System.out.println("total "+count+" tuples in the result.");
            inl.close();
        }catch (Exception e) {
            System.err.println("*** Error preparing for nested_loop_join");
            System.err.println("" + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }


        try {
            int count = 0;
            System.out.println("\n\nJoin on String attribute.");
            Query7_CondExpr_str(outFilter);
            FileScan am = null;
            am = new FileScan(outer_relation_name, Stypes, Ssizes,
                    (short) Stypes.length, (short) Sprojection.length, Sprojection, null);
            HashJoin inl = null;
            inl = new HashJoin(Stypes, Stypes.length, Ssizes,
                    Rtypes, Rtypes.length, Rsizes,
                    50,
                    am, inner_relation_name,
                    outFilter, null, proj1, proj1.length);
            AttrType [] JJtype = {
                    new AttrType(AttrType.attrInteger),
                    new AttrType(AttrType.attrString),
                    new AttrType(AttrType.attrInteger),
                    new AttrType(AttrType.attrReal),
                    new AttrType(AttrType.attrInteger),
                    new AttrType(AttrType.attrInteger),
                    new AttrType(AttrType.attrString)
            };
            int c = 0;
            while ((t = inl.get_next()) != null) {
                count ++;
                t.print(JJtype);
            }
            am.close();
            System.out.println("total "+count+" tuples in the result.");
            inl.close();
        }catch (Exception e) {
            System.err.println("*** Error preparing for nested_loop_join");
            System.err.println("" + e);
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }




        Runtime.getRuntime().exit(1);
    }
}


public class HashJoinTest {
    public static void main(String argv[]) {
        boolean sortstatus=false;
//        SystemDefs global = new SystemDefs("bingjiedb", 100, 70, null);
//        JavabaseDB.openDB("/tmp/nwangdb", 5000);

        HashJoinsDriver jjoin = new HashJoinsDriver();

        try{
            sortstatus = jjoin.runAllTests();
        }catch (Exception e){
            e.printStackTrace();
        }
        if (sortstatus != true) {
            System.out.println("Error ocurred during join tests");
        } else {
            System.out.println("join tests completed successfully");
        }
    }
}
