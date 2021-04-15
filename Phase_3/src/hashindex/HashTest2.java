package hashindex;

import java.io.File;
import java.io.IOException;
import java.util.Vector;

import global.*;
import heap.Heapfile;
import heap.Scan;
import heap.Tuple;
import iterator.FldSpec;
import iterator.Iterator;
import iterator.RelSpec;
//import tests.Sailor;


class Row{
    int id;
    char name;
    public Row(int id, char name){
        this.id = id;
        this.name = name;
    }
}

public class HashTest2 implements GlobalConst {
    Heapfile hf = null;
    String heap_file_name = "heap1.in", index_file_name= "notwhatever";
    AttrType [] Dtypes;
    short [] Ssizes;

    public static void main(String[] args) {

        System.out.println("Start");
        try {
            HashTest2 thiss = new HashTest2();
            thiss.createAHeapFile();
            thiss.testHindex();

        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("End");
    }

    private void createAHeapFile() throws Exception{
        hf = new Heapfile(heap_file_name);
        Vector data = new Vector();
        for(int i=1; i<10; i++){
            data.addElement(new Row(i, (char)(96+i) ) );
        }

        Dtypes = new AttrType[2];
        Dtypes[0] = new AttrType (AttrType.attrInteger);
        Dtypes[1] = new AttrType (AttrType.attrString);
        Ssizes = new short [1];
        Ssizes[0] = 30; //first elt. is 30

        Tuple t = new Tuple();
        t.setHdr((short) 2, Dtypes, Ssizes);
        int size = t.size();

        RID rid;

        for (int i=0; i<data.size(); i++) {
            t.setIntFld(1, ((Row)data.elementAt(i)).id);
            t.setStrFld(2, String.valueOf(((Row)data.elementAt(i)).name));
            hf.insertRecord(t.getTupleByteArray());
        }
        hf = null;
    }

    private void testHindex() throws Exception {
        HIndex h = new HIndex(index_file_name, AttrType.attrInteger, 4,5);
        Scan s = (new Heapfile(heap_file_name)).openScan();
        Tuple tup = new Tuple();
        RID rid = new RID();
        while((tup=s.getNext(rid))!=null){
            tup.setHdr((short)2, Dtypes, Ssizes);
            HashKey key = new HashKey(tup.getIntFld(1));
            h.insert(key, rid);
        }
        FldSpec[] out = {
            new FldSpec(new RelSpec(RelSpec.outer), 1),
            new FldSpec(new RelSpec(RelSpec.outer), 2)
        };
        HashIndexWindowedScan hiwfs = new HashIndexWindowedScan(new IndexType(IndexType.Hash), heap_file_name, index_file_name, Dtypes, Ssizes, Dtypes.length, out.length, out, null, 1, false);
        Iterator it;
        while((it=hiwfs.get_next())!=null){
            while((tup=it.get_next())!=null){
                tup.setHdr((short)2, Dtypes, Ssizes);
                tup.print(Dtypes);
            }
            System.out.println("\n New Bucket ");
        }

    }


    public HashTest2() {
        long time=System.currentTimeMillis();
        time=10;

        String dbpath = "HASHTEST" + time + ".minibase-db";
        new File(dbpath).delete();
        //SystemDefs.MINIBASE_RESTART_FLAG=true;
        SystemDefs sysdef = new SystemDefs(dbpath, 5000, 100, "Clock");

    }


    public static void printPinnedPages() {
        System.out.println("pin: "+(SystemDefs.JavabaseBM.getNumBuffers()- SystemDefs.JavabaseBM.getNumUnpinnedBuffers()));

    }

}
