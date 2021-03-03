package driver;


import java.io.*;
import java.util.*;
import java.lang.*;
import heap.*;
import global.*;
import tests.TestDriver;

/** Note that in JAVA, methods can't be overridden to be more private.
 Therefore, the declaration of all private functions are now declared
 protected as opposed to the private type in C++.
 */

//watching point: RID rid, some of them may not have to be newed.

class Driver  extends TestDriver implements GlobalConst
{
    protected String dbpath;
    protected String logpath;

    private static int   NUM_RECORDS = 0;
    private static int   PGNUM = 12;
    private static int   COLS = 1;
    private static RID   rid;
    private static Heapfile  f = null;
    private boolean status = OK;

    public Driver(){
        super("main");
    }

    public boolean runTests () {
        System.out.println ("\n" + "Running " + testName() + " tests...." + "\n");

        dbpath = "/tmp/main"+System.getProperty("user.name")+".minibase-db";
        logpath = "/tmp/main"+System.getProperty("user.name")+".minibase-log";
        // Each page can handle at most 25 tuples on original data => 7308 / 25 = 292
        SystemDefs sysdef = new SystemDefs(dbpath,100, NUMBUF,"Clock");

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
        System.out.println("\n\n[0]   Read input data");
        System.out.println("[1]   Run Nested Loop skyline");
        System.out.println("\n[2]   Run Block Nested Loop skyline");
        System.out.println("[3]   Run Sorted First skyline");
        System.out.println("[4]   Display output data");

        System.out.println("\n[5]  Quit!");
        System.out.print("Hi, make your choice :");
    }
    
    private void readDataIntoHeap() throws IOException, InvalidTupleSizeException, InvalidTypeException {

        // Create the heap file object
        try {
            f = new Heapfile("file_1");
        }
        catch (Exception e) {
            status = FAIL;
            System.err.println ("*** Could not create heap file\n");
            e.printStackTrace();
        }

        if ( status == OK && SystemDefs.JavabaseBM.getNumUnpinnedBuffers()
                != SystemDefs.JavabaseBM.getNumBuffers() ) {
            System.err.println ("*** The heap file has left pages pinned\n");
            status = FAIL;
        }

        if ( status == OK ) {

            // Read data and construct tuples
            File file = new File("data/subset.txt");
            Scanner sc = new Scanner(file);

            COLS = sc.nextInt()+1;

            //  setting attribute types
            AttrType [] Stypes = new AttrType[5];
            Stypes[0] = new AttrType (AttrType.attrReal);
            Stypes[1] = new AttrType (AttrType.attrReal);
            Stypes[2] = new AttrType (AttrType.attrReal);
            Stypes[3] = new AttrType (AttrType.attrReal);
            Stypes[4] = new AttrType (AttrType.attrReal);

            //SOS
            short [] Ssizes = null;

            Tuple t = new Tuple();
            try {
                t.setHdr((short) 5,Stypes, Ssizes);
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
                t.setHdr((short) 5, Stypes, Ssizes);
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

                for(int i=1; i<doubleArray.length; i++) {
                    try {
                        t.setFloFld(i, (float) doubleArray[i]);
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

                System.out.println("RID: "+rid);
            }
        }
    }

    protected String testName () {
        return "Main Driver";
    }

    protected boolean runAllTests (){
        int choice=1;

        while(choice!=5) {
            menu();

            try{
                choice= GetStuff.getChoice();

                switch(choice) {
                    case 0:
                        readDataIntoHeap();
                        break;
                    case 1:

                        break;
                    case 2:
                        break;
                    case 3:
                        break;
                    case 4:
                        System.out.println("Please input the page number: ");
                        /*
                        num= GetStuff.getChoice();
                        if(num<0) break;
                        BT.printPage(new PageId(num), keyType);
                         */
                        break;
                    case 5:

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

        try{
            Driver driver = new Driver();
            driver.runTests();
        }
        catch (Exception e) {
            System.err.println ("Error encountered during running main driver:\n");
            e.printStackTrace();
            Runtime.getRuntime().exit(1);
        }
    }

}
