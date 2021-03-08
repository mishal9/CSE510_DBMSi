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

    private static RID   rid;
    private static Heapfile  f = null;
    private boolean status = OK;
    private static String _fileName;
    private static int[] _pref_list;
    private static int _n_pages;
    private static int COLS;

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
        System.out.println("\n\n[102]   Read input data 2");
        System.out.println("\n\n[103]   Read input data 3");
        System.out.println("\n\n[104]   Set pref = [1]");
        System.out.println("\n\n[105]   Set pref = [1,3]");
        System.out.println("\n\n[106]   Set pref = [1,3,5]");
        System.out.println("\n\n[107]   Set pref = [1,2,3,4,5]");
        System.out.println("\n\n[108]   Set n_page = 5");
        System.out.println("\n\n[109]   Set n_page = 10");
        System.out.println("\n\n[110]   Set n_page = <your_wish>");
        System.out.println("[1]  Run Nested Loop skyline on data with parameters ");
        System.out.println("[2]  Run Block Nested Loop on data with parameters ");
        System.out.println("[3]  Run Sort First Sky on data with parameters ");
        System.out.println("[4]  Run Btree Sky on data with parameters ");
        System.out.println("[5]  Run Btree Sort Sky on data with parameters ");
        System.out.println("\n[0]  Quit!");
        System.out.print("Hi, make your choice :");
    }
    
    private void readDataIntoHeap(String fileName) throws IOException, InvalidTupleSizeException, InvalidTypeException {

        // Create the heap file object
        try {
            f = new Heapfile("hFile");
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
            File file = new File("../../data/"+fileName+".txt");
            Scanner sc = new Scanner(file);

            COLS = sc.nextInt();

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

                for(int i=0; i<doubleArray.length; i++) {
                    try {
                        t.setFloFld(i+1, (float) doubleArray[i]);
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
        int choice=100;

        while(choice!=0) {
            menu();

            try{
                choice= GetStuff.getChoice();

                switch(choice) {

                    case 102:
                        readDataIntoHeap("data2");
                        break;

                    case 103:
                        readDataIntoHeap("data3");
                        break;

                    case 104:
                        _pref_list = new int[]{1};
                        break;

                    case 105:
                        _pref_list = new int[]{1,3};
                        break;

                    case 106:
                        _pref_list = new int[]{1,3,5};
                        break;

                    case 107:
                        _pref_list = new int[]{1,2,3,4,5};
                        break;

                    case 108:
                        _n_pages = 5;
                        break;

                    case 109:
                        _n_pages = 10;
                        break;

                    case 110:
                        _n_pages = GetStuff.getChoice();
                        if(_n_pages<0)
                            break;
                        break;

                    case 1:
                        // call nested loop sky
                        break;

                    case 2:
                        // call block nested loop sky
                        break;

                    case 3:
                        // call sort first sky
                        break;

                    case 4:
                        // call btree sky
                        break;

                    case 5:
                        // call btree sort sky
                        break;

                    case 0:
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
