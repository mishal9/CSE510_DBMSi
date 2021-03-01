package driver;


import java.io.*;
import java.util.*;
import java.lang.*;
import heap.*;
import bufmgr.*;
import diskmgr.*;
import global.*;
import btree.*;

/** Note that in JAVA, methods can't be overridden to be more private.
 Therefore, the declaration of all private functions are now declared
 protected as opposed to the private type in C++.
 */

//watching point: RID rid, some of them may not have to be newed.

class Driver  implements GlobalConst
{
    protected String dbpath;
    protected String logpath;

    private static int   NUM_RECORDS = 0;
    private static int   SORTPGNUM = 12;
    private static int   COLS = 1;
    private static RID   rid;
    private static Heapfile  f = null;
    private boolean status = true;


    public Driver(){
        try {
            f = new Heapfile("test1.in");
        }
        catch (Exception e) {
            status = false;
            e.printStackTrace();
        }
    }

    // Create unsorted data file "test1.in"



    public void runTests () {

        Random random = new Random();
        dbpath = "MAIN" + random.nextInt() + ".minibase-db";
        logpath = "MAIN" + random.nextInt() + ".minibase-log";

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

        //This step seems redundant for me.  But it's in the original
        //C++ code.  So I am keeping it as of now, just in case I
        //I missed something
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        }
        catch (IOException e) {
            System.err.println ("IO error: "+e);
        }

        //Run the tests. Return type different from C++
        runAllTests();

        //Clean up again
        try {
            Runtime.getRuntime().exec(remove_logcmd);
            Runtime.getRuntime().exec(remove_dbcmd);
        }
        catch (IOException e) {
            System.err.println ("IO error: "+e);
        }

        System.out.print ("\n" + "..." + " Finished ");
        System.out.println (".\n\n");


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
    
    private void readData() throws FileNotFoundException {
        File file = new File("data/subset.txt");
        Scanner sc = new Scanner(file);

        COLS = sc.nextInt();
        AttrType[] attrType = new AttrType[COLS];

        while (sc.hasNextLine()) {
            double[] doubleArray = Arrays.stream(Arrays.stream(sc.nextLine().trim()
                    .split("\\s+"))
                    .filter(s -> !s.isEmpty())
                    .toArray(String[]::new))
                    .mapToDouble(Double::parseDouble)
                    .toArray();

            for(double x: doubleArray)
                System.out.print(x+"\t");

            System.out.println();
        }
    }


    protected void runAllTests (){
        int choice=1;

        while(choice!=5) {
            menu();

            try{
                choice= GetStuff.getChoice();

                switch(choice) {
                    case 0:
                        readData();
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
            e.printStackTrace();
            System.err.println ("Error encountered during buffer manager tests:\n");
            Runtime.getRuntime().exit(1);
        }
    }

}
