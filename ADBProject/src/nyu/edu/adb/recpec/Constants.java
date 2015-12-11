package nyu.edu.adb.recpec;

/**
 * 
 * This class is used to store all constants that are used in the application.
 * This allows for easier maintenance of constant values.
 * 
 * @author Rahul Desai, Suruchi Sharma
 *
 */
public class Constants {

  // Type of transaction
  public final static int RW = 0;
  public final static int RO = 1;

  // Type of operation
  public final static int OP_READ = 0;
  public final static int OP_WRITE = 1;
  public final static int OP_COMMIT = 2;

  //Type of lock
  public final static int NO_LOCK = 0;
  public final static int RL = 1;
  public final static int WL = 2;

  public final static int SITES = 10;
  public final static int VARIABLES = 20;

}
