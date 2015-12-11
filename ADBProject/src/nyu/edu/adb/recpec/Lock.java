package nyu.edu.adb.recpec;

/**
 * This class is used to store the lock information for a variable
 * 
 * @author Rahul Desai, Suruchi Sharma
 *
 */
public class Lock {
  private int variableID;
  private String TransactionID;
  private int lockType;

  public Lock(int v, String t, int type) {
    this.variableID = v;
    this.TransactionID = t;
    this.lockType = type;
  }

  /**
   * Getter to fetch the type of the lock, read-write/read-only
   * @return
   */
  public int getLockType() {
    return this.lockType;
  }

  /**
   * Getter to fetch the transaction ID which has acquired a lock on the variable 
   * @return
   */
  public String getTransactionId() {
    return this.TransactionID;
  }

  /**
   * Getter to fetch the variableID on which the lock is applied
   * @return
   */
  public int getVariableID() {
    return this.variableID;
  }

  /**
   * @param t
   * Set the type of the lock for the variable
   */
  public void setLockType(int t) {
    this.lockType = t;
  }

  /**
   * @param s
   * The transaction that has acquired the lock on the variable
   */
  public void setTransactionID(String s) {
    this.TransactionID = s;
  }

  /**
   * @param v
   * Variable on which the lock has to be acquired
   */
  public void setVariableID(int v) {
    this.variableID = v;
  }

  @Override
  public String toString() {
    return this.variableID + " " + this.TransactionID + " " + this.lockType;
  }
}
