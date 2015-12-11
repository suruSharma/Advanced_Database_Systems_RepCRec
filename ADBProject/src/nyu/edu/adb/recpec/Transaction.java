package nyu.edu.adb.recpec;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Rahul Desai, Suruchi Sharma Class to hold all information pertaining
 *         to a transaction
 *
 */
public class Transaction {

  private String transactionID;
  private int startTime;
  private int typeOfTransaction;
  private List<Operation> operations;
  public ArrayList<Lock> alreadyLocked;
  public Set<Integer> sitesAccessed;
  public String transactionThatWaitsForThisTransaction;
  public Set<String> transactionsThatThisTransactionWaitsFor;

  public Transaction(String transactionID, int startTime, int typeOfTransaction) {
    this.transactionID = transactionID;
    this.startTime = startTime;
    this.typeOfTransaction = typeOfTransaction;
    this.operations = new ArrayList<Operation>();
    this.alreadyLocked = new ArrayList<Lock>();
    this.sitesAccessed = new HashSet<Integer>();
    transactionsThatThisTransactionWaitsFor = new HashSet<String>();
    transactionThatWaitsForThisTransaction = null;
  }

  /**
   * @param variableID
   * ID of the variable
   * @param type
   * Type of the lock
   */
  public void addLockToAlreadyLocked(int variableID, int type) {
    Lock temp = new Lock(variableID, this.transactionID, type);
    this.alreadyLocked.add(temp);
  }

  /**
   * @param variableID
   * Variable ID
   * @return
   */
  public boolean doesTransactionAlreadyHaveAWriteLockOnVariable(int variableID) {
    for (int i = 0; i < this.alreadyLocked.size(); i++) {
      if (this.alreadyLocked.get(i).getLockType() == Constants.WL
          && this.alreadyLocked.get(i).getVariableID() == variableID) {
        return true;
      }
    }
    return false;
  }

  /**
   * Method to get the locked list
   * @return
   */
  public ArrayList<Lock> getAlreadyLockedList() {
    return this.alreadyLocked;
  }

  /**
   * @param type
   * Type of the lock
   * @param variableID
   * Variable to be checked
   * @return
   */
  public int getNumberOfLocksWithTransaction(int type, int variableID) {
    int number = 0;
    for (int i = 0; i < this.alreadyLocked.size(); i++) {
      if (this.alreadyLocked.get(i).getVariableID() == variableID
          && this.alreadyLocked.get(i).getLockType() == type) {
        number++;
      }
    }
    return number;
  }

  /**
   * @return list of operations
   */
  public List<Operation> getOperations() {
    return operations;
  }

  /**
   * @return the start time of the transaction
   */
  public int getStartTime() {
    return startTime;
  }

  
  /**
   * @return the transactionID
   */
  public String getTransactionID() {
    return transactionID;
  }

  /**
   * @return type of transaction
   */
  public int getTypeOfTransaction() {
    return typeOfTransaction;
  }

  /**
   * @param op
   * Add the operation to the list of transactions
   */
  public void insertOperation(Operation op) {
    this.operations.add(op);

  }

  /**
   * @param variableID
   * Varianle ID whose lock has to be removed
   */
  public void removeLockFromAlreadyLocked(int variableID) {
    int index = -1;
    for (int i = 0; i < this.alreadyLocked.size(); i++) {
      if (this.alreadyLocked.get(i).getVariableID() == variableID) {
        index = i;
      }
    }
    if (index != -1) {
      this.alreadyLocked.remove(index);
    }
  }

  /**
   * @param operations
   * List of operations for a transaction
   */
  public void setOperations(List<Operation> operations) {
    this.operations = operations;
  }

  /**
   * @param startTime
   * Start time of a transaction
   */
  public void setStartTime(int startTime) {
    this.startTime = startTime;
  }

  /**
   * @param transactionID
   * Set the transaction ID
   */
  public void setTransactionID(String transactionID) {
    this.transactionID = transactionID;
  }

  /**
   * @param typeOfTransaction
   * Set the type of the transaction
   */
  public void setTypeOfTransaction(int typeOfTransaction) {
    this.typeOfTransaction = typeOfTransaction;
  }

  @Override
  public String toString() {
    StringBuilder value = new StringBuilder();
    value.append("Transaction ID-> " + this.transactionID + "\n");
    value.append("Transaction Type-> " + this.typeOfTransaction + "\n");
    value.append("Start time-> " + this.startTime + "\n");
    return value.toString();
  }
}
