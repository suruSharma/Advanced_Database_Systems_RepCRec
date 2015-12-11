package nyu.edu.adb.recpec;

import java.util.ArrayList;

/**
 * @author Rahul Desai, Suruchi Sharma This class holds the list of locks that
 *         denotes the lock table.
 */
/**
 * @author Suruchi
 *
 */
/**
 * @author Suruchi
 *
 */
public class LockTable {
  public ArrayList<Lock> lockTable;

  public LockTable() {
    lockTable = new ArrayList<Lock>();
  }

  /**
   * @param variable
   *          The variable on which the lock is acquired
   * @param transactionID
   *          The transaction that acquries the lock
   * @param lockType
   *          The type of lock that is acquired. 1-Read lock, 2- Write lock
   */
  public void addLock(int variable, String transactionID, int lockType) {
    Lock temp = new Lock(variable, transactionID, lockType);
    this.lockTable.add(temp);
  }

  /**
   * @param variableID
   *          The variable on which a read lock is desired
   * @param transactionID
   *          The transaction which wishes to acqurie a read lock
   * @return
   */
  public boolean canWeGetReadLockOnVariable(int variableID, String transactionID) {
    ArrayList<Lock> locks = this.getAllLocksForVariable(variableID);
    if (locks.size() == 0) {
      return true;
    }
    boolean hasWriteLock = false;
    for (int i = 0; i < locks.size(); i++) {
      if (locks.get(i).getLockType() == Constants.WL
          && !locks.get(i).getTransactionId().equals(transactionID)) {
        hasWriteLock = true;
      }
    }
    if (hasWriteLock) {
      return false;
    } else {
      return true;
    }
  }

  /**
   * @param variableID
   *          The variable whose lock has to be changed from read to write
   * @param transactionID
   *          The transaction ID acquiring the lock
   */
  public void convertReadLockToWriteLock(int variableID, String transactionID) {
    for (int i = 0; i < this.lockTable.size(); i++) {
      if (this.lockTable.get(i).getTransactionId().equals(transactionID)
          && this.lockTable.get(i).getVariableID() == variableID) {
        if (this.lockTable.get(i).getLockType() == Constants.RL) {
          this.lockTable.get(i).setLockType(Constants.WL);
        }
      }
    }
  }

  /**
   * @param transactionID
   *          TransactionID to be checked for
   * @return
   */
  public boolean doesLockTableContainLockWithTransactionID(String transactionID) {
    for (int i = 0; i < this.lockTable.size(); i++) {
      if (this.lockTable.get(i).getTransactionId().equals(transactionID)) {
        return true;
      }
    }
    return false;
  }

  /**
   * @param variableID
   *          Variable ID to be checked for
   * @return
   */
  public boolean doesLockTableContainLockWithVariableID(int variableID) {
    for (int i = 0; i < this.lockTable.size(); i++) {
      if (this.lockTable.get(i).getVariableID() == variableID) {
        return true;
      }
    }
    return false;
  }

  /**
   * @param variableID
   *          The variableID to be checked
   * @param transactionID
   *          The transactionID to be checkhed
   * @param type
   *          The lock type, 1:Read Lock, 2 : write lock
   * @return
   */
  public boolean doesThisTableContainLockWith(int variableID, String transactionID, int type) {
    for (int i = 0; i < this.lockTable.size(); i++) {
      if (this.lockTable.get(i).getVariableID() == variableID
          && this.lockTable.get(i).getTransactionId().equals(transactionID)
          && this.lockTable.get(i).getLockType() == type) {
        return true;
      }
    }
    return false;
  }

  /**
   * @param variableID
   *          Variable for which locks are to be acquired
   * @return
   */
  public ArrayList<Lock> getAllLocksForVariable(int variableID) {
    ArrayList<Lock> answer = new ArrayList<Lock>();
    for (int i = 0; i < this.lockTable.size(); i++) {
      if (this.lockTable.get(i).getVariableID() == variableID) {
        answer.add(this.lockTable.get(i));
      }
    }
    return answer;
  }

  /**
   * @param variableID
   *          The variableID for which the read lock is to be acquired
   * @param transactionID
   *          The transaction that acquires the lock
   */
  public void getReadLockOnVariable(int variableID, String transactionID) {
    if (this.canWeGetReadLockOnVariable(variableID, transactionID)) {
      this.addLock(variableID, transactionID, Constants.RL);
    } else {
      return;
    }
  }

  /**
   * @param variableID
   *          The variable ID that is to be freed from the lock
   * @param transactionID
   *          The transaction that formerly held the lock
   * @param lockType
   *          The type of lock held
   */
  public void removeLock(int variableID, String transactionID, int lockType) {
    int index = -1;
    for (int i = 0; i < this.lockTable.size(); i++) {
      if (this.lockTable.get(i).getVariableID() == variableID
          && this.lockTable.get(i).getTransactionId().equals(transactionID)
          && this.lockTable.get(i).getLockType() == lockType) {
        index = i;
      }
    }
    if (index != -1) {
      this.lockTable.remove(index);
    } else {
    }
  }

  /**
   * @param transactionID
   *          The transaction ID for which the locks have to be released
   */
  public void removeLockWithTransactionID(String transactionID) {
    int i = 0;
    while (i < this.lockTable.size()) {
      if (this.lockTable.get(i).getTransactionId().equals(transactionID)) {
        this.lockTable.remove(i);
      } else {
        i++;
      }
    }
  }

  @Override
  public String toString() {
    StringBuilder answer = new StringBuilder();
    for (int i = 0; i < this.lockTable.size(); i++) {
      answer.append(this.lockTable.get(i) + "\n");
    }

    return answer.toString();
  }

}
