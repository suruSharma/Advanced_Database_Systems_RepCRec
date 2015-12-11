package nyu.edu.adb.recpec;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author Rahul Desai, Suruchi Sharma
 *
 */
public class TransactionManager {
  private Map<Integer, Site> sites;
  private Map<String, Transaction> currentTransactions;
  private int time = 0;
  public Map<String, ArrayList<Operation>> waitList;

  //Constructor
  public TransactionManager() {
    sites = new HashMap<Integer, Site>();
    for (int i = 1; i <= Constants.SITES; i++) {
      Site site = new Site(i);
      sites.put(i, site);
    }
    currentTransactions = new HashMap<String, Transaction>();
    waitList = new HashMap<String, ArrayList<Operation>>();
  }

  /**
   * aborts the Transaction transactionID
   * 
   * @param transactionID
   */
  private void abort(String transactionID) {
    if (currentTransactions.containsKey(transactionID)) {
      Transaction t = (Transaction) currentTransactions.get(transactionID);
      ArrayList<Operation> ops = (ArrayList<Operation>) t.getOperations();
      for (int i = 0; i < ops.size(); i++) {
        if (ops.get(i).getOperationType() == Constants.OP_WRITE) {
          //This operation is a write operation
          //The variable that was written to
          //It Value has to be set to CurrentValue
          //across all sites that are up
          int variableID = ops.get(i).getVariableIndex();
          for (int j = 1; j <= Constants.SITES; j++) {
            if (!this.sites.get(j).isDown()) {
              ArrayList<Variable> tempVar = (ArrayList<Variable>) this.sites.get(j).getVariables();
              for (int p = 0; p < tempVar.size(); p++) {
                if (tempVar.get(p).getID() == variableID) {
                  this.sites.get(j).setCurrValToVal(variableID);
                }
              }
            } else {//site is down
              //if site is down do not do anything
            }
          }
        } else {
          //for read operation
          //there is no change that has to be made to the database for read operation
        }
      }

      for (int i = 1; i <= Constants.SITES && !this.sites.get(i).isDown(); i++) {
        this.sites.get(i).lockInfo.removeLockWithTransactionID(transactionID);
      }
      System.out.println("Transaction " + t.getTransactionID() + " is aborted");
      //notify the transaction that waits for this transaction to end/abort that this transaction has now aborted
      this.notifyWaitingTransaction(t.getTransactionID());

    } else {
      //transaction ID is invalid
      System.out.println(transactionID + " IS INVALID");
    }

  }

  /**
   * performs the begin operation for both Read-Write and Read-Only Transactions
   * 
   * @param transactionID
   * @param typeOfTransaction
   */
  private void beginTransaction(String transactionID, int typeOfTransaction) {
    if (!currentTransactions.containsKey(transactionID)) {
      Transaction transaction = new Transaction(transactionID, time, typeOfTransaction);
      currentTransactions.put(transactionID, transaction);
    }
  }

  /**
   * counts the number of UP Sites containing Variable variableID
   * 
   * @param variableID
   * @return
   */
  private int countUpSitesContainingVariable(int variableID) {
    int answer = 0;
    for (int i = 1; i <= Constants.SITES; i++) {
      if (!this.sites.get(i).isDown()) {
        ArrayList<Variable> vars = (ArrayList<Variable>) this.sites.get(i).getVariables();
        for (int j = 0; j < vars.size(); j++) {
          if (vars.get(j).getID() == variableID) {
            answer++;
          }
        }
      }
    }
    return answer;
  }

  /**
   * returns true if the Transaction already has a read lock on the variable
   * 
   * @param trans
   * @param variableID
   * @return
   */
  private boolean doesThisTransactionAlreadyHaveReadLockOnVariable(Transaction trans, int variableID) {
    ArrayList<Lock> locked = trans.getAlreadyLockedList();
    for (int i = 0; i < locked.size(); i++) {
      if (locked.get(i).getLockType() == Constants.RL
          && locked.get(i).getVariableID() == variableID) {
        return true;
      }
    }
    return false;
  }

  /**
   * prints values of all variables on all sites
   */
  public void dump() {
    for (int i = 1; i <= Constants.SITES; i++) {
      System.out.print("Site " + i + "\n");
      System.out.print(sites.get(i).toString());//toString() already has \n at the end
    }
    System.out.print("\n");
  }

  /**
   * prints the values at site with index i
   * 
   * @param index
   */
  public void dumpI(int index) {
    if (sites.containsKey(index)) {
      System.out.print("Site " + index + "\n");
      System.out.print(sites.get(index).toString());//toString() already has \n at the end
    }
    System.out.print("\n");
  }

  /**
   * prints value of variable i across all sites
   * 
   * @param index
   */
  public void dumpx(int index) {
    for (int i = 1; i <= Constants.SITES; i++) {
      ArrayList<Variable> var = (ArrayList<Variable>) sites.get(i).getVariables();
      for (int j = 0; j < var.size(); j++) {
        if (var.get(j).getID() == index) {
          System.out.print("Site " + i + "\n");
          System.out.print(var.get(j).toString());//toString() already has \n at the end
        }
      }
    }
    System.out.print("\n");
  }

  /**
   * performs the end operation on the given Transaction
   * 
   * @param transactionID
   */
  private void endTransaction(String transactionID) {
    if (this.currentTransactions.containsKey(transactionID)) {
      Transaction tToEnd = this.currentTransactions.get(transactionID);
      if (this.currentTransactions.get(transactionID).getTypeOfTransaction() == Constants.RO) {
        System.out.println("End read only transaction " + transactionID);
      } else if (this.currentTransactions.get(transactionID).getTypeOfTransaction() == Constants.RW) {
        boolean areAllAccessedSitesUp = true;
        for (int i = 1; i <= Constants.SITES; i++) {
          if (tToEnd.sitesAccessed.contains(i)
              && (this.sites.get(i).isDown() || !this.sites.get(i).lockInfo
                  .doesLockTableContainLockWithTransactionID(transactionID))) {
            areAllAccessedSitesUp = false;
          }
        }
        if (!areAllAccessedSitesUp) {
          //All sites that this transaction accessed have not been UP throughout
          //at least one of them failed since the Transaction transactionID accessed it
          this.abort(transactionID);
          this.notifyWaitingTransaction(transactionID);
        } else {
          System.out.println("End Read-Write Transaction " + transactionID);
          Transaction tempTransaction = (Transaction) currentTransactions.get(transactionID);
          ArrayList<Lock> lockedByTempTransaction = tempTransaction.getAlreadyLockedList();

          for (int i = 0; i < lockedByTempTransaction.size(); i++) {
            if (lockedByTempTransaction.get(i).getLockType() == Constants.WL) {
              //write lock
              int variableID = lockedByTempTransaction.get(i).getVariableID();
              for (int j = 1; j <= Constants.SITES; j++) {
                if (!this.sites.get(j).isDown()
                    && this.sites.get(j).doesSiteContainVariable(variableID)
                    && this.sites.get(j).lockInfo
                        .doesLockTableContainLockWithTransactionID(transactionID)) {
                  //site is up
                  this.sites.get(j).setValToCurrVal(variableID);
                  Variable temp = this.sites.get(j).getVariableWithID(variableID);
                  if (temp.getAvailableForRead() == false && temp != null) {
                    temp.setAvailableForRead(true);
                  }
                  ArrayList<Variable> allVars = (ArrayList<Variable>) this.sites.get(j)
                      .getVariables();
                  for (int k = 0; k < allVars.size(); k++) {
                    if (allVars.get(k).getID() == variableID) {
                      allVars.get(k).getHistoricalData().add(
                          new VariableHistory(time, allVars.get(k).getValue()));
                      this.sites.get(j).lockInfo
                          .removeLock(variableID, transactionID, Constants.WL);
                    }
                  }

                }
              }
            } else {
              //read lock
              int variableID = lockedByTempTransaction.get(i).getVariableID();
              for (int j = 1; j <= Constants.SITES; j++) {
                Site s = this.sites.get(j);
                s.lockInfo.removeLock(variableID, transactionID, Constants.RL);
                ;
              }
            }
          }
          this.notifyWaitingTransaction(transactionID);
        }
      }
      //POSTPROCESSING
      currentTransactions.remove(transactionID);
      waitList.remove(transactionID);
    }
  }

  /**
   * Function that takes line by line input and takes initiates action
   * 
   * @param line
   */
  public void execute(String line) {
    time++;
    String[] operation = line.split("; ");
    ArrayList<String> endTransactionList = new ArrayList<String>();
    for (String op : operation) {
      System.out.println(op);
      if (op.startsWith("dump()")) {
        dump();
      } else if (op.startsWith("dump(x")) {
        int index = Integer.parseInt(op.substring(6, op.length() - 1));
        dumpx(index);
      } else if (op.startsWith("dump(")) {
        int index = Integer.parseInt(op.substring(5, op.length() - 1));
        dumpI(index);
      } else if (op.startsWith("begin(")) {
        beginTransaction(op.substring(6, op.length() - 1), Constants.RW);
      } else if (op.startsWith("beginRO(")) {
        beginTransaction(op.substring(8, op.length() - 1), Constants.RO);
      } else if (op.startsWith("R(")) {
        String[] t = op.substring(2, op.length() - 1).split(",");
        readValue(t[0], Integer.parseInt(t[1].substring(t[1].indexOf("x") + 1)));
      } else if (op.startsWith("W(")) {
        String[] t = op.substring(2, op.length() - 1).split(",");
        writeValue(t[0], Integer.parseInt(t[1].substring(t[1].indexOf("x") + 1)), Integer
            .parseInt(t[2]));
      } else if (op.startsWith("end(")) {
        //endTransaction(op.substring(4, op.length() - 1));
        endTransactionList.add(op.substring(4, op.length() - 1));
      } else if (op.startsWith("fail(")) {
        failSite(op.substring(5, op.length() - 1));
      } else if (op.startsWith("recover(")) {
        recoverSite(op.substring(8, op.length() - 1));
      }
    }
    for (int i = 0; i < endTransactionList.size(); i++) {
      endTransaction(endTransactionList.get(i));
    }
  }

  /**
   * performs the fail(siteID) operation
   * 
   * @param str
   */
  private void failSite(String str) {
    int siteID = Integer.parseInt(str);
    if (this.sites.containsKey(siteID)) {
      Site s = (Site) sites.get(siteID);
      s.fail();
    } else {
      //invalid site id
      System.out.println("SUCH A SITE DOES NOT EXIST (INVALID OPERATION fail(" + str + ")");
    }
  }

  /**
   * returns an ArrayList<Lock> of all the locks on Variable variableID across
   * all sites
   * 
   * @param variableID
   * @return
   */
  private ArrayList<Lock> getAllLocksForVariableFromAllSites(int variableID) {
    ArrayList<Lock> ans = new ArrayList<>();
    for (int i = 1; i <= Constants.SITES; i++) {
      ArrayList<Lock> lockForThisSite = this.sites.get(i).lockInfo
          .getAllLocksForVariable(variableID);
      for (int j = 0; j < lockForThisSite.size(); j++) {
        ans.add(lockForThisSite.get(j));
      }
    }
    return ans;
  }

  /**
   * gets all the locks that we can get on Variable variableID across all sites
   * for Transaction transactionID
   * 
   * @param transactionID
   * @param variableID
   */
  private void getAllWriteLocksOnVariableThatYouCanGet(String transactionID, int variableID) {
    Transaction transaction = this.currentTransactions.get(transactionID);
    for (int i = 1; i <= Constants.SITES; i++) {
      if (!this.sites.get(i).isDown()) {
        Site s = this.sites.get(i);
        ArrayList<Variable> v = (ArrayList<Variable>) s.getVariables();
        boolean answer = false;
        for (int j = 0; j < v.size(); j++) {
          if (v.get(j).getID() == variableID) {
            answer = true;
          }
        }
        if (answer) {
          if (this.sites.get(i).lockInfo.doesLockTableContainLockWithVariableID(variableID)) {
            //table contains lock with given variable ID
            //Transaction transactionID cannot get write lock on the Variable variableID at this site
          } else {//table does not contain any lock with given variableID
            //we can get write lock on this site
            this.sites.get(i).lockInfo.addLock(variableID, transactionID, Constants.WL);
            transaction.addLockToAlreadyLocked(variableID, Constants.WL);
            transaction.sitesAccessed.add(i);
          }
        }
      }
    }
  }

  /**
   * gets write locks on all Up sites containing the variable, does not store
   * the lock in the Transaction's alreadyLocked List
   * 
   * @param variableID
   * @param transactionID
   */
  private void getWriteLocksOnVariable(int variableID, String transactionID) {
    for (int i = 1; i <= Constants.SITES; i++) {
      if (!this.sites.get(i).isDown()) {
        if (this.sites.get(i).doesSiteContainVariable(variableID)) {
          //site contains variable on which to get lock
          Transaction t = this.currentTransactions.get(transactionID);
          t.sitesAccessed.add(i);
          this.sites.get(i).lockInfo.addLock(variableID, transactionID, Constants.WL);
        }
      }
    }
  }

  /**
   * inserts Transaction transactionID's Operation op into the waitList
   * 
   * @param op
   * @param transactionID
   */
  private void insertToWaitList(Operation op, String transactionID) {
    ArrayList<Operation> ops;
    if (waitList.containsKey(transactionID)) {
      ops = (ArrayList<Operation>) waitList.get(transactionID);
      ops.add(op);
    } else {
      ops = new ArrayList<Operation>();
      ops.add(op);
      waitList.put(transactionID, ops);
    }
  }

  /**
   * implementation of the wait-die protocol
   * 
   * @param lockedBy
   * @param transactionID
   * @return
   */
  private boolean isAbort(String lockedBy, String transactionID) {
    if (currentTransactions.containsKey(lockedBy) && currentTransactions.containsKey(transactionID)) {
      Transaction lockedTransaction = (Transaction) currentTransactions.get(lockedBy);
      Transaction actualTransaction = (Transaction) currentTransactions.get(transactionID);
      if (lockedTransaction.getStartTime() <= actualTransaction.getStartTime()) {
        //the transaction that has the lock is the older transaction
        //so the transaction that wants the lock should abort
        abort(transactionID);
        //Since the Transaction transactionID aborted
        //We notify any other Transactions that might be waiting for it to end/abort
        this.notifyWaitingTransaction(transactionID);
      } else {
        //Transaction that has the lock is younger than the transaction that wants the lock
        //so the transaction that wants the lock(older transaction) should wait
        return false;
      }
    }
    return true;
  }

  /**
   * returns true if there is an older transaction which holds lock on Variable
   * variableID across any site
   * 
   * @param tStartTime
   * @param variableID
   * @return
   */
  private boolean isThereAnOlderTransactionWithLockOnVariable(int tStartTime, int variableID) {
    for (int i = 1; i <= Constants.SITES; i++) {
      Site tempSite = this.sites.get(i);
      ArrayList<Lock> lockForVariable = tempSite.lockInfo.getAllLocksForVariable(variableID);
      for (int j = 0; j < lockForVariable.size(); j++) {
        String currentTID = lockForVariable.get(j).getTransactionId();
        int currentStartTime = this.currentTransactions.get(currentTID).getStartTime();
        if (currentStartTime < tStartTime) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * returns true if the transaction dies or aborts Also makes transation1 wait
   * for transaction2 Implements Wait-Die Protocol
   * 
   * @param transactionID1
   * @param transactionID2
   * @return
   */
  private boolean makeTransaction1WaitForTransaction2(String transactionID1, String transactionID2) {
    if (transactionID1.equals(transactionID2)) {//both transactionID's cannot be same
      return true;
    } else {
      Transaction transaction2 = this.currentTransactions.get(transactionID2);
      Transaction transaction1 = this.currentTransactions.get(transactionID1);
      if (transaction2.transactionThatWaitsForThisTransaction == null) {
        //no transaction waits for transaction2
        //directly make transaction1 wait for transaction2
        transaction2.transactionThatWaitsForThisTransaction = transactionID1;
        transaction1.transactionsThatThisTransactionWaitsFor.add(transactionID2);
      } else if (transaction2.transactionThatWaitsForThisTransaction.equals(transaction1
          .getTransactionID())) {
        //transaction1 is already waiting on transaction2, don't do anything
      } else {
        if (transaction1.getStartTime() < transaction2.getStartTime()) {
          //t1 is older than t2 
          //some transaction waits for transaction2
          Transaction tWaitingForT2 = this.currentTransactions
              .get(transaction2.transactionThatWaitsForThisTransaction);
          if (transaction1.getStartTime() <= tWaitingForT2.getStartTime()) {
            //transaction 1 is older than tWaitingForT2
            //so transaction 1 should wait for tWaitingForT2
            makeTransaction1WaitForTransaction2(transaction1.getTransactionID(), tWaitingForT2
                .getTransactionID());
          } else {
            //transaction1 is younger than tWaitingForT2
            //transaction1 should abort
            this.abort(transaction1.getTransactionID());
            return false;
          }

        } else {
          //t1 is younger than t2
          //t1 should abort
          this.abort(transactionID1);
          return false;
        }
      }
    }
    return true;
  }

  /**
   * executes the operation from waitList of the Transaction that was waiting
   * for Transaction transactionID to end/abort
   * 
   * @param transactionID
   */
  private void notifyWaitingTransaction(String transactionID) {
    Transaction tr = this.currentTransactions.get(transactionID);
    if (tr.transactionThatWaitsForThisTransaction == null) {
      //no transaction waits for this transaction so do not do anything
    } else {
      //Transaction waitingTransaction waits for Transaction transactionID
      Transaction waitingTransaction = this.currentTransactions
          .get(tr.transactionThatWaitsForThisTransaction);
      if (this.waitList.size() > 0
          && this.waitList.containsKey(waitingTransaction.getTransactionID())) {
        waitingTransaction.transactionsThatThisTransactionWaitsFor.remove(transactionID);
        int variableThatWaitingTransactionNeeds;
        ArrayList<Operation> opList = this.waitList.get(waitingTransaction.getTransactionID());
        Operation op = opList.get(0);
        variableThatWaitingTransactionNeeds = op.getVariableIndex();
        if (op.getOperationType() == 1) {
          this.getAllWriteLocksOnVariableThatYouCanGet(waitingTransaction.getTransactionID(),
              variableThatWaitingTransactionNeeds);
        }

        if (waitingTransaction.transactionsThatThisTransactionWaitsFor.size() == 0) {
          this.waitList.remove(waitingTransaction.getTransactionID());
          if (op.getOperationType() == 0) {
            //read operation
            this.readValue(waitingTransaction.getTransactionID(), op.getVariableIndex());
          } else {//write operation
            this.writeValue(waitingTransaction.getTransactionID(), op.getVariableIndex(), op
                .getValue());
          }
        }
      }
    }
  }

  /**
   * Updates the wait for list of Transaction transactionID. Implements wait-die
   * 
   * @param transactionID
   * @param variableID
   */
  private void putThisTransactionIntoWaitListingOfTransactionThatHaveLock(String transactionID,
      int variableID) {
    Transaction transaction = this.currentTransactions.get(transactionID);
    boolean val = true;
    for (int i = 1; i <= Constants.SITES && val; i++) {
      if (!this.sites.get(i).isDown()) {
        Site site = this.sites.get(i);
        if (site.lockInfo.doesLockTableContainLockWithVariableID(variableID)) {
          for (int j = 0; j < site.lockInfo.lockTable.size() && val; j++) {
            if (site.lockInfo.lockTable.get(j).getVariableID() == variableID) {
              String str = site.lockInfo.lockTable.get(j).getTransactionId();
              Transaction otr = this.currentTransactions.get(str);
              if (otr.transactionThatWaitsForThisTransaction == null) {
                //no transaction waits for the transaction that has the lock
                otr.transactionThatWaitsForThisTransaction = transactionID;
                transaction.transactionsThatThisTransactionWaitsFor.add(str);
              } else {
                //some transaction already waits for the transaction that has the lock
                val = makeTransaction1WaitForTransaction2(transactionID, otr.getTransactionID());
              }
            }
          }
        }
      }
    }
  }

  /**
   * performs the read operation for both Read-Write and Read-Only Transactions
   * 
   * @param transactionID
   * @param variable
   * @param typeOfTransaction
   */
  private void read(String transactionID, int variable, int typeOfTransaction) {
    Transaction transactionForReading = this.currentTransactions.get(transactionID);
    if (typeOfTransaction == Constants.RO) {//for read only transaction
      boolean hasItBeenRead = false;
      for (int i = 1; i <= Constants.SITES && !hasItBeenRead; i++) {
        Site tempSite = sites.get(i);
        if (tempSite.isDown()) {
          //site is down
          //Do not do anything, go to next site
        } else {
          //site is not down
          ArrayList<Variable> variablesInTempSite = (ArrayList<Variable>) tempSite.getVariables();
          for (int j = 0; j < variablesInTempSite.size(); j++) {
            if (variablesInTempSite.get(j).getID() == variable
                && variablesInTempSite.get(j).getAvailableForRead()) {
              //variable we are looking for found in tempSite and they are available for reading
              //We have parse the VariableHistory of this Variable and choose the correct value
              ArrayList<VariableHistory> history = (ArrayList<VariableHistory>) variablesInTempSite
                  .get(j).getHistoricalData();
              int currentMax = -1;//time counter
              int maxIndex = -1;//index in the ArrayList which has max
              for (int k = 0; k < history.size(); k++) {
                if (history.get(k).getTime() > currentMax
                    && history.get(k).getTime() < transactionForReading.getStartTime()) {
                  maxIndex = k;
                  currentMax = history.get(k).getTime();
                }
              }
              if (maxIndex != -1) {
                hasItBeenRead = true;
                System.out.println("The value that is read is " + history.get(maxIndex).getValue());
                Operation op = new Operation(0, variable, 0, Constants.OP_READ);
                transactionForReading.insertOperation(op);
              }
            }
          }
        }
      }
      if (hasItBeenRead == false) {
        //variable could not be read for any site so this operation has to wait
        System.out.println(transactionID + "(R,x" + variable + ") has to wait");
        Operation op = new Operation(0, variable, 0, Constants.OP_READ);
        insertToWaitList(op, transactionID);
      }
    } else {//for read-write transaction

      if (this.doesThisTransactionAlreadyHaveReadLockOnVariable(transactionForReading, variable)) {
        //transaction already has a read lock on variable at some site
        int index = -1;
        for (int q = 1; q <= Constants.SITES; q++) {
          if (!this.sites.get(q).isDown()) {
            if (this.sites.get(q).lockInfo.doesThisTableContainLockWith(variable, transactionID,
                Constants.RL)) {
              index = q;
            }
          }
        }
        ArrayList<Variable> varsInSite = (ArrayList<Variable>) this.sites.get(index).getVariables();
        Variable var = varsInSite.get(0);
        for (int m = 0; m < varsInSite.size(); m++) {
          if (varsInSite.get(m).getID() == variable) {
            var = varsInSite.get(m);
          }
        }
        System.out.println("The value that is read is " + var.getValue());
      } else {
        //the transaction does not have a read lock on this variable
        boolean hasItBeenRead = false;
        for (int i = 1; i <= Constants.SITES && !hasItBeenRead; i++) {
          Site tempSite = sites.get(i);
          if (tempSite.isDown()) {
            //site is down
            //Do not do anything
          } else {
            //site is not down
            ArrayList<Variable> variablesInTempSite = (ArrayList<Variable>) tempSite.getVariables();
            for (int j = 0; j < variablesInTempSite.size(); j++) {
              if (variablesInTempSite.get(j).getID() == variable
                  && variablesInTempSite.get(j).getAvailableForRead()) {
                //variable we are looking for found in tempSite and they are available for reading(in term of recovery)
                if (tempSite.lockInfo.canWeGetReadLockOnVariable(variable, transactionID)) {
                  //we can get read lock on the variable
                  if (tempSite.lockInfo.doesThisTableContainLockWith(variable, transactionID,
                      Constants.WL)) {
                    //this transaction has a write lock on the this variable at this site
                    hasItBeenRead = true;
                    Variable currentVariable = tempSite.getVariableWithID(variable);
                    Operation op = new Operation(0, variable, 0, Constants.OP_READ);
                    transactionForReading.insertOperation(op);
                    transactionForReading.sitesAccessed.add(i);
                    System.out.println("The value that is read is "
                        + currentVariable.getCurrValue());
                    //value read is the current value because transaction already has read lock on the variable
                  } else {
                    //this transaction does not have a write lock on this variable
                    hasItBeenRead = true;
                    tempSite.lockInfo.addLock(variable, transactionID, Constants.RL);
                    Variable currentVariable = tempSite.getVariableWithID(variable);
                    Operation op = new Operation(0, variable, 0, Constants.OP_READ);
                    transactionForReading.insertOperation(op);
                    transactionForReading.sitesAccessed.add(i);
                    transactionForReading.addLockToAlreadyLocked(variable, Constants.RL);
                    System.out.println("The value that is read is " + currentVariable.getValue());
                  }
                } else {
                  //this is the variable that we want to lock
                  //but we can't get a read lock on it
                  //so implement wait-die
                  hasItBeenRead = true;
                  ArrayList<Lock> tempList = tempSite.lockInfo.getAllLocksForVariable(variable);
                  Lock writeLock = tempList.get(0);
                  String tidWithWriteLock = writeLock.getTransactionId();
                  String tidWithoutWriteLock = transactionID;
                  boolean answer = isAbort(tidWithWriteLock, tidWithoutWriteLock);
                  if (answer == false) {
                    //This operation has to wait
                    this.putThisTransactionIntoWaitListingOfTransactionThatHaveLock(transactionID,
                        variable);
                    Operation op = new Operation(0, variable, 0, Constants.OP_READ);
                    insertToWaitList(op, transactionID);
                    System.out.println(transactionID + "(R,x" + variable + ") has to wait");
                  }
                }
              }
            }
          }
        }
        if (hasItBeenRead == false) {
          //variable could not be read for any site so this operation has to wait
          Operation op = new Operation(0, variable, 0, Constants.OP_READ);
          insertToWaitList(op, transactionID);
          System.out.println(transactionID + "(R,x" + variable + ") has to wait");
        }
      }
    }
  }

  /**
   * This method acts as intermediary between execute and read() calls the
   * read() with appropriate value for typeOfTransaction
   * 
   * @param transactionID
   * @param variable
   */
  private void readValue(String transactionID, int variable) {
    if (currentTransactions.containsKey(transactionID)) {
      Transaction t = (Transaction) currentTransactions.get(transactionID);
      if (t.getTypeOfTransaction() == Constants.RO) {
        read(transactionID, variable, Constants.RO);
      } else {
        read(transactionID, variable, -1);
      }
    }
  }

  /**
   * performs the recover(siteID) operation
   * 
   * @param siteID
   */
  private void recoverSite(String siteID) {
    int site = Integer.parseInt(siteID);
    if (this.sites.containsKey(site)) {
      this.sites.get(site).recover();
    } else {
      //No such site
      System.out.println("INVALID operation recover(" + siteID + ")");
    }
  }

  /**
   * returns the total number of locks on Variable variableID across all sites
   * 
   * @param variableID
   * @return
   */
  public int totalNumberOfLocksOnAllSitesForVariable(int variableID) {
    int answer = 0;
    for (int i = 1; i <= Constants.SITES; i++) {
      answer += sites.get(i).lockInfo.getAllLocksForVariable(variableID).size();
    }
    return answer;
  }

  /**
   * returns number of sites that are UP and which contain Variable variableID
   * 
   * @param variableID
   * @return
   */
  private int upSiteCountContainingVariable(int variableID) {
    int answer = 0;
    for (int i = 1; i <= Constants.SITES; i++) {
      if (!this.sites.get(i).isDown()) {
        //site is not down
        ArrayList<Variable> vList = (ArrayList<Variable>) this.sites.get(i).getVariables();
        for (int j = 0; j < vList.size(); j++) {
          if (vList.get(j).getID() == variableID) {
            answer++;
          }
        }
      } else {
        //site is down
      }
    }
    return answer;
  }

  /**
   * performs the actual write operation on appropriate sites
   * 
   * @param variableID
   * @param val
   * @param transactionID
   */
  private void writeToSites(int variableID, int val, String transactionID) {
    for (int i = 1; i <= Constants.SITES; i++) {
      if (this.sites.get(i).doesSiteContainVariable(variableID) && !this.sites.get(i).isDown()
          && this.sites.get(i).lockInfo.doesLockTableContainLockWithTransactionID(transactionID)) {
        this.sites.get(i).writeToSite(variableID, val);
      }
    }
  }

  /**
   * @param transactionID
   * @param variableID
   * @param value
   * 
   *          Write the value to a variable
   */
  public void writeValue(String transactionID, int variableID, int value) {
    Transaction transaction;
    if (currentTransactions.containsKey(transactionID)) {
      transaction = (Transaction) currentTransactions.get(transactionID);
      if (transaction.getTypeOfTransaction() == Constants.RW) {
        if (transaction.doesTransactionAlreadyHaveAWriteLockOnVariable(variableID)) {
          //Transaction transactionID has at least one write lock for Variable variableID
          int numberOfLocksWithTransactionForVariable = transaction
              .getNumberOfLocksWithTransaction(Constants.WL, variableID);
          int upSiteContainingVariable = this.countUpSitesContainingVariable(variableID);
          if (upSiteContainingVariable == numberOfLocksWithTransactionForVariable) {
            //Transaction transactionID has all the write locks that it needs
            //perform teh write operation
            this.writeToSites(variableID, value, transactionID);
            Operation op = new Operation(value, variableID, this.time, Constants.OP_WRITE);
            transaction.insertOperation(op);
          } else {
            //Transaction transactionID does not have all the write locks on Variable variableID
            this.getAllWriteLocksOnVariableThatYouCanGet(transactionID, variableID);
            this.putThisTransactionIntoWaitListingOfTransactionThatHaveLock(transactionID,
                variableID);
          }
        } else {
          //it does not have any write locks on the variable
          int count = this.upSiteCountContainingVariable(variableID);
          if (count > 0) {
            //there are sites that are UP and that contain the variable to be written to
            boolean areThereOlderTransactions = this.isThereAnOlderTransactionWithLockOnVariable(
                transaction.getStartTime(), variableID);
            if (areThereOlderTransactions) {
              //there are older transactions holding locks on the variable
              //abort this transaction
              this.abort(transactionID);
              this.notifyWaitingTransaction(transactionID);
            } else {
              //there are no older transactions
              int numberOfLocks = this.totalNumberOfLocksOnAllSitesForVariable(variableID);
              if (numberOfLocks == 0) {
                //there are no locks on the variable on any site
                //so write operation can be performed
                this.getWriteLocksOnVariable(variableID, transactionID);
                transaction.addLockToAlreadyLocked(variableID, Constants.WL);
                this.writeToSites(variableID, value, transactionID);
                Operation op = new Operation(value, variableID, this.time, Constants.OP_WRITE);
                transaction.insertOperation(op);
              } else if (numberOfLocks == 1) {
                //the number of locks is 1
                //but it maybe the same transaction that has the lock on the variable
                ArrayList<Lock> allLocks = this.getAllLocksForVariableFromAllSites(variableID);
                if (allLocks.get(0).getTransactionId().equals(transactionID)
                    && allLocks.get(0).getLockType() == Constants.RL) {
                  //same transaction has lock on the variable
                  //delete this lock on this variable variable from the site
                  //and get a write lock on all sites
                  for (int p = 1; p <= Constants.SITES; p++) {
                    this.sites.get(p).lockInfo.removeLock(variableID, transactionID, Constants.RL);
                  }
                  transaction.removeLockFromAlreadyLocked(variableID);
                  this.getWriteLocksOnVariable(variableID, transactionID);
                  transaction.addLockToAlreadyLocked(variableID, Constants.WL);
                  this.writeToSites(variableID, value, transactionID);
                  Operation op = new Operation(value, variableID, this.time, Constants.OP_WRITE);
                  transaction.insertOperation(op);
                } else {
                  //different transaction has lock on the variable
                  this.putThisTransactionIntoWaitListingOfTransactionThatHaveLock(transactionID,
                      variableID);
                  Operation op = new Operation(value, variableID, time, Constants.OP_WRITE);
                  insertToWaitList(op, transactionID);
                  this.getAllWriteLocksOnVariableThatYouCanGet(transactionID, variableID);
                  System.out.println("W(" + transactionID + "," + "," + variableID + "," + value
                      + ")" + " has to wait");
                }
              } else {
                //there are locks on the variable on some sites
                //so the operation has to wait
                this.putThisTransactionIntoWaitListingOfTransactionThatHaveLock(transactionID,
                    variableID);
                Operation op = new Operation(value, variableID, time, Constants.OP_WRITE);
                insertToWaitList(op, transactionID);
                this.getAllWriteLocksOnVariableThatYouCanGet(transactionID, variableID);
                System.out.println("W(" + transactionID + "," + "," + variableID + "," + value
                    + ")" + " has to wait");
              }
            }
          } else {
            //all sites containing the given variable are down
            //The operation has to wait
            this.putThisTransactionIntoWaitListingOfTransactionThatHaveLock(transactionID,
                variableID);
            System.out.println("W(" + transactionID + "," + "," + variableID + "," + value + ")"
                + " has to wait");
            Operation op = new Operation(0, variableID, 0, Constants.OP_READ);
            insertToWaitList(op, transactionID);
            this.getAllWriteLocksOnVariableThatYouCanGet(transactionID, variableID);
          }
        }
      }
    }
  }
}