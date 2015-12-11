package nyu.edu.adb.recpec;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Rahul Desai, Suruchi Sharma This class holds the information requried
 *         for a Site
 */
public class Site {
  private int ID;
  private List<Variable> variables;
  private boolean down;
  public LockTable lockInfo;

  public Site(int id) {
    this.ID = id;
    variables = new ArrayList<Variable>();
    initializeSite();
    lockInfo = new LockTable();
    down = false;
  }

  /**
   * @param transactionID
   *          Transaction to be aborted
   */
  public void abortTransaction(String transactionID) {
    for (int i = 0; i < this.lockInfo.lockTable.size(); i++) {
      if (this.lockInfo.lockTable.get(i).getTransactionId() == transactionID) {
        this.setCurrValToVal(this.lockInfo.lockTable.get(i).getVariableID());
      }
    }
  }

  /**
   * @param variableID
   *          Method to check the is the variableID exists on the site
   * @return
   */
  public boolean doesSiteContainVariable(int variableID) {
    ArrayList<Variable> list = new ArrayList<Variable>();
    list = (ArrayList<Variable>) this.getVariables();
    for (int i = 0; i < list.size(); i++) {
      if (list.get(i).getID() == variableID) {
        return true;
      }
    }
    return false;
  }

  /**
   * Mrthod to fail a site
   */
  public void fail() {
    System.out.println("Setting down flag of site " + this.getID() + " to true");
    this.down = true;
    System.out.println("Now we have to set the currVal of all variable to val");
    for (int i = 0; i < this.lockInfo.lockTable.size(); i++) {
      getVariableWithID(this.lockInfo.lockTable.get(i).getVariableID()).setCurrValue(
          getVariableWithID(this.lockInfo.lockTable.get(i).getVariableID()).getValue());
    }
    System.out.println("Deleting all entries from lock table of site " + this.getID());
    System.out.println("Size of locktable is " + this.lockInfo.lockTable.size());
    this.lockInfo.lockTable.clear();
    System.out.println("Size of locktable becomes " + this.lockInfo.lockTable.size());
  }

  /**
   * Getter to fetch the ID of a site
   * 
   * @return
   */
  public int getID() {
    return this.ID;
  }

  /**
   * List fo variables in a site
   * 
   * @return
   */
  public List<Variable> getVariables() {
    return this.variables;
  }

  /**
   * @param variableID
   *          Fetch the Variable object for the given variableID
   * @return
   */
  public Variable getVariableWithID(int variableID) {
    ArrayList<Variable> list = new ArrayList<Variable>();
    list = (ArrayList<Variable>) this.getVariables();
    for (int i = 0; i < list.size(); i++) {
      if (list.get(i).getID() == variableID) {
        return list.get(i);
      }
    }
    return null;
  }

  /**
   * This method is used to initialize the variables that are present on a site
   */
  public void initializeSite() {
    for (int i = 1; i <= Constants.VARIABLES; i++) {
      if (i % 2 == 0) {//index of variable is even
        Variable var = new Variable(i);
        this.variables.add(var);
      } else {//index of variable is odd
        if (this.ID == ((1 + i) % 10)) {
          Variable var = new Variable(i);
          this.variables.add(var);
        }
        if (i % 10 == 9 && this.ID % 10 == 0) {
          Variable var = new Variable(i);
          this.variables.add(var);
        }
      }
    }
  }

  /**
   * Method to check if a site is down
   * 
   * @return
   */
  public boolean isDown() {
    return this.down;
  }

  /**
   * Method to recover a failed site
   */
  public void recover() {
    if (this.isDown()) {
      System.out.println("Site " + this.getID() + " is down");
      System.out.println("Setting down flag of site " + this.getID() + " to false");
      this.down = false;
      System.out.println("Setting only exclusive variables as available to read");
      System.out.println("Non-exclusive variables should not be available to read");
      ArrayList<Variable> var = (ArrayList<Variable>) this.getVariables();
      for (int i = 0; i < var.size(); i++) {
        if (var.get(i).isCopied()) {
          //this variable is not exclusive
          var.get(i).setAvailableForRead(false);
        } else {
          //this variable is exclusive
          var.get(i).setAvailableForRead(true);
        }
      }
    } else {
      System.out.println("Site " + this.getID() + " is NOT down, call to recover it is invalid");
    }

  }

  /**
   * @param variableID
   *          Set the current value to the final value of the variable
   */
  public void setCurrValToVal(int variableID) {
    ArrayList<Variable> list = (ArrayList<Variable>) this.getVariables();
    for (int i = 0; i < list.size(); i++) {
      if (list.get(i).getID() == variableID) {
        list.get(i).setCurrValue(list.get(i).getValue());
      }
    }
  }

  /**
   * @param variableID
   *          Method to set the final value to the current(temporary) value
   */
  public void setValToCurrVal(int variableID) {
    ArrayList<Variable> list = (ArrayList<Variable>) this.getVariables();
    for (int i = 0; i < list.size(); i++) {
      if (list.get(i).getID() == variableID) {
        list.get(i).setValue(list.get(i).getCurrValue());
      }
    }
  }

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < variables.size(); i++) {
      sb.append("x").append(variables.get(i).getID()).append(" = ").append(
          variables.get(i).getValue()).append("\n");
    }
    return sb.toString();
  }

  /**
   * Enhanced toString
   * 
   * @return
   */
  public String toStringPlusExtraInfo() {
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < variables.size(); i++) {
      sb.append("x").append(variables.get(i).getID()).append(" = ").append(
          variables.get(i).getValue()).append(variables.get(i).getAvailableForRead()).append(
          variables.get(i).getCurrValue()).append("\n");
    }
    return sb.toString();
  }

  /**
   * @param variableID
   *          VariableID whose value is to be set
   * @param value
   *          The value of the variableID
   */
  public void writeToSite(int variableID, int value) {
    ArrayList<Variable> tempVariable = (ArrayList<Variable>) this.getVariables();
    for (int i = 0; i < tempVariable.size(); i++) {
      if (tempVariable.get(i).getID() == variableID) {
        tempVariable.get(i).setCurrValue(value);
        //System.out.println(this.getVariables().get(i).getCurrValue()+"is currvalue");
      }
    }
  }
}
