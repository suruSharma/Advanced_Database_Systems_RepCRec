package nyu.edu.adb.recpec;

/**
 * @author Rahul Desai, Suruchi Sharma 
 * 
 * This class holds the information of an
 *         operation
 */
/**
 * @author Suruchi
 *
 */
public class Operation {

  private int value;
  private int variableIndex;
  private int time;
  private int operationType;

  public Operation(int value, int variableIndex, int time, int operationType) {
    this.value = value;
    this.variableIndex = variableIndex;
    this.time = time;
    this.operationType = operationType;
  }

  /**
   * Getter to fetch the operation type
   * 
   * @return
   */
  public int getOperationType() {
    return operationType;
  }

  /**
   * Getter to fetch the time of the operation
   * 
   * @return
   */
  public int getTime() {
    return time;
  }

  /**
   * Getter to fetch the value of the variable
   * 
   * @return
   */
  public int getValue() {
    return value;
  }

  /**
   * Getter to fetch the variable index of pertaining to the current operation
   * 
   * @return
   */
  public int getVariableIndex() {
    return variableIndex;
  }

  /**
   * @param operationType
   *          The type of operation, 0- read, 1-write, 2-commit
   */
  public void setOperationType(int operationType) {
    this.operationType = operationType;
  }

  /**
   * @param time
   *          The time of the operation
   */
  public void setTime(int time) {
    this.time = time;
  }

  /**
   * @param value
   *          The value to be assigned to the current operation
   */
  public void setValue(int value) {
    this.value = value;
  }

  /**
   * @param variableIndex
   *          The variable index associated with the current operation
   */
  public void setVariableIndex(int variableIndex) {
    this.variableIndex = variableIndex;
  }
}