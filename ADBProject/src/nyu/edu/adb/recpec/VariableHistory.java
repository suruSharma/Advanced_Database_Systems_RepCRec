package nyu.edu.adb.recpec;

/**
 * @author Rahul Desai, Suruchi Sharma Class to hold the historical values for a
 *         variable
 *
 */
public class VariableHistory {
  private int time;
  private int value;

  public VariableHistory(int time, int value) {
    this.time = time;
    this.value = value;
  }

  public int getTime() {
    return time;
  }

  public int getValue() {
    return value;
  }

  public void setTime(int time) {
    this.time = time;
  }

  public void setValue(int value) {
    this.value = value;
  }

  @Override
  public String toString() {
    return ("|" + this.time + "," + this.value + "|");
  }
}