package nyu.edu.adb.recpec;
import java.io.FileReader;
import java.io.IOException;
import java.util.Scanner;

/**
 * This class is the main class that initiates the processing the transactions
 * that appear in the input file. The input file needs to be provided as a
 * command line argument while invoking this class.
 * 
 * @author Rahul Desai, Suruchi Sharma
 *
 */
public class App {

  public static void main(String[] args) {
    String input = args[0];
    parseInput(input);
  }

  /**
   * @param input
   *          The input file that contains all the transactions.
   */
  private static void parseInput(String input) {
    try {
      FileReader f = new FileReader(input);
      Scanner in = new Scanner(f);
      while (in.hasNext()) {
        manager.execute(in.nextLine());
      }
      System.out.println(manager.waitList.size());
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static TransactionManager manager = null;
}
