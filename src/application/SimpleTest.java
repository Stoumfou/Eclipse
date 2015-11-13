package application;

import java.util.ArrayList;
import java.util.List;

import services.DataStoreManager;

/**
 * A simple test program for the {@link DataStoreManager}.
 *
 * @author Jean-Michel Busca
 *
 */
public class SimpleTest {

  //
  // CONSTANTS
  //
  private static final int MAX_ACCOUNTS = 10;
  private static final int MAX_CUSTOMERS = 5;

  //
  // CLASS FIELDS
  //
  private static int testTotal = 0;
  private static int testOK = 0;

  //
  // HELPER CLASSES
  //
  /**
   * Emulates a user performing bank operations. These operations are defined in
   * the {@link #run()} method.
   * <p>
   * This class is used to perform multi-user tests. See the
   * {@link SimpleTest#main(String[])} method.
   *
   * @author Jean-Michel Busca
   *
   */
  static class CustomerEmulator extends Thread {

    private final DataStoreManager manager;
    private final String user;

    /**
     * Creates a new user emulator with the specified name, using the specified
     * data store manager.
     * <p>
     * Note: the data store manager must be dedicated to the user (no two users
     * share the same data store manager object).
     *
     * @param manager
     *          the manager to use
     * @param user
     *          the name of the user running the test
     */
    public CustomerEmulator(DataStoreManager manager, String user) {
      this.manager = manager;
      this.user = user;
    }

    @Override
    public String toString() {
      return user + "[" + manager + "]";
    }

    @Override
    public void run() {
      System.out.println(this + ": starting");

      // TODO complete the test

      System.out.println(this + ": exiting");
    }

  }

  //
  // HELPER METHODS
  //
  /**
   * Checks whether the specified test was successful and updates the fields
   * <code>testTotal</code> and <code>testOK</code> accordingly.
   *
   * @param test
   *          the name of the test
   * @param ok
   *          <code>true</code> if the test was sucessful and <code>false</code>
   *          otherwise
   */
  private synchronized static void check(String test, boolean ok) {
    testTotal += 1;
    System.out.print(test + ": ");
    if (ok) {
      testOK += 1;
      System.out.println("ok");
    } else {
      System.out.println("FAILED");
    }
  }

  /**
   * Runs a single-user test suite on the specified data store manager, on
   * behalf of the specified user.
   *
   * @param manager
   *          the manager to test
   * @param user
   *          the name of the user running the test
   * @throws Exception
   *           if anything goes wrong
   */
  private static void singleUserTests(DataStoreManager manager, String user)
          throws Exception {

    // TODO complete the test

  }

  //
  // MAIN
  //
  /**
   * Runs the simple test program.
   *
   * @param args
   *          url login password
   *          <p>
   *          to be specified in Eclipse:<br>
   *          Run/Run Configurations.../Arguments/Program arguments
   */
  public static void main(String[] args) {

    // check parameters
    if (args.length != 3) {
      System.err.println("usage: SimpleTest <url> <login> <password>");
      System.exit(1);
    }
    System.out.println(args[0]+' '+args[1]+' '+args[2]);

    DataStoreManager manager = null;
    List<DataStoreManager> managers = new ArrayList<DataStoreManager>();
    try {

      // create the data store manager
      manager = new DataStoreManager(args[0], args[1], args[2]);

      // create and populate the database
      manager.createDB();
      for (int i = 0; i < MAX_ACCOUNTS; i++) {
        manager.createAccount(i + 1);
      }

      // execute single-user tests
      System.out.println("Running single-user tests...");
      singleUserTests(manager, "single user");

      // execute multi-users tests
      System.out.println("Running multi-users tests...");
      List<CustomerEmulator> emulators = new ArrayList<CustomerEmulator>();
      for (int i = 0; i < MAX_CUSTOMERS; i++) {
        DataStoreManager manager2 = new DataStoreManager(args[0], args[1],
                args[2]);
        managers.add(manager2);
        CustomerEmulator emulator = new CustomerEmulator(manager2, "user#" + i);
        emulators.add(emulator);
        emulator.start();
      }

      // wait for the test to complete
      for (CustomerEmulator e : emulators) {
        e.join();
      }

    } catch (Exception e) {

      System.err.println("test aborted: " + e);
      e.printStackTrace();

    } finally {

      if (manager != null) {
        try {
          manager.close();
        } catch (Exception e) {
          System.err.println("unexpected exception: " + e);
        }
      }

      if (managers != null) {
        for (DataStoreManager m : managers) {
          try {
            m.close();
          } catch (Exception e) {
            System.err.println("unexpected exception: " + e);
          }
        }
      }

    }

    // print test results
    if (testTotal == 0) {
      System.out.println("no test performed");
    } else {
      String r = "test results: ";
      r += "total=" + testTotal;
      r += ", ok=" + testOK + " (" + ((testOK * 100) / testTotal) + "%)";
      System.out.println(r);
    }

  }
}
