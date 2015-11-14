package services;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.List;

import com.mysql.jdbc.PreparedStatement;

//import com.mysql.jdbc.Statement;

import model.Operation;

/**
 * Provides the application with high-level methods to access the persistent
 * data store. The class hides the fact that data is stored in a RDBMS and all
 * the complex SQL machinery required to access it.
 * <p>
 * <b>Note: DO NOT alter this class' interface.</b>
 *
 * @author Jean-Michel Busca
 *
 */
public class DataStoreManager {

  //
  // CLASS FIELDS
	private static Connection con = null;    
	private static Statement stm = null;
	private static ResultSet res = null;

  // ...

  // example of a create table statement executed by createDB()
  private static final String CREATE_TABLE_DUMMY = "create table DUMMY ("
          + "ATT int, " + "primary key (ATT)" + ")";

  /**
   * Creates a new <code>DataStoreManager</code> object that connects to the
   * specified database, using the specified login and password.
   * <p>
   * The constructor creates a dedicated SQL connection to the database. This
   * connection will later be used to execute the SQL statements required by
   * high-level methods.
   *
   * @param url
   *          the url of the database to connect to
   * @param user
   *          the login to use
   * @param password
   *          the password
   * @throws DataStoreException
   *           if an unrecoverable error occurs
   */
  public DataStoreManager(String url, String user, String password)
          throws DataStoreException {
    // TODO Auto-generated method stub
	  try {
		con = DriverManager.getConnection(url,user,password);
		stm = con.createStatement();
	} catch (SQLException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
  }

  /**
   * Creates the schema of the bank's database. This includes all the schema
   * elements: tables, triggers, views, etc. If the database already exists,
   * this method first deletes it using "drop" statements. The database is empty
   * after this method returns.
   * <p>
   * The method executes a sequence of hard-coded SQL statements, as shown
   * above.
   *
   * @throws DataStoreException
   *           if an unrecoverable error occurs
   */
  public void createDB() throws DataStoreException {
    // TODO Auto-generated method stub
	  //String sql = CREATE_TABLE_DUMMY;

	  String sqlCreateDB = "drop table OPERATION;"
	  		+ "drop table ACCOUNT;"
	  		+ "create table ACCOUNT               ("
	  		+ "ACCNUMBER INT NOT NULL,"
	  		+ "SOLDE DOUBLE CHECK(solde>=0),"
	  		+ "constraint ACCOUNT_PK primary key (ACCNUMBER));"
	  		+ "create table OPERATION ("
	  		+ "OPNUMBER int,"
	  		+ "ACCNUMBER int,"
	  		+ "BALANCE double,"
	  		+ "OPDATE date not null,"
	  		+ "constraint OPERATION_PK primary key(OPNUMBER),"
	  		+ "constraint OPERATION_FK_ACCNUMBER foreign key (ACCNUMBER) references ACCOUNT(ACCNUMBER));";
	  try {
		PreparedStatement ppStm = (PreparedStatement) con.prepareStatement(sqlCreateDB);
		ppStm.executeUpdate(sqlCreateDB);
		System.out.println("Tables créées");
	} catch (SQLException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	  
  }

  /**
   * Creates a new account with the specified number. This number uniquely
   * identifies bank accounts.
   *
   * @param number
   *          the number of the account
   * @return <code>true</code> if the method succeeds and <code>false</code>
   *         otherwise
   * @throws DataStoreException
   *           if an unrecoverable error occurs
   *
   */
  public boolean createAccount(int number) throws DataStoreException {
    // TODO Auto-generated method stub
	 String sqlCreateAccount = "INSERT INTO account(accnumber) value ("+number+");";
	try {
		stm = con.createStatement();
		stm.executeUpdate(sqlCreateAccount);
		return true;
	} catch (SQLException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}	  
    return false;
  }

  /**
   * Returns the balance of the specified account.
   *
   * @param number
   *          the number of the account
   * @return the balance of the account, or -1.0 if the account does not exist
   * @throws DataStoreException
   *           if an unrecoverable error occurs
   */
  public double getBalance(int number) throws DataStoreException {
    // TODO Auto-generated method stub
	  String sqlGetBalance="SELECT solde FROM account where ACCNUMBER="+number+";";
	  try {
		stm= con.createStatement();
		res=stm.executeQuery(sqlGetBalance);
		if(res.next()){
			double balance = res.getDouble("solde");
			//System.out.println(balance);
			return balance;}
		return -1.0;
		
	} catch (SQLException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
    return -1.0;
  }

  /**
   * Adds the specified amount to the specified account. A call to this method
   * performs a deposit if the amount is a positive value, and a withdrawal
   * otherwise. A debit operation without insufficient funds must be rejected.
   *
   * @param number
   *          the number of the account
   * @param amount
   *          the amount to add to the account's balance
   * @return the new balance of the account, or -1.0 if the withdrawal could not
   *         be performed
   * @throws DataStoreException
   *           if an unrecoverable error occurs
   */
  public double addBalance(int number, double amount) throws DataStoreException {
    // TODO Auto-generated method stub
	  if(amount>0)
	  {
	  String sqlAddBalance = "UPDATE account SET solde="+amount+" where ACCNUMBER="+number+";";
	  String sqlReturnBalance = "SELECT solde from account where ACCNUMBER="+number+";";
	  try {
		stm= con.createStatement();
		stm.executeUpdate(sqlAddBalance);
		res = stm.executeQuery(sqlReturnBalance);
		if(res.next()){
			double balance = res.getDouble("solde");
			return balance;}
		return -1.0;
		
	} catch (SQLException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	  }
	  
    return -1.0;
  }

  /**
   * Transfers the specified amount between the specified accounts.
   *
   * @param from
   *          the number of the debited account
   * @param to
   *          the number of the credited account
   * @param amount
   *          the amount to transfert
   * @return <code>true</code> if the method succeeds and <code>false</code>
   *         otherwise
   * @throws DataStoreException
   *           if an unrecoverable error occurs
   */
  public boolean transfer(int from, int to, double amount)
          throws DataStoreException {
    // TODO Auto-generated method stub
    return false;
  }

  /**
   * Returns the list of operations on the specified account in the specified
   * time interval.
   *
   * @param number
   *          the number of the account;
   * @param from
   *          start date/time (inclusive) of time interval; from the beginning
   *          of time if <code>null</code>
   * @param to
   *          end date/time (inclusive) of time interval; to the end of time if
   *          <code>null</code>
   * @return the list of operations on the account in the time interval
   * @throws DataStoreException
   *           if an unrecoverable error occurs
   */
  public List<Operation> getOperations(int number, Date from, Date to)
          throws DataStoreException {
    // TODO Auto-generated method stub
    return null;
  }

  /**
   * Closes this manager and releases all related ressources. This method must
   * be called when this manager is no longer used.
   *
   * @throws DataStoreException
   *           if an unrecoverable error occurs
   */
  public void close() throws DataStoreException {
    // TODO Auto-generated method stub
	  try {
		con.close();
	} catch (SQLException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	  System.out.println("Connection fermée");
  }

}
