package lw.db;

import java.sql.*;
import java.util.*;
import java.io.IOException;
import java.util.logging.*;

import lw.utils.LwLogger;


/**
* This class encapsulates request-handling for a database connection
*
* @author      Liam Wade
* @version     %I%
* 
* 	Stopped using wrapDataInQuotes() as didn't suit DERBY - wouldn't accept numbers wrapped.
* DERBY Notes: When supplying data in original XML message...
*	All char/varchar must be enclosed in single quotes.
*	Numbers must NOT be enclosed in quotes.
*	DATE data supplied in a normal statement (i.e. not a Prepared Statement) must be enclosed in single quotes.
*	DATE data supplied to a Prepared Statement must NOT be enclosed in quotes.
* ORACLE Notes: When supplying data in original XML message...
*	Everything can be wrapped in quotes, but obv no need to wrap numbers.
*
**/
public class LwDbConnection {

    private static final Logger logger = Logger.getLogger("gemha");

	private Connection		dbConn = null;
	private String			driverImplementer = "";
	private String			dbURL  = null;
	private String			userName = null;
	private String			userPass = null;
	private boolean			autoCommit = true;	// Note: if true, all SQL statements will be executed and committed as individual transactions
												// with no need to call commit(), otherwise transactions are grouped until commited
	private String			lastSQL = null;
	private HashMap<String,LwPreparedStatement> preparedStatements = new HashMap<String,LwPreparedStatement>();
	private int numUncommitedRows = 0;
	private String dateFormat = null;			// Remember a date format, if it was set

  /**
    * Will create a new LwDbConnection object and connect to the database.
    *
	* @param jdbcClass the database driver name - throws IllegalArgumentException if null
	* @param dbURL db URL - throws IllegalArgumentException if null
	* @param userName db user name - may be null
	* @param userPass db password - may be null
	* @param autoCommit set to true if we want to automatically commit transactions when we disconnect.
	*
    * @exception LwDbException
    */
	public LwDbConnection(String jdbcClass, String dbURL, String userName, String userPass, boolean autoCommit)
																												throws LwDbException {
		checkNullArgument(jdbcClass);
		checkNullArgument(dbURL);

		connectStandAlone(jdbcClass, dbURL, userName, userPass, autoCommit);
	}

  /**
    * Will create a new LwDbConnection object, for use in an Application
    * Server environment, where the Connection is passed to LwDbConnection for use.
    *
    * @param dbConn the database connection already opened in, for example in an App Server - throws IllegalArgumentException if null
	*
    * @exception LwDbException
    */
	public LwDbConnection(Connection dbConn) {
		checkNullArgument(dbConn);
		this.dbConn = dbConn;
	}

  /**
    * Connect to the specified database, which is not managed
    * by an application server.
    *
  	* @param logger the java jogger for recording events, info etc
	* @param jdbcClass the database driver name
	* @param dbURL db URL
	* @param userName db user name
	* @param userPass db password
	* @param autoCommit set to true if we want to automatically commit transactions when we disconnect.
	*
    * @exception LwDbException
    */
	private void connectStandAlone(String jdbcClass, String dbURL, String userName, String userPass, boolean autoCommit)
																												throws LwDbException {
		this.dbURL = dbURL;
		this.userName = userName;
		this.userPass = userPass;
		this.autoCommit = autoCommit;

		try {
			Class.forName(jdbcClass);
		}
		catch( Throwable e ) {
			logger.severe("Could not load class " + jdbcClass + " Exception: " + e.getMessage());
			logger.severe("CLASSPATH is " + System.getProperty("java.class.path"));
			
			throw new LwDbException("Could not load class " + jdbcClass + " Exception: " + e.getMessage());
		}

		try {
			if (userName == null) {
				logger.info("No UserName for database supplied. Will try to connect without.");
				dbConn = DriverManager.getConnection(dbURL);
				logger.info("Connected to Db " + dbURL + " with no UserName supplied");
			}
			else {
				dbConn = DriverManager.getConnection(dbURL, userName, userPass);
				logger.info("Connected to Db " + dbURL + " as " + userName);
			}

			dbConn.setAutoCommit(autoCommit);

			// Hammy way of finding out if we're talking to Oracle etc
			String connClassName = dbConn.getClass().getName();
			if (connClassName.contains("oracle")) {
				driverImplementer = "oracle";
			} else if (connClassName.contains("mysql")) {
				driverImplementer = "mysql";
			} else if (connClassName.contains("derby")) {
				driverImplementer = "derby";
			} else if (connClassName.contains("sqlserver")) {
				driverImplementer = "sqlserver";
			}
			
			logger.config("Assumed Driver Implementer is " + driverImplementer);
			logger.config("JDBC Driver Name is " + dbConn.getClass().getName());
			logger.config("Connection AutoCommit is " + dbConn.getAutoCommit());
//			oracle.jdbc.driver.OracleDriver
		}
		catch (SQLException se) {
			logger.severe("Could not create connection to database " + dbURL + " Message: " + se.getMessage() + " Error Code: " + se.getErrorCode());
			throw new LwDbException("Could not create connection to database " + dbURL + " Message: " + se.getMessage() + " Error Code: " + se.getErrorCode(), se.getErrorCode());
		}
	}

	/**
	  *
	  * Check to see iof the database connection is still open..
	  *
	  * @return true if the database connection is open, otherwise false
	  */
	public boolean connectionOpen() {
		return dbConn != null;
	}

	/**
	  *
	  * Open the connection again, after having previously called close().
	  * This is for a locally managed connection only.
	  *
	  * @return the SQL as a string
	  */
	public void reOpen()
					throws LwDbException {

		try {
			dbConn = DriverManager.getConnection(dbURL, userName, userPass);
			logger.info("Connected to Db " + dbURL + " as " + userName);

			dbConn.setAutoCommit(autoCommit);

			logger.config("Connection AutoCommit is " + dbConn.getAutoCommit());

			reInstatePreparedStatements();

			// Set the date format again, if had been set at start of first connection
			if (this.dateFormat != null) {
				setDateFormat(this.dateFormat);
			}
		}
		catch (SQLException se) {
			logger.severe("Could not create connection to database " + dbURL + " Message: " + se.getMessage() + " Error Code: " + se.getErrorCode());
			throw new LwDbException("Could not create connection to database " + dbURL + " Message: " + se.getMessage() + " Error Code: " + se.getErrorCode(), se.getErrorCode());
		}
	}

	/**
	  *
	  *
	  * @param dateFormat the new format for dates in queries and results
	  *
      * @exception LwDbException
      *
	  */
	public void setDateFormat(String dateFormat) throws LwDbException {
		if (dateFormat == null) {
			throw new LwDbException("Parameter was null.", -1001);
		}
		else { // remember format in case we close connection and re-open
			this.dateFormat = dateFormat;
		}


		try {
			////////////////////////////////////////////////////////////////////////////////////////////////////////
			// Generate ALTER clause...
			////////////////////////////////////////////////////////////////////////////////////////////////////////
			String instructionSQL = "ALTER SESSION SET nls_date_format = '" + dateFormat + "'";


			Statement stmt = dbConn.createStatement();

			stmt.executeUpdate(instructionSQL);

			stmt.close(); // frees up database and JDBC resources without waiting for garbage collection.

		}
		catch (SQLException se) {
			logger.severe("Could not set Date Format. Caught exception: " + se.getMessage() + " Error Code: " + se.getErrorCode());
			throw new LwDbException("Could not set Date Format. Caught exception: " + se.getMessage() + " Error Code: " + se.getErrorCode(), se.getErrorCode());
		}
	}

	/**
	  *
	  * Get the SQL submitted by the last instruction.
	  *
	  * @return the SQL as a string
	  */
	public String getLastSQL() {
		return lastSQL;
	}

	/**
	  * Submit a simple (probably key-based) query to the database.
	  *
	  * @param objectName the table to be queried
	  * @param colList the names of columns for which values are to be returned in the query result.
	  * @param qualList the names of columns, and thLw values, to be ANDed in the WHERE clause.
	  *
      * @exception LwDbException
      *
	  * @return a LwDbQueryResult containing the results of the query
	  */
	public LwDbQueryResult getQueryResults(String objectName, Properties colList, Properties qualList)
																							throws LwDbException {
		checkNullArgument(objectName);
		checkNullArgument(colList);
		checkNullArgument(qualList);

    	ResultSet results = null;

		try {
			////////////////////////////////////////////////////////////////////////////////////////////////////////
			// Generate SELECT clause...
			////////////////////////////////////////////////////////////////////////////////////////////////////////
			lastSQL = "SELECT ";

			// First wrap all values in quotes - should stil be OK for number values
//			colList = wrapDataInQuotes(colList);

			Enumeration<?> e = colList.propertyNames();
			if (e != null && e.hasMoreElements()) {
				String key = (String) e.nextElement();
				lastSQL = lastSQL  + " " + key + " ";

				while (e != null && e.hasMoreElements()) {
					key = (String) e.nextElement();
					lastSQL = lastSQL + "," + key + " ";
				}

				lastSQL += " FROM " + objectName + " ";
			}
			else { // nothing to update!
				throw new LwDbException("Parameter colList was empty.", -1002);
			}


			////////////////////////////////////////////////////////////////////////////////////////////////////////
			// Generate WHERE clause...
			////////////////////////////////////////////////////////////////////////////////////////////////////////
			// First wrap all values in quotes - should stil be OK for number values
//			qualList = wrapDataInQuotes(qualList);

			// Note: qualifying any col "= null" will be valid SQL syntax, but will NOT return the row.
			//		 You MUST use the "is null" syntax.
			Enumeration<?> enumQualifiers = qualList.propertyNames();
			if (enumQualifiers != null && enumQualifiers.hasMoreElements()) {

				String key = (String) enumQualifiers.nextElement();
				String value = qualList.getProperty(key);
				if (value.equals("null")) {
					lastSQL = lastSQL + " WHERE " + key + " is " + value + " ";
				}
				else {
					lastSQL = lastSQL + " WHERE " + key + " = " + value + " ";
				}

				while (enumQualifiers != null && enumQualifiers.hasMoreElements()) {
					key = (String) enumQualifiers.nextElement();
					value = qualList.getProperty(key);
					if (value.equals("null")) {
						lastSQL = lastSQL + " AND " + key + " is " + value + " ";
					}
					else {
						lastSQL = lastSQL + " AND " + key + " = " + value + " ";
					}
				}
			}

			logger.fine("lastSQL: " + lastSQL);

			Statement stmt = dbConn.createStatement();
			results = stmt.executeQuery(lastSQL);
		}
		catch (SQLException se) {
			logger.severe("SELECT query failed. Caught exception: " + se.getMessage() + " Error Code: " + se.getErrorCode() + " SQL State: " + se.getSQLState());
			throw new LwDbException("LwDbConnection.getQueryResults(): SELECT query failed. Caught exception: " + se.getMessage() + " Error Code: " + se.getErrorCode() + " SQL State: " + se.getSQLState(), se.getErrorCode());
		}

		// Transform result set
		Vector<Properties> r = getAsPropertySet(results);

		// Create a new Lw results objects
		LwDbQueryResult eqr = new LwDbQueryResult(r, "COLUMNS");

		try {results.close();} catch (SQLException se) {/* Cannot do anything about this kind of error, just continue */}

		return eqr;
	}

	/**
	  * Execute a prepared SELECT statement, supplying the values for expected parameters.
	  *
	  * @param preparedStatementName the key name of the statement
	  * @param paramList the names of PARAMETERISED columns only and the values to replace those ?s. Need names so can sort on name, to ensure positional matching. (were sorted in preparation phase)
	  *
      * @exception LwDbException
      *
	  * @return a LwDbQueryResult containing the results of the query
	  */
	public LwDbQueryResult executePreparedQuery(String preparedStatementName, Properties paramList)
															throws LwDbException {

		if (preparedStatementName == null || paramList == null || paramList.size() < 1) {
			throw new LwDbException("LwDbConnection.executePreparedStatement(): Required Parameter was null or empty.", -1001);
		}

    	ResultSet results = null;
    	LwPreparedStatement preparedStatement = null;

		try {
			// Note: Do not wrap values in quotes for parameter processing, otherwise can't send them all in with setString()
			// ALL parameters are submitted as strings, so we leave it up to the SQL engine to perform any conversions on the fly.

			preparedStatement = preparedStatements.get(preparedStatementName);

			if (preparedStatement == null) {
				throw new LwDbException("PreparedStatement " + preparedStatementName + " does not exist", -1003);
			}
			else {
				// Make sure correct number of parameters were sent in, otherwise hangs connection
				if (preparedStatement.getNumParameters() != paramList.size()) {
					logger.severe("Wrong number of parameter values supplied for execution of PreparedStatement " + preparedStatementName + ". Expected " + preparedStatement.getNumParameters() + ", found " + paramList.size());
					throw new LwDbException("Wrong number of parameter values supplied for execution of PreparedStatement " + preparedStatementName + ". Expected " + preparedStatement.getNumParameters() + ", found " + paramList.size(), -1004);
				}

				preparedStatement.setParameters(paramList);

				results = preparedStatement.executeQuery();

				logger.fine("Executed prepared insert statement " + preparedStatementName);
			}
		}
		catch (SQLException se) {
			logger.severe("Could not execute Prepared Query " + preparedStatementName + ". Caught exception: " + se.getMessage() + " Error Code: " + se.getErrorCode());
			throw new LwDbException("LwdbConnection.executePreparedQuery(): Could not execute Prepared Query " + preparedStatementName + ". Caught exception: " + se.getMessage() + " Error Code: " + se.getErrorCode(), se.getErrorCode());
		}

		// Transform result set
		Vector<Properties> r = getAsPropertySet(results);

		// Create a new Lw results objects
		LwDbQueryResult eqr = new LwDbQueryResult(r, preparedStatement.getReturnType());

		try {results.close();} catch (SQLException se) {/* Cannot do anything about this kind of error, just continue */}

		return eqr;
	}

	/**
	  * Insert a row into a database table.
	  *
	  * @param objectName the table in which the row is to inserted
	  * @param colList the names of columns involved in the insert and the values. Cannot be null or empty.
	  *
      * @exception LwDbException
      *
	  * @return the number of rows inserted, if successful
	  */
	public int insert(String objectName, Properties colList)
															throws LwDbException {
		if (objectName == null || colList == null) {
			throw new LwDbException("LwDbConnection.insert(): Parameter was null.", -1001);
		}

		try {
			lastSQL = "INSERT INTO " + objectName + " ";
			String strColNames = "(";
			String strColValues = "(";

			// First wrap all values in quotes - should still be OK for number values
//			colList = wrapDataInQuotes(colList);

			// Note: qualifying any col "= null" will be valid SQL syntax, but will NOT return the row.
			//		 You MUST use the "is null" systax.
			Enumeration<?> e = colList.propertyNames();
			if (e != null && e.hasMoreElements()) {
				while (e.hasMoreElements()) {
					String key = (String) e.nextElement();
					String value = colList.getProperty(key);

					strColNames += key + (e.hasMoreElements() ? "," : "");
					strColValues += value + (e.hasMoreElements() ? "," : "");
				}

				strColNames += ")";
				strColValues = " VALUES " + strColValues + ")";
				lastSQL += strColNames + strColValues;
			}
			else { // nothing to lock for update!
				throw new LwDbException("Parameter colList was empty.", -1002);
			}

			logger.fine("lastSQL: " + lastSQL);

			Statement stmt = dbConn.createStatement();

			int numRowsInserted = stmt.executeUpdate(lastSQL);
			numUncommitedRows += numRowsInserted;

			stmt.close(); // frees up database and JDBC resources without waiting for garbage collection (incl result set).

			return numRowsInserted;

		}
		catch (SQLException se) {
			logger.severe("Could not insert row. Caught exception: " + se.getMessage() + " Error Code: " + se.getErrorCode() + "SQL was: " + lastSQL);
			throw new LwDbException("LwDbConnection.insert(): Could not insert row. Caught exception: " + se.getMessage() + " Error Code: " + se.getErrorCode(), se.getErrorCode());
		}
	}

	/**
	  * Prepare an SQL statement from a template for parameterised use - can be of any type.
	  *
	  * @param preparedStatementTemplate the template that will define how to create the statement
	  *
      * @exception LwDbException
      *
	  */
	public void prepareStatement(LwPreparedStatementTemplate preparedStatementTemplate)
																		throws LwDbException {
		if (preparedStatementTemplate == null ) {
			throw new LwDbException("LwDbConnection.prepareStatement(): Required Parameter was null.", -1001);
		}

		// If we've already prepared this statement, just return...
		if (preparedStatements.containsKey(preparedStatementTemplate.getPreparedStatementName())) {
			return;
		}


		LwPreparedStatement newPreparedStatement = new LwPreparedStatement(preparedStatementTemplate);

		lastSQL = newPreparedStatement.getPreparedSQL();
		logger.fine("lastSQL: " + lastSQL);

		try {
			newPreparedStatement.prepareStatement(dbConn);

			preparedStatements.put(preparedStatementTemplate.getPreparedStatementName(), newPreparedStatement);
			logger.info("Created prepared statement " + preparedStatementTemplate.getPreparedStatementName());

		}
		catch (SQLException se) {
			logger.severe("Could not prepare statement " + preparedStatementTemplate.getPreparedStatementName() + ". Caught exception: " + se.getMessage() + " Error Code: " + se.getErrorCode());
			throw new LwDbException("LwDbConnection.prepareStatement(): Could not prepare statement " + preparedStatementTemplate.getPreparedStatementName() + ". Caught exception: " + se.getMessage() + " Error Code: " + se.getErrorCode(), se.getErrorCode());
		}
	}

	/**
	  * Prepare an SQL-Insert statement for parameterised use.
	  *
	  * @param preparedStatementName the key name to give the new statement, for later retrieval
	  * @param objectName the table in which the row is to inserted
	  * @param colList the names of columns involved in the insert. Also a constant value or '?' (to parameterise).
	  *
      * @exception LwDbException
      *
	  */
	public void prepareInsert(String preparedStatementName, String objectName, Properties colList)
															throws LwDbException {
		if (preparedStatementName == null || colList == null || colList.size() < 1) {
			throw new LwDbException("LwDbConnection.prepareInsert(): Required Parameter was null or empty.", -1001);
		}

		// If we've already prepared this statement, just return...
		if (preparedStatements.containsKey(preparedStatementName)) {
			return;
		}

		if (objectName == null) {
			throw new LwDbException("LwDbConnection.prepareInsert(): Required objectName Parameter was null or empty.", -1002);
		}

		LwPreparedStatement newPreparedStatement = new LwPreparedStatement(preparedStatementName, "insert", objectName, colList);

		lastSQL = newPreparedStatement.getPreparedSQL();
		logger.fine("lastSQL: " + lastSQL);

		try {
			newPreparedStatement.prepareStatement(dbConn);

			preparedStatements.put(preparedStatementName, newPreparedStatement);
			logger.info("Created prepared insert statement " + preparedStatementName);

		}
		catch (SQLException se) {
			logger.severe("Could not prepare insert " + preparedStatementName + ". Caught exception: " + se.getMessage() + " Error Code: " + se.getErrorCode());
			throw new LwDbException("Could not prepare insert " + preparedStatementName + ". Caught exception: " + se.getMessage() + " Error Code: " + se.getErrorCode(), se.getErrorCode());
		}
	}

	/**
	  * Execute a prepared statement, supplying the values for expected parameters.
	  *
	  * @param preparedStatementName the key name of the statement
	  * @param paramList the names of PARAMETERISED columns only and the values to replace those ?s. Need names so can sort on name, to ensure positional matching. (were sorted in preparation phase)
	  *
      * @exception LwDbException
      *
	  * @return the number of rows inserted, if successful
	  */
	public int executePreparedStatement(String preparedStatementName, Properties paramList)
															throws LwDbException {
		if (preparedStatementName == null || paramList == null || paramList.size() < 1) {
			throw new LwDbException("LwDbConnection.executePreparedStatement(): Required Parameter was null or empty.", -1001);
		}

		try {
			// Note: Do not wrap values in quotes for parameter processing, otherwise can't send them all in with setString()
			// ALL parameters are submitted as strings, so we leave it up to the SQL engine to perform any conversions on the fly.

			LwPreparedStatement preparedStatement = preparedStatements.get(preparedStatementName);

			if (preparedStatement == null) {
				throw new LwDbException("PreparedStatement " + preparedStatementName + " does not exist", -1003);
			}
			else {
				// Make sure correct number of parameters were sent in, otherwise hangs connection
				if (preparedStatement.getNumParameters() != paramList.size()) {
					logger.severe("Wrong number of parameter values supplied for execution of PreparedStatement " + preparedStatementName + ". Expected " + preparedStatement.getNumParameters() + ", found " + paramList.size());
					throw new LwDbException("Wrong number of parameter values supplied for execution of PreparedStatement " + preparedStatementName + ". Expected " + preparedStatement.getNumParameters() + ", found " + paramList.size(), -1004);
				}

				preparedStatement.setParameters(paramList);

				int numRowsAffected = preparedStatement.executeUpdate();
				numUncommitedRows += numRowsAffected;
				logger.fine("Executed prepared insert statement " + preparedStatementName);

				return numRowsAffected;
			}
		}
		catch (SQLException se) {
			logger.severe("Could not execute Prepared Statement " + preparedStatementName + ". Caught exception: " + se.getMessage() + " Error Code: " + se.getErrorCode());
			throw new LwDbException("Could not execute Prepared Statement " + preparedStatementName + ". Caught exception: " + se.getMessage() + " Error Code: " + se.getErrorCode(), se.getErrorCode());
		}
	}

	/**
	  * Lock a row for update.
	  * The cols supplied in colList should contain ALL cols that may possibly be updated
	  *
	  * @param objectName the table in which the row is to be locked
	  * @param colList the names of columns to be updated (later) and thLw old values. Cannot be null or empty.
	  *
      * @exception LwDbException
      *
	  * @return the number of rows locked, if successful, 0 if data has changed
	  */
	public int lockForUpdatePessimistic(String objectName, Properties colList)
																	throws LwDbException {
		if (objectName == null || colList == null) {
			throw new LwDbException("LwDbConnection.lockForUpdatePessimistic(): Parameter was null.", -1001);
		}

    	int rowsReturned = 0;

		try {
			lastSQL = "SELECT * FROM " + objectName + " ";

			// First wrap all values in quotes - should stil be OK for number values
//			colList = wrapDataInQuotes(colList);

			// Note: qualifying any col "= null" will be valid SQL syntax, but will NOT return the row.
			//		 You MUST use the "is null" systax.
			Enumeration<?> e = colList.propertyNames();
			if (e != null && e.hasMoreElements()) {
				String key = (String) e.nextElement();
				String value = colList.getProperty(key);
				if (value.equals("null")) {
					lastSQL = lastSQL + " WHERE " + key + " is " + value + " ";
				}
				else {
					lastSQL = lastSQL + " WHERE " + key + " = " + value + " ";
				}

				while (e != null && e.hasMoreElements()) {
					key = (String) e.nextElement();
					value = colList.getProperty(key);
					if (value.equals("null")) {
						lastSQL = lastSQL + " AND " + key + " is " + value + " ";
					}
					else {
						lastSQL = lastSQL + " AND " + key + " = " + value + " ";
					}
				}
				
				lastSQL = lastSQL + " FOR UPDATE ";
				if (driverImplementer.equals("oracle")) {
					lastSQL = lastSQL + " NOWAIT "; // NOWAIT means won't wait if already locked, gimme ORA-00054
				}
			}
			else { // nothing to lock for update!
				throw new LwDbException("Parameter colList was empty.", -1002);
			}

			logger.fine("lastSQL: " + lastSQL);

			Statement stmt = dbConn.createStatement();

			ResultSet result = stmt.executeQuery(lastSQL);	// only one row can be returned in from an executeQuery

			while (result.next()) {
				rowsReturned++;
			}

			stmt.close(); // frees up database and JDBC resources without waiting for garbage collection (incl result set).

			return rowsReturned;

		}
		catch (SQLException se) {
			if (se.getErrorCode() == 00054) { // then row already locked, and we didn't want to wait
				logger.warning("Could not lock a row. Already locked.");
				throw new LwDbException("Could not lock a row. Already locked.", -1005);
			}
			else {
				logger.severe("Could not lock a row. Caught exception: " + se.getMessage() + " Error Code: " + se.getErrorCode());
				throw new LwDbException("Could not lock a row. Caught exception: " + se.getMessage() + " Error Code: " + se.getErrorCode(), se.getErrorCode());
			}
		}
	}

	/**
	  * Update a row (or rows).
	  * When using Pessimistic (or no) locking, the qualList will only contain identifying col/val pairs, to be added to the WHERE clause.
	  * When using Optimistic locking, the qualList will also contain a col/OLD-val pair for each col being updated (in the colList set), to be added to the WHERE clause.
	  *
	  * @param objectName the table in which the row(s) is to be updated
	  * @param colList the names of columns to be updated and thLw NEW values.
	  * @param qualList the names of columns, and thLw values, to be ANDed in the WHERE clause.
	  *
      * @exception LwDbException
      *
	  * @return the number of rows updated, if successful (0..n), < 0 for error (the caller will know whether optimistic "locking" was used, so a return of 0 can be properly interpreted
	  */
	public int update(String objectName, Properties colList, Properties qualList)
																			throws LwDbException {
		if (objectName == null || colList == null) {
			throw new LwDbException("Parameter was null.", -1001);
		}


		try {
			////////////////////////////////////////////////////////////////////////////////////////////////////////
			// Generate UPDATE clause...
			////////////////////////////////////////////////////////////////////////////////////////////////////////
			lastSQL = "UPDATE " + objectName + " SET ";

			// First wrap all values in quotes - should stil be OK for number values
//			colList = wrapDataInQuotes(colList);

			Enumeration<?> e = colList.propertyNames();
			if (e != null && e.hasMoreElements()) {
				String key = (String) e.nextElement();
				lastSQL = lastSQL  + " " + key + " = " + colList.getProperty(key) + " ";

				while (e != null && e.hasMoreElements()) {
					key = (String) e.nextElement();
					lastSQL = lastSQL + "," + key + " = " + colList.getProperty(key) + " ";
				}
			}
			else { // nothing to update!
				throw new LwDbException("Parameter colList was empty.", -1002);
			}


			////////////////////////////////////////////////////////////////////////////////////////////////////////
			// Generate WHERE clause...
			////////////////////////////////////////////////////////////////////////////////////////////////////////
			// First wrap all values in quotes - should stil be OK for number values
//			qualList = wrapDataInQuotes(qualList);

			// Note: qualifying any col "= null" will be valid SQL syntax, but will NOT return the row.
			//		 You MUST use the "is null" systax.
			Enumeration<?> enumQualifiers = qualList.propertyNames();
			if (enumQualifiers != null && enumQualifiers.hasMoreElements()) {

				String key = (String) enumQualifiers.nextElement();
				String value = qualList.getProperty(key);
				if (value.equals("null")) {
					lastSQL = lastSQL + " WHERE " + key + " is " + value + " ";
				}
				else {
					lastSQL = lastSQL + " WHERE " + key + " = " + value + " ";
				}

				while (enumQualifiers != null && enumQualifiers.hasMoreElements()) {
					key = (String) enumQualifiers.nextElement();
					value = qualList.getProperty(key);
					if (value.equals("null")) {
						lastSQL = lastSQL + " AND " + key + " is " + value + " ";
					}
					else {
						lastSQL = lastSQL + " AND " + key + " = " + value + " ";
					}
				}
			}
			else { // nothing to lock for update!
				throw new LwDbException("Parameter qualList was empty.", -1003);
			}

			logger.fine("lastSQL: " + lastSQL);

			Statement stmt = dbConn.createStatement();

			int numRowsUpdated = stmt.executeUpdate(lastSQL);
			numUncommitedRows += numRowsUpdated;

			stmt.close(); // frees up database and JDBC resources without waiting for garbage collection.

			return numRowsUpdated;

		}
		catch (SQLException se) {
			logger.severe("Could not update row(s). Caught exception: " + se.getMessage() + " Error Code: " + se.getErrorCode());
			throw new LwDbException("Could not update row(s). Caught exception: " + se.getMessage() + " Error Code: " + se.getErrorCode(), se.getErrorCode());
		}
	}

	/**
	  * Delete row(s) from a table.
	  *
	  * @param objectName the table from which the row is to be deleted
	  * @param colList the names of columns to be used in the WHERE clause and thLw old values. If empty or null, ALL rows will be deleted.
	  *
      * @exception LwDbException
      *
	  * @return the number of rows locked, if successful, 0 if data has changed
	  */
	public int delete(String objectName, Properties colList)
													throws LwDbException {
		if (objectName == null) {
			throw new LwDbException("Parameter was null.", -1001);
		}

		try {
			lastSQL = "DELETE FROM " + objectName;

			// First wrap all values in quotes - should stil be OK for number values
//			colList = wrapDataInQuotes(colList);

			// Note: qualifying any col "= null" will be valid SQL syntax, but will NOT return the row.
			//		 You MUST use the "is null" systax.
			Enumeration<?> e = colList.propertyNames();
			if (e != null && e.hasMoreElements()) {

				String key = (String) e.nextElement();
				String value = colList.getProperty(key);
				if (value.equals("null")) {
					lastSQL += " WHERE " + key + " is " + value + " ";
				}
				else {
					lastSQL += " WHERE " + key + " = " + value + " ";
				}


				while (e != null && e.hasMoreElements()) {
					key = (String) e.nextElement();
					value = colList.getProperty(key);
					if (value.equals("null")) {
						lastSQL += " AND " + key + " is " + value + " ";
					}
					else {
						lastSQL += " AND " + key + " = " + value + " ";
					}
				}
			}

			logger.fine("lastSQL: " + lastSQL);

			Statement stmt = dbConn.createStatement();

			int numRows = stmt.executeUpdate(lastSQL);
			numUncommitedRows += numRows;

			stmt.close(); // frees up database and JDBC resources without waiting for garbage collection (incl result set).

			return numRows;

		}
		catch (SQLException se) {
			logger.severe("Could not delete row(s). Caught exception: " + se.getMessage() + " Error Code: " + se.getErrorCode());
			throw new LwDbException("Could not delete row(s). Caught exception: " + se.getMessage() + " Error Code: " + se.getErrorCode(), se.getErrorCode());
		}
	}

	/**
        * This method rolls back a database transaction.
        *
        * @exception LwDbException
        */
	public void sessionRollback()
							throws LwDbException {

		try {
			dbConn.rollback();
			numUncommitedRows = 0;
		}
		catch(SQLException se) {
			throw new LwDbException("Could not roll back transaction. Message: " + se.getMessage() + " Error Code: " + se.getErrorCode(), se.getErrorCode());
		}
	}

	/**
        * This method commits a database transaction.
        *
        * @exception LwDbException
        */
	public void sessionCommit()
							throws LwDbException {

		try {
			dbConn.commit();
			numUncommitedRows = 0;
		}
		catch(SQLException se) {
			throw new LwDbException("Could not commit transaction. Message: " + se.getMessage() + " Error Code: " + se.getErrorCode(), se.getErrorCode());
		}
	}

	/**
        * This method commits database transactions, if we've reached the specified number of actions (inserts, updates or deletes).
        *
        * @exception LwDbException
        */
	public void sessionCommitIfReached(int uncommitedRowsLimit)
											throws LwDbException {

		if (numUncommitedRows >= uncommitedRowsLimit) {
			this.sessionCommit();
		}
	}


	/**
	  * (Re)establish all the PreparedStatements for a (new) connection.
	  *
	  * @param preparedStatementName the key name of the statement
	  *
      * @exception LwDbException
      *
	  */
	public void reInstatePreparedStatements()
									throws LwDbException {

		for (String preparedStatementName : preparedStatements.keySet()) {
			reInstatePreparedStatement(preparedStatementName);
		}
	}

	/**
	  * (Re)establish a PreparedStatement for a (new) connection.
	  *
	  * @param preparedStatementName the key name of the statement
	  *
      * @exception LwDbException
      *
	  */
	public void reInstatePreparedStatement(String preparedStatementName)
														throws LwDbException {
		checkNullArgument(preparedStatementName);
		try {
			LwPreparedStatement p = preparedStatements.get(preparedStatementName);
			if (p != null) {
				p.prepareStatement(dbConn);
				logger.info("Re-instated Prepared statement" + preparedStatementName);
			}
		}
		catch (SQLException se) {
			logger.severe("Could not re-instate prepared statement " + preparedStatementName + ". Caught exception: " + se.getMessage() + " Error Code: " + se.getErrorCode());
			throw new LwDbException("Could not re-instate prepared statement " + preparedStatementName + ". Caught exception: " + se.getMessage() + " Error Code: " + se.getErrorCode(), se.getErrorCode());
		}
	}

	/**
	  * Close all the PreparedStatements for a (new) connection.
	  *
	  * @param preparedStatementName the key name of the statement
	  *
      * @exception LwDbException
      *
	  */
	public void closePreparedStatements()
									throws LwDbException {

		for (String preparedStatementName : preparedStatements.keySet()) {
			closePreparedStatement(preparedStatementName);
		}
	}

	/**
	  * Close the specified PreparedStatement. This should be called when finished with the PreparedStatement.
	  * Frees up database and JDBC resources without waiting for garbage collection (incl any result set).
	  *
	  * @param preparedStatementName the key name of the statement
	  *
      * @exception LwDbException
      *
	  */
	public void closePreparedStatement(String preparedStatementName)
														throws LwDbException {
		checkNullArgument(preparedStatementName);
		try {
			LwPreparedStatement p = preparedStatements.get(preparedStatementName);
			if (p != null) {
				p.close();
				logger.info("Destroyed prepared insert statement" + preparedStatementName);
			}
		}
		catch (SQLException se) {
			logger.severe("Could not close prepared statement " + preparedStatementName + ". Caught exception: " + se.getMessage() + " Error Code: " + se.getErrorCode());
			throw new LwDbException("Could not close prepared statement " + preparedStatementName + ". Caught exception: " + se.getMessage() + " Error Code: " + se.getErrorCode(), se.getErrorCode());
		}
	}

	/**
	  * Surround the values in the supplied list with single quotes.
      *
	  * @param inProperties a Properties list to have values surrounded by quotes
	  *
	  * @return the Properties list with values surrounded by quotes, null if inProperties was null
	  */
	public Properties wrapDataInQuotes(Properties inProperties) {
		Properties outProperties = new Properties();

		if (inProperties != null) {
			Enumeration<?> e = inProperties.propertyNames();
			while (e.hasMoreElements()) {
				String key = (String) e.nextElement();

				if (inProperties.getProperty(key).equals("null") || inProperties.getProperty(key).equals("'null'")) { // don't wrap null in quotes
					outProperties.put(key, "null");
				}
				else if (inProperties.getProperty(key).equals("?") || inProperties.getProperty(key).equals("'?'")) { // don't wrap ? in quotes
					outProperties.put(key, "?");
				}
				else if (isNum(inProperties.getProperty(key))) { // don't wrap numbers in quotes
					outProperties.put(key, inProperties.getProperty(key));
				}
				else {
					// Note: change to using using StringBuilder when recompiled for Java1.5 - is more efficient for single-threaded apps
					StringBuffer newData = new StringBuffer(inProperties.getProperty(key));
					// First double up any quotes in the data, so Oracle will accept them as part of the string
					int index = 0;
					while ( (index = newData.indexOf("'", index)) >= 0) {
						newData.insert(index++, '\'');

						// Move beyond found quote and make sure not out of bounds
						if (index++ >= newData.length()) {
							break; // don't want to get an out of bounds exception
						}
					}

					// Now wrap in quotes...
					newData.insert(0, '\'');
					newData.append('\'');
					outProperties.put(key, newData.toString());
				}

			}
		}

		return outProperties;
	}

	/**
        * This method transforms the data in a ResultSet into a Vector of Properties (i.e. Rows x cols)
        *
        * @param rs the result set to be transformed
        * 
        * @return a Vector of Properties, empty if rs was null 
        */
	public Vector<Properties> getAsPropertySet(ResultSet rs) {
		Vector<Properties> results = new Vector<Properties>();

		if (rs == null) {
			return results;
		}

		try {
			// get number of columns in result...
			ResultSetMetaData rsmd = rs.getMetaData();
			int numberOfColumns = rsmd.getColumnCount();

			// Go through the result rows...
			while (rs.next()) {
				// Process this row's columns, transferring the data to a Properties object...
				Properties rowData = new Properties();
				for (int i=1; i <= numberOfColumns; i++) {
					String colName = rsmd.getColumnName(i);
					String colData = rs.getString(i);
					if (colName != null && colData != null) {
						rowData.put(colName, colData);
					}
				}
				results.addElement(rowData);
			}
		}
		catch (SQLException se) {
			System.out.println("SQL Error in EirUpsmaDb.getAsPropertySet() : " + se.getMessage());
			System.out.println("SQL State in EirUpsmaDb.getAsPropertySet() : " + se.getSQLState());
			System.out.println("SQL Error in EirUpsmaDb.getAsPropertySet() : " + se.getErrorCode());
		}

		return results;
	}

	/**
        * This method closes the connection to the database
        *
        * @param out the Logger to use to report events etc
        */
	public void close(LwLogger out) {
		// EirLogger used when close() is called by
		// a VM shutdown hook, in which case the logger may be dead (it's shutdown hook may be
		// executed before ours), so a FileWriter object is used instead.

		// close the preparedStatements
		try {closePreparedStatements();} catch(LwDbException e) {/* do nothing, closing database anyway */}

		// Closing session object
		try{
			try {
				dbConn.close();
				dbConn = null;
				if (out != null) {
					out.appendln("Closed connection to database " + dbURL + " as " + userName);
				}
				else {
					logger.info("Closed connection to database " + dbURL + " as " + userName);
				}
			}
			catch (SQLException se) {
				if (out != null) {
					out.appendln("Could not properly close connection to database " + dbURL + " Message: " + se.getMessage() + " Error Code: " + se.getErrorCode());
				}
				else {
					logger.warning("Could not properly close connection to database " + dbURL + " Message: " + se.getMessage() + " Error Code: " + se.getErrorCode());
				}
				throw new LwDbException("Could not properly close connection to database " + dbURL + " Message: " + se.getMessage() + " Error Code: " + se.getErrorCode(), se.getErrorCode());
			}

		}
		catch(IOException e) {
			System.out.println("LwDbConnection.close(): While closing connection caught " + e.getMessage());
		}
		catch(Exception e) {
			if (out != null) {
				try {
					out.appendln("LwDbConnection.close(): While closing connection caught " + e.getMessage());
				}
				catch(IOException enumQualifiers) {
					System.out.println("LwDbConnection.close(): While reporting Exception, caught " + enumQualifiers.getMessage());
				}
			}
			else {
				logger.warning("While closing session or connection caught " + e.getMessage());
			}
		}
	}

	/**
     * This method tells you if a string contains only a valid number
     *
     * @param out the Logger to use to report events etc
     */
	private boolean isNum(String s) {
		assert s != null;
		
		try {
			Double.parseDouble(s);
		}
		catch (NumberFormatException nfe) {
			return false;
		}
			return true;
		}
	
	/**
	 * @param o the object to be checked for null.
	 * 
	 * @throws IllegalArgumentException if o is null
	 */
	private void checkNullArgument(Object o) {
		if ((o == null)) throw new IllegalArgumentException("[" + Thread.currentThread().getName() + "]: Null value received.");
	}
}