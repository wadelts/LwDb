package lw.db;

import java.sql.*;
import java.util.*;

/**
  * Encapsulates Prepared database statements.
  *
  * In general, Prepared Statements created from a settings file will explicitly define the order of parameters.
  * Prepared Statements created (2on the fly") from the supplied XML input message, will leave it to this object
  * to sort the parameters at SQL creation and parameter-setting.
  *
  * @author Liam Wade
  * @version 1.0 18/11/2008
  */
public class LwPreparedStatement {
	private String preparedStatementName = null;		// the unique identifyer for the preparedStatement
	private List<String> paramColumns = null;			// identifies the positions of parameters. If not depending on sorting, will be null
	PreparedStatement preparedStatement = null;			// the actual database PreparedStatement
	private String SQL = null;			// the SQL string used to create the preparedStatement
	private String returnType = "COLUMNS";				// shape of response from Db query - COLUMNS or XML
	private int numParameters = 0;						// the number of parameters in the preparedStatement (i.e. a count of ? chars)
														// Need to calc this myself because PreparedStatement.getParameterMetaData() is not supported by the Oracle Driver and
														// causes an AbstractMethodError exception.

  /**
    * Will create a new LwPreparedStatement object.
	* @param preparedStatementName the unique identifier for the PreparedStatement
	* @param actionType the type of action to be performed by the PreparedStatement - insert, update, delete
	* @param objectName the name of the database object being manipulated (table)
	* @param colList the list of Column names (and values) to be used when creating the SQL for this LwPreparedStatement
    */
	public LwPreparedStatement(String preparedStatementName, String actionType, String objectName, Properties colList) {
		checkNullArgument(preparedStatementName);
		checkNullArgument(actionType);
		checkNullArgument(objectName);
		checkNullArgument(colList);

		this.preparedStatementName = preparedStatementName;

		if (actionType.equals("insert")) {
			this.SQL = createInsertSQLFromColList(objectName, colList);
		}

		numParameters = getNumQmarks(this.SQL);
	}

  /**
    * Will create a new LwPreparedStatement object.
	* @param LwPreparedStatementTemplate the instructions for creating the Prepared Statement
    */
	public LwPreparedStatement(LwPreparedStatementTemplate preparedStatementTemplate)
																		throws LwDbException {
		checkNullArgument(preparedStatementTemplate);

		this.preparedStatementName = preparedStatementTemplate.getPreparedStatementName();
		this.SQL = preparedStatementTemplate.getTemplateSQL();
		this.paramColumns = preparedStatementTemplate.getParamColumns();
		if (preparedStatementTemplate.getReturnType() != null) {
			this.returnType = preparedStatementTemplate.getReturnType();
		}

		numParameters = getNumQmarks(this.SQL);

		// Make sure parameter columns numbers match number of ?'s
		if (paramColumns.size() != numParameters) {
			throw new LwDbException("LwPreparedStatement constructor: Fatal error: Prepared Statement " + preparedStatementName + " had incorrect number of parameters: found " + paramColumns.size() + " expected " + numParameters);
		}
	}

  /**
    * Will create a new LwPreparedStatement object.
	* @param preparedStatementName the unique identifier for the PreparedStatement
	* @param SQL the SQL that will be used later to prepare the statement
    */
	public LwPreparedStatement(String preparedStatementName, String SQL) {
		checkNullArgument(preparedStatementName);
		checkNullArgument(SQL);

		this.preparedStatementName = preparedStatementName;
		this.SQL = SQL;

		numParameters = getNumQmarks(SQL);
	}

  /**
    * Will create a new prepareStatement, using the stored SQL
    *
	* @param dbConn the database connection
    */
	public void prepareStatement(Connection dbConn)
										throws SQLException {
		checkNullArgument(dbConn);

		preparedStatement = dbConn.prepareStatement(SQL);
	}

  /**
    * Set parameters for the prepared statement
    *
    * @param colList the names of PARAMETERISED columns only and the values to replace those ?s. Need names so can sort on name, to ensure positional matching. (were sorted in preparation phase)
    */
	public void setParameters(Properties colList)
										throws SQLException {
		checkNullArgument(colList);
		if (paramColumns == null) { // then just rely on sorting for positional accuracy of params
			// Must sort in both SQL-generation and parameter-setting, so guarantee positional parameter matching...
			List<String> list = new ArrayList<String>(colList.size());
			for(Object o : colList.keySet()) {
				list.add((String)o);
			}
			Collections.sort(list);

			int paramNum = 0;
			for(String key : list) {
				String value = colList.getProperty(key);

				preparedStatement.setString(++paramNum, value);
			}
		}
		else { // use the order of the stored list to determine positions
			int paramNum = 0;
			for(String key : paramColumns) {
				String value = colList.getProperty(key);

				preparedStatement.setString(++paramNum, value);
			}
		}
	}

  /**
    * Will execute the prepareStatement on the already-known connection
    *
	  * @return the number of rows affected by the action
    */
	public int executeUpdate()
							throws SQLException, LwDbException {
		if (preparedStatement == null) {
			throw new LwDbException("LwPreparedStatement.executeUpdate(): Fatal error: preparedStatement was null");
		}

		return preparedStatement.executeUpdate();
	}

  /**
    * Will execute the SELECT prepareStatement on the already-known connection
    *
	* @return the result set
    */
	public ResultSet executeQuery()
							throws SQLException, LwDbException {

		if (preparedStatement == null) {
			throw new LwDbException("LwPreparedStatement.executeUpdate(): Fatal error: preparedStatement was null");
		}

		return preparedStatement.executeQuery();
	}

	/**
	  *
	  * Get the the name of the preparedStatement
	  *
	  * @return the preparedStatementName
	  */
	public String getPreparedStatementName() {
		return preparedStatementName;
	}

	/**
	  *
	  * Get the number of parameters expected
	  *
	  * @return the numParameters
	  */
	public int getNumParameters() {
		return numParameters;
	}

	/**
	  *
	  * Get the SQL used to prepare the preparedStatement
	  *
	  * @return the parameterised SQL string
	  */
	public String getPreparedSQL() {
		return SQL;
	}

	/**
	  *
	  * Get the the name of the returnType
	  *
	  * @return the returnType
	  */
	public String getReturnType() {
		return returnType;
	}

  /**
    * Close the contained preparedStatement and set to null
    *
    */
	public void close()
					throws SQLException {
		if (preparedStatement == null) {
			return;
		}

		preparedStatement.close();
		preparedStatement = null;
	}

	/**
	  *
	  * Get the number of ? chars in a String
	  *
	  * @return the numParameters
	  */
	private int getNumQmarks(String inputStr) {
		assert inputStr != null;
		
		StringBuilder sb = new StringBuilder(inputStr);

		// First remove everything surrounded by single quotes, so data-based ?s not counted
		int i1 = 0;
		while ( (i1 = sb.indexOf("'")) >= 0) {
			int fromIndex = i1 + 1;
			if (fromIndex < sb.length()) { // just to be sure not out of bounds
				int i2 = 0;
				if ( (i2 = sb.indexOf("'", fromIndex)) >= 0) { // then found closing single quote
					sb.delete(i1, i2+1); // remove this quoted piece
				}
			}
		}

		// Now can look for ?s
		int numQmarks = 0;
		int i = 0;
		while ( i < sb.length() && (i = sb.indexOf("?", i)) >= 0) {
			numQmarks++;
			i++;	// move one past last-found ?
		}

		return numQmarks;
	}

	/**
	  * Surround the values in the supplied list with single quotes.
      *
	  * @param inProperties a Properties list to have values surrounded by quotes
	  *
	  * @return the Properties list with values surrounded by quotes
	  */
	public Properties wrapDataInQuotes(Properties inProperties) {
		checkNullArgument(inProperties);
		
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
	  *
	  * Create an INSERT SQL string, with ? parameters, to be used to prepare a statement
	  *
	  * @param objectName the name of the database object being manipulated (table)
	  * @param colList the list of Column names (and values) to be used when creating the SQL for this LwPreparedStatement
	  *
	  * @return the parameterised SQL
	  */
	private String createInsertSQLFromColList(String objectName, Properties colList) {
		assert objectName != null;
		assert colList != null;

		String newSQL = "INSERT INTO " + objectName + " ";
		String strColNames = "(";
		String strColValues = "(";

		// First wrap all values in quotes - should still be OK for number values
		colList = wrapDataInQuotes(colList);

		// Must sort in both preparation and execution, so guarantee positional parameter matching...
		List<String> list = new ArrayList<String>(colList.size());
		for(Object o : colList.keySet()) {
			list.add((String)o);
		}
		Collections.sort(list);

		for(String key : list) {
			String value = colList.getProperty(key);

			strColNames += key + ",";
			strColValues += value + ",";
		}

		strColNames = strColNames.substring(0, (strColNames.length()-1)); // remove superfluous ','
		strColValues = strColValues.substring(0, (strColValues.length()-1)); // remove superfluous ','

		strColNames += ")";
		strColValues = " VALUES " + strColValues + ")";

		newSQL += strColNames + strColValues;

		return newSQL;
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