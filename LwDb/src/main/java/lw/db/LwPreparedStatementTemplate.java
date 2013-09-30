package lw.db;

import java.sql.*;
import java.util.*;

/**
  * Encapsulates a template for creating a Prepared database statement.
  * @author Liam Wade
  * @version 1.0 01/12/2008
  */
public class LwPreparedStatementTemplate {
	private String preparedStatementName = null;
	private String templateSQL = null;
	private ArrayList<String> paramColumns = null;
	private String returnType = "COLUMNS";

  /**
    * Will create a new LwPreparedStatementTemplate object.
	* @param preparedStatementName the unique identifier for the PreparedStatement
	* @param templateSQL the SQL that will be used later to prepare the statement
	* @param paramColumns the names of the parameter columns. MUST match the ?s in the templateSQL and describes the sequence of those parameters.
	* @param returnType the expected type of return data - COLUMNS or XML
    */
	public LwPreparedStatementTemplate(String preparedStatementName, String templateSQL, ArrayList<String> paramColumns, String returnType) {
		checkNullArgument(preparedStatementName);
		checkNullArgument(templateSQL);
		checkNullArgument(paramColumns);

		this.preparedStatementName = preparedStatementName;
		this.templateSQL = templateSQL;
		this.paramColumns = paramColumns;
		if (returnType != null) {
			this.returnType = returnType;
		}
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
	  * Get the SQL to be used to prepare the preparedStatement
	  *
	  * @return the parameterised templateSQL string
	  */
	public String getTemplateSQL() {
		return templateSQL;
	}

	/**
	  *
	  * Get the names of the parameters as a List, in order
	  *
	  * @return the paramColumns
	  */
	public ArrayList<String> getParamColumns() {
		return paramColumns;
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
	 * @param o the object to be checked for null.
	 * 
	 * @throws IllegalArgumentException if o is null
	 */
	private void checkNullArgument(Object o) {
		if ((o == null)) throw new IllegalArgumentException("[" + Thread.currentThread().getName() + "]: Null value received.");
	}
}