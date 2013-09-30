package lw.db;

import java.util.*;

/**
  * Encapsulates results from database queries.
  * @author Liam Wade
  * @version 1.0 04/12/2008
  */
public class LwDbQueryResult {

	private Vector<Properties> result = null;		// the actual rows and columns for the query result
	private String returnType = "COLUMNS";
	
  /**
    * Will create a new LwDbQueryResult.
    */
	public LwDbQueryResult() {
	}

  /**
    * Will create a new LwDbQueryResult.
	* @param result the text explaining the error
    */
	public LwDbQueryResult(Vector<Properties> result, String returnType) {
		checkNullArgument(result);
		checkNullArgument(returnType);

		this.result = result;
		this.returnType = returnType;
	}

	/**
	  *
	  * Get the the name of the result
	  *
	  * @return a Vector of Properties representing the result
	  */
	public Vector<Properties> getResult() {
		return result;
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