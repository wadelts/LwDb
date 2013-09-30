package lw.db;

/**
  * Encapsulates exceptions resulting from errors returned from database activity.
  * @author Liam Wade
  * @version 1.0 18/11/2008
  */
public class LwDbException extends Exception
{
  /**
    * Will create a new exception.
    */
	public LwDbException() {
	}

  /**
    * Will create a new exception with the given reason.
	* @param reason the text explaining the error
    */
	public LwDbException(String reason) {
		super(reason);
		errorCode = 0;
	}

  /**
    * Will create a new exception with the given reason.
	* @param reason the text explaining the error
	* @param errorCode the error code associated with the exception
    */
	public LwDbException(String reason, int errorCode) {
		super(reason);
		this.errorCode = errorCode;
	}

	/**
	  *
	  * Get the last error code
	  *
	  * @return the last error code
	  */
	public int getErrorCode() {
		return errorCode;
	}

	private int errorCode = 0;		// aid to identifying actual problem
}