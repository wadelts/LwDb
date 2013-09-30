/**
 * 
 */
package lw.db.test;

import static org.junit.Assert.*;

import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import java.util.Vector;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import lw.db.LwDbConnection;
import lw.db.LwDbException;
import lw.db.LwDbQueryResult;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author wadel
 *
 */
public class TestDbConnectionPositiveTests {
	private static final Logger logger = Logger.getLogger("gemha");

	// Driver mysql-connector-java-5.1.26-bin.jar is in C:\Program Files (x86)\MySQL\Connector J 5.1.26
	// but I don't need that for maven compiles - see POM instead
	static private String connectionDriverName = "com.mysql.jdbc.Driver"; 
	static private String connectionURL = "jdbc:mysql://localhost:3306/users";
	static private String connectionUserName = "corpUser01";
	static private String connectionPassword = "dopple66ganger";

	@BeforeClass
	public static void performSetup() {
		String logLevel = System.getProperty("myProp.logLevel");
		if (logLevel != null) {
			// Set the logging level as per JVM command line...
			logger.setUseParentHandlers(false);  // Otherwise get duplicate messages on console
	
			logger.setLevel(Level.parse(logLevel));
	        
	        // PUBLISH this level - otherwise console maintains its level
			ConsoleHandler handler = new ConsoleHandler();
	        handler.setLevel(Level.parse(logLevel));
	        logger.addHandler(handler);
	
	        logger.info("Console logLevel set to " + logLevel);
		}
	}
	
	/**
	 * Test connecting to database and simple SELECT.
	 */
	@Test
	public void testLwDbConnectionFromClass() {
		LwDbConnection dbConn = null;
		try {
			dbConn = new LwDbConnection(connectionDriverName, connectionURL, connectionUserName, connectionPassword, false);
		} catch (LwDbException e) {
			fail("Couldn't connect. " + e);
		}
		
		Properties selectCols = new Properties();
		selectCols.put("groupId", "");
		selectCols.put("groupDescription", "");
		Properties whereCols = new Properties();
		LwDbQueryResult result = null;
		try {
			result = dbConn.getQueryResults("users.CorpGroup", selectCols, whereCols);
		} catch (LwDbException e) {
			fail("Couldn't SELECT CorpGroup data. " + e);
		}

		List<Properties> table = result.getResult();
		assertTrue(table.size() > 0);

		for ( Properties row : table) {
			String groupId = row.getProperty("GroupId");
			String groupDescription = row.getProperty("GroupDescription");
			if (groupId.equals("1")) {
				assertEquals("Technician User", groupDescription);
			}
		}

		dbConn.close(null);
	}

	/**
	 * Test direct INSERT, then DELETE of inserted row.
	 */
	@Test
	public void testDirectInsertAndDelete() {
		LwDbConnection dbConn = null;
		try {
			dbConn = new LwDbConnection(connectionDriverName, connectionURL, connectionUserName, connectionPassword, false);
		} catch (LwDbException e) {
			fail("Couldn't connect. " + e);
		}
		
		// INSERT
		final String insertGroupId = "999";
		Properties insertRow = new Properties();
		insertRow.put("groupId", insertGroupId);
		insertRow.put("GroupName", "'SOME_USER'");
		insertRow.put("groupDescription", "'Test Group - delme'");
		try {
			int numRowsProcessed = dbConn.insert("users.CorpGroup", insertRow);
			assertEquals("Incorrect number of rows inserted", 1, numRowsProcessed);
			dbConn.sessionCommit();
		} catch (LwDbException e) {
			fail("Couldn't INSERT into CorpGroup table. " + e);
		}
		
		// SELECT to test insert was successful
		Properties selectCols = new Properties();
		selectCols.put("groupId", "");
		selectCols.put("groupDescription", "");
		Properties whereCols = new Properties();
		LwDbQueryResult result = null;
		try {
			result = dbConn.getQueryResults("users.CorpGroup", selectCols, whereCols);
		} catch (LwDbException e) {
			fail("Couldn't SELECT CorpGroup data. " + e);
		}

		List<Properties> table = result.getResult();
		assertTrue(table.size() > 0);

		// Confirm inserted row exists...
		boolean testRowFound = false;
		int numRowsInTableAfterInsert = 0;		// remember number of rows after insert
		for ( Properties row : table) {
			numRowsInTableAfterInsert++;
			String groupId = row.getProperty("GroupId");
			String groupDescription = row.getProperty("GroupDescription");
			if (groupId.equals(insertGroupId)) {
				testRowFound = true;
				assertEquals("Test Group - delme", groupDescription);
			}
		}
		assertTrue("Never found Inserted row", testRowFound);

		// DELETE
		Properties deletetRowWhere = new Properties();
		deletetRowWhere.put("groupId", insertGroupId);
		try {
			int numRowsProcessed = dbConn.delete("users.CorpGroup", deletetRowWhere);
			assertEquals("Incorrect number of rows removed", 1, numRowsProcessed);
			dbConn.sessionCommit();
		} catch (LwDbException e) {
			fail("Couldn't DELETE from CorpGroup table. " + e);
		}

		
		// SELECT to test delete was successful
		try {
			result = dbConn.getQueryResults("users.CorpGroup", selectCols, whereCols);
		} catch (LwDbException e) {
			fail("Couldn't SELECT CorpGroup data. " + e);
		}

		table = result.getResult();
		assertTrue(table.size() == numRowsInTableAfterInsert-1);

		dbConn.close(null);
	}

	/**
	 * Test method for {@link lw.db.LwDbConnection#LwDbConnection(java.sql.Connection)}.
	 */
	@Test
	public void testLwDbConnectionConnection() {
		//fail("Not yet implemented");
	}

}
