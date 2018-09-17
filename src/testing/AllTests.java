package testing;

import java.io.IOException;

import org.apache.log4j.Level;

import app_kvServer.KVServer;
import junit.framework.Test;
import junit.framework.TestSuite;
import logger.LogSetup;
import app_kvEcs.ECSClient;

public class AllTests {

	static ECSClient ecs;

	static {
		try {
			new LogSetup("logs/testing/test.log", Level.ERROR);
			try {
				Runtime.getRuntime().exec("cleanup.sh").waitFor();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			ecs = new ECSClient();
			ecs.initService(3, 1, "FIFO");
			ecs.start();

			// use a default of 128 keys, FIFO replacement
			// new KVServer(50000, 128, "FIFO", "128.100.13.238:50000", "localhost:2181");
		} catch (IOException e) {
			e.printStackTrace();
			// ecs.shutdown();
		}
	}

	public static Test suite() {
		TestSuite clientSuite = new TestSuite("Basic Storage ServerTest-Suite");
		clientSuite.addTestSuite(ConnectionTest.class);
		clientSuite.addTestSuite(InteractionTest.class);
		clientSuite.addTestSuite(AdditionalTest.class);

		return clientSuite;
	}

}

/*
 * Another version of test code
 * 
 * package testing;
 * 
 * import org.junit.runner.RunWith; import org.junit.runners.Suite;
 * 
 * import junit.framework.JUnit4TestAdapter; import junit.framework.Test;
 * 
 * @RunWith(Suite.class)
 * 
 * @Suite.SuiteClasses({ AdditionalTest.class, ConnectionTest.class,
 * InteractionTest.class })
 * 
 * public class AllTests { public static Test suite() { return new
 * JUnit4TestAdapter(AllTests.class); } }
 */
