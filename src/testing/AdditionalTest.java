package testing;

import org.junit.Test;

import junit.framework.TestCase;

import client.KVStore;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;

import java.io.*;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

import client.KVStore;
import app_kvEcs.ECSClient;
import app_kvServer.KVServer;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;

import java.io.*;
import java.net.Socket;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import common.Metadata;

import common.Entry;

class testingThread extends Thread {
	private KVStore kvclient;
	private String key;
	private String value;

	public testingThread(KVStore c, String k, String v) {
		kvclient = c;
		key = k;
		value = v;
	}

	public void run() {
		try {
			for (int i = 0; i < 5; i++) {
				TimeUnit.MILLISECONDS.sleep(50);
				kvclient.put(key, value);
			}
		} catch (Exception e) {
			System.err.println("somethigns wrong!");
		}
	}
}

public class AdditionalTest extends TestCase {

	private KVStore kvClient1;
	private KVStore kvClient2;
	private KVStore kvClient3;

	static ECSClient ecs = AllTests.ecs;

	public void setUp() {

		kvClient1 = new KVStore("128.100.13.235", 52730);
		try {
			kvClient1.connect();
		} catch (Exception e) {
		}

		kvClient2 = new KVStore("128.100.13.235", 52730);
		try {
			kvClient2.connect();
		} catch (Exception e) {
		}

		kvClient3 = new KVStore("128.100.13.235", 52731);
		try {
			kvClient3.connect();
		} catch (Exception e) {
		}

	}

	public void tearDown() {

		kvClient1.disconnect();
		kvClient2.disconnect();
		kvClient3.disconnect();
		File db = new File("DBdata.dat");
		db.delete();
	}

	/**
	 * Test whether multiple clients can put <key, value> into one server.
	 */
	@Test
	public void testMultiplePut() {

		String key1 = "key1";
		String value1 = "v1";
		String key2 = "key2";
		String value2 = "v2";
		String key3 = "key3";
		String value3 = "v3";

		KVMessage response1 = null;
		KVMessage response2 = null;
		KVMessage response3 = null;
		Exception ex1 = null;
		Exception ex2 = null;
		Exception ex3 = null;

		try {
			response1 = kvClient1.put(key1, value1);
		} catch (Exception e) {
			ex1 = e;
		}

		try {
			response2 = kvClient1.put(key2, value2);
		} catch (Exception e) {
			ex2 = e;
		}

		try {
			response3 = kvClient1.put(key3, value3);
		} catch (Exception e) {
			ex3 = e;
		}

		assertTrue(ex1 == null && ex2 == null && ex3 == null
				&& (response1.getStatus() == StatusType.PUT_SUCCESS || response1.getStatus() == StatusType.PUT_UPDATE)
				&& (response2.getStatus() == StatusType.PUT_SUCCESS || response2.getStatus() == StatusType.PUT_UPDATE)
				&& (response3.getStatus() == StatusType.PUT_SUCCESS || response3.getStatus() == StatusType.PUT_UPDATE));
	}

	/**
	 * Test the correctness for multiple insertions from different clients. The
	 * server should be able to handle all data from various clients.
	 */
	@Test
	public void testMultipleUpdate() {
		String key1 = "key1";
		String value1 = "v1";
		String value2 = "v2";

		KVMessage response1 = null;
		KVMessage response2 = null;
		Exception ex = null;

		try {
			kvClient1.put(key1, value1); // client 1 put in one value
			response1 = kvClient2.put(key1, value2); // client 2 update the
														// value
			response2 = kvClient1.get(key1); // client 1 get the value, see if
												// it changed
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response1.getStatus() == StatusType.PUT_UPDATE && response1.getValue().equals(value2)
				&& response2.getStatus() == StatusType.GET_SUCCESS && response2.getValue().equals(value2));
	}

	/**
	 * Test for data consistency of the concurrent server.
	 * 
	 * 
	 */
	@Test
	public void testMultipleConnect() {
		testingThread t1 = new testingThread(kvClient1, "k1", "v1");

		try {
			t1.start();

			t1.join();

		} catch (InterruptedException e) {
		}

		KVMessage response1 = null;
		Exception ex = null;

		try {
			response1 = kvClient1.get("k1");
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response1.getStatus() == StatusType.GET_SUCCESS && response1.getValue().equals("v1"));

	}

	/**
	 * Test for the too long value. Should be put error.
	 */
	@Test
	public void testTooLongValue() {
		String key = "key";

		char[] data = new char[130000];
		String value = new String(data);

		KVMessage response = null;
		Exception ex = null;

		try {
			response = kvClient1.put(key, value);
		} catch (Exception e) {
			ex = e;
		}

		assertTrue(ex == null && response == null);
	}

	/**
	 * Test for the putting too long key. Should be put error.
	 */
	@Test
	public void testPutLongKey() {
		String value = "value";

		char[] data = new char[30];
		String key = new String(data);

		KVMessage response = null;
		Exception ex1 = null;

		try {
			response = kvClient3.put(key, value);
		} catch (Exception e) {
			ex1 = e;
		}

		assertTrue(ex1 == null && response == null);
	}

	//
	/**
	 * Test for the getting too long key. Should be get error.
	 */
	@Test
	public void testGetLongKey() {

		char[] data = new char[30];
		String key = new String(data);

		KVMessage response = null;
		Exception ex1 = null;

		try {
			response = kvClient1.get(key);
		} catch (Exception e) {
			ex1 = e;
		}

		assertTrue(ex1 == null && response == null);
	}

	//
	//
	//
	//
	/**
	 * Test for a key containing space.
	 */
	@Test
	public void testSpaceKey() {

		KVMessage response1 = null;
		KVMessage response2 = null;
		Exception ex1 = null;

		try {
			response1 = kvClient1.put("space check", "yes");
			response2 = kvClient1.get("space check");
		} catch (Exception e) {
			ex1 = e;
		}

		assertTrue(ex1 == null && response1.getStatus() == StatusType.PUT_SUCCESS
				&& response2.getStatus() == StatusType.GET_SUCCESS && response2.getValue().equals("yes"));
	}

	/**
	 * Test for a value containing space.
	 */
	@Test
	public void testSpaceValue() {

		KVMessage response1 = null;
		KVMessage response2 = null;
		Exception ex1 = null;

		try {
			response1 = kvClient1.put("spacecheck", "y e s");
			response2 = kvClient1.get("spacecheck");
		} catch (Exception e) {
			ex1 = e;
		}

		assertTrue(ex1 == null && response1.getStatus() == StatusType.PUT_SUCCESS
				&& response2.getStatus() == StatusType.GET_SUCCESS && response2.getValue().equals("y e s"));
	}

	/**
	 * Test to delete a key that doesn't exists
	 */
	@Test
	public void testWrongDelete() {
		KVMessage response1 = null;
		Exception ex1 = null;

		try {
			response1 = kvClient1.put("?????", null);
		} catch (Exception e) {
			ex1 = e;
		}

		assertTrue(ex1 == null && response1.getStatus() == StatusType.DELETE_ERROR);
	}

	@Test
	public void testValidLongValue() {
		KVMessage response1 = null;
		Exception ex1 = null;

		String value = new String();
		for (int i = 0; i < 1000; i++) {
			value = value + "**********";
		}

		try {
			response1 = kvClient1.put("reallyLongValue", value);
		} catch (Exception e) {
			ex1 = e;
		}

		assertTrue(ex1 == null && response1.getStatus() == StatusType.PUT_SUCCESS);
	}

	/*---------------------------ecs---------------------------*/
	@Test
	public void testInit() {

		assertTrue(addressFree("128.100.13.235:52730") == false && addressFree("128.100.13.235:52731") == false
				&& addressFree("128.100.13.235:52732") == false);
	}

	@Test
	public void testStart() {
		ecs.start();
		Exception e1 = null;
		KVMessage m = null;
		try {
			kvClient1.put("k1", "v1");
			m = kvClient1.get("k1");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			e1 = e;
		}

		assertTrue(e1 == null && m.getStatus() == StatusType.GET_SUCCESS && m.getValue().equals("v1"));
	}

	@Test
	public void testAdd() {
		ecs.addNode(10, "FIFO");
		while (addressFree("128.100.13.235:52733"))
			;
		KVStore temp = new KVStore("128.100.13.235", 52733);
		temp.disconnect();

		assertTrue(true);
	}

	@Test
	public void testRemove() {
		ecs.removeNode();
		assertTrue(addressFree("128.100.13.235:52733"));
	}

	@Test
	public void testReinitialization() {

		boolean reinitialized = ecs.initService(1, 1, "FIFO");

		assertTrue(reinitialized == false);
	}

	@Test
	public void testMetaTransportation() {
		ArrayList<String> r = new ArrayList<String>();
		r.add("128.100.13.234:52730");
		r.add("128.100.13.234:52731");
		r.add("128.100.13.234:52732");
		r.add("128.100.13.234:52733");

		Metadata ts = new Metadata(r);

		String rebuiltString = ts.sendMeta();
		String again = new Metadata(rebuiltString).sendMeta();

		assertTrue(rebuiltString.equals(again));
	}

	//
	// /**
	// * Create multiple clients
	// *
	// * @param num_clients
	// */
	// // public ArrayList<KVStore> connectMultiClients (int num_clients) {
	// // ArrayList<KVStore> rtn = new ArrayList<KVStore>();
	// //
	// // for (int i = 0; i < num_clients; i++) {
	// // rtn.add(new KVStore("localhost", (50000)));
	// // }
	// //
	// // return rtn;
	// // }
	//
	/*-----------------------M3--------------------------*/
	@Test
	public void testMetaConstructorOne() {

		ArrayList<String> r = new ArrayList<String>();
		r.add("128.100.13.234:52730");
		r.add("128.100.13.234:52731");
		r.add("128.100.13.234:52732");
		r.add("128.100.13.234:52733");

		Metadata ts = new Metadata(r);
		assertTrue(ts.sendMeta().equals(
				"128.100.13.234:52730:C4035BA5776F9556487296449F1BE645:06AB163B0D306B7F50CF61747A35C6BE:35FABBC631829A0C5DABD3F07F480D15:06AB163B0D306B7F50CF61747A35C6BE_128.100.13.234:52731:06AB163B0D306B7F50CF61747A35C6BF:35FABBC631829A0C5DABD3F07F480D14:4A1A2F0DB27A84D6364A93572A38D267:35FABBC631829A0C5DABD3F07F480D14_128.100.13.234:52732:35FABBC631829A0C5DABD3F07F480D15:4A1A2F0DB27A84D6364A93572A38D266:C4035BA5776F9556487296449F1BE645:4A1A2F0DB27A84D6364A93572A38D266_128.100.13.234:52733:4A1A2F0DB27A84D6364A93572A38D267:C4035BA5776F9556487296449F1BE644:06AB163B0D306B7F50CF61747A35C6BF:C4035BA5776F9556487296449F1BE644"));

	}

	@Test
	public void testMetaSend() {

		ArrayList<String> r = new ArrayList<String>();
		r.add("128.100.13.234:52730");
		r.add("128.100.13.234:52731");
		r.add("128.100.13.234:52732");
		r.add("128.100.13.234:52733");

		Metadata ts = new Metadata(r);
		assertTrue(ts.sendMeta().equals(
				"128.100.13.234:52730:C4035BA5776F9556487296449F1BE645:06AB163B0D306B7F50CF61747A35C6BE:35FABBC631829A0C5DABD3F07F480D15:06AB163B0D306B7F50CF61747A35C6BE_128.100.13.234:52731:06AB163B0D306B7F50CF61747A35C6BF:35FABBC631829A0C5DABD3F07F480D14:4A1A2F0DB27A84D6364A93572A38D267:35FABBC631829A0C5DABD3F07F480D14_128.100.13.234:52732:35FABBC631829A0C5DABD3F07F480D15:4A1A2F0DB27A84D6364A93572A38D266:C4035BA5776F9556487296449F1BE645:4A1A2F0DB27A84D6364A93572A38D266_128.100.13.234:52733:4A1A2F0DB27A84D6364A93572A38D267:C4035BA5776F9556487296449F1BE644:06AB163B0D306B7F50CF61747A35C6BF:C4035BA5776F9556487296449F1BE644"));

	}

	@Test
	public void testMetaConstructorTwo() {

		ArrayList<String> r = new ArrayList<String>();
		r.add("128.100.13.234:52730");
		r.add("128.100.13.234:52731");
		r.add("128.100.13.234:52732");
		r.add("128.100.13.234:52733");

		Metadata ts = new Metadata(r);

		Metadata ts2 = new Metadata(ts.sendMeta());
		assertTrue(ts.sendMeta().equals(ts2.sendMeta()));

	}

	@Test
	public void testNextServer() {

		ArrayList<String> r = new ArrayList<String>();
		r.add("128.100.13.234:52730");
		r.add("128.100.13.234:52731");
		r.add("128.100.13.234:52732");
		r.add("128.100.13.234:52733");

		Metadata ts = new Metadata(r);

		Entry et = ts.getNextServer("128.100.13.234:52730");
		assertTrue(et.getAddressPortString().equals("128.100.13.234:52731"));

		et = ts.getNextServer("128.100.13.234:52731");
		assertTrue(et.getAddressPortString().equals("128.100.13.234:52732"));

		et = ts.getNextServer("128.100.13.234:52732");
		assertTrue(et.getAddressPortString().equals("128.100.13.234:52733"));

		et = ts.getNextServer("128.100.13.234:52733");
		assertTrue(et.getAddressPortString().equals("128.100.13.234:52730"));

	}

	@Test
	public void testCoordRange() {

		ArrayList<String> r = new ArrayList<String>();
		r.add("128.100.13.234:52730");
		r.add("128.100.13.234:52731");
		r.add("128.100.13.234:52732");
		r.add("128.100.13.234:52733");

		Metadata ts = new Metadata(r);

		assertTrue(ts.requestCoordInRange("128.100.13.234:52733", "haha"));
		assertFalse(ts.requestCoordInRange("128.100.13.234:52733", "lala"));

		assertTrue(ts.requestCoordInRange("128.100.13.234:52730", "fafa"));
		assertFalse(ts.requestCoordInRange("128.100.13.234:52733", "lala"));

	}

	@Test
	public void testReplicaRange() {

		ArrayList<String> r = new ArrayList<String>();
		r.add("128.100.13.234:52730");
		r.add("128.100.13.234:52731");
		r.add("128.100.13.234:52732");
		r.add("128.100.13.234:52733");

		Metadata ts = new Metadata(r);

		assertTrue(ts.requestReplicaInRange("128.100.13.234:52733", "haha"));
		assertFalse(ts.requestReplicaInRange("128.100.13.234:52733", "fafa"));

		assertTrue(ts.requestReplicaInRange("128.100.13.234:52730", "fafa"));
		assertFalse(ts.requestReplicaInRange("128.100.13.234:52732", "haha"));

	}

	@Test
	public void testgetReplicaRange() {

		ArrayList<String> r = new ArrayList<String>();
		r.add("128.100.13.234:52730");
		r.add("128.100.13.234:52731");
		r.add("128.100.13.234:52732");
		r.add("128.100.13.234:52733");

		Metadata ts = new Metadata(r);

		assertTrue(ts.getReplicaRange("128.100.13.234:52730")
				.equals("35FABBC631829A0C5DABD3F07F480D15:06AB163B0D306B7F50CF61747A35C6BE"));
		assertTrue(ts.getReplicaRange("128.100.13.234:52731")
				.equals("4A1A2F0DB27A84D6364A93572A38D267:35FABBC631829A0C5DABD3F07F480D14"));
		assertTrue(ts.getReplicaRange("128.100.13.234:52732")
				.equals("C4035BA5776F9556487296449F1BE645:4A1A2F0DB27A84D6364A93572A38D266"));
		assertTrue(ts.getReplicaRange("128.100.13.234:52733")
				.equals("06AB163B0D306B7F50CF61747A35C6BF:C4035BA5776F9556487296449F1BE644"));

	}

	@Test
	public void testgetCoordRange() {

		ArrayList<String> r = new ArrayList<String>();
		r.add("128.100.13.234:52730");
		r.add("128.100.13.234:52731");
		r.add("128.100.13.234:52732");
		r.add("128.100.13.234:52733");

		Metadata ts = new Metadata(r);

		assertTrue(ts.getCoordRange("128.100.13.234:52730")
				.equals("C4035BA5776F9556487296449F1BE645:06AB163B0D306B7F50CF61747A35C6BE"));
		assertTrue(ts.getCoordRange("128.100.13.234:52731")
				.equals("06AB163B0D306B7F50CF61747A35C6BF:35FABBC631829A0C5DABD3F07F480D14"));
		assertTrue(ts.getCoordRange("128.100.13.234:52732")
				.equals("35FABBC631829A0C5DABD3F07F480D15:4A1A2F0DB27A84D6364A93572A38D266"));
		assertTrue(ts.getCoordRange("128.100.13.234:52733")
				.equals("4A1A2F0DB27A84D6364A93572A38D267:C4035BA5776F9556487296449F1BE644"));

	}

	@Test
	public void testEventualConsistency() {
		Exception e1 = null;
		KVMessage m = null;
		try {
			kvClient1.put("event", "123");
			TimeUnit.MILLISECONDS.sleep(5000);
			m = kvClient3.get("event");

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			e1 = e;
		}

		assertTrue(e1 == null && m.getStatus() == StatusType.GET_SUCCESS && m.getValue().equals("123"));
	}

	@Test
	public void testRevive() {
		Exception e1 = null;
		try {
			Runtime.getRuntime().exec("ssh -n 128.100.13.235 nohup 'lsof -t -i:52732 | xargs kill'").waitFor();
			TimeUnit.MILLISECONDS.sleep(10000);
		} catch (InterruptedException | IOException e) {
			e.printStackTrace();
			e1 = e;
		}

		assertTrue(addressFree("128.100.13.235:52732") == false && e1 == null);

	}

	public static boolean addressFree(String address) {
		String[] parts = address.split(":");
		String host = parts[0];
		int port = Integer.parseInt(parts[1]);
		Socket s = null;
		try {
			s = new Socket(host, port);
			return false;
		} catch (IOException e) {
			return true;
		} finally {
			if (s != null) {
				try {
					s.close();
				} catch (IOException e) {
					System.out.println("ERROR: port availability exception");
				}
			}
		}
	}

	// /*********************** for ECS *******************/
	//
	//
	// /*************************** function to load data set */
	//
	// public static ArrayList<Pair> recur(ArrayList<Pair> hehe, String path) {
	//
	// System.out.println("Path is" + path);
	//
	// File folder = new File(path);
	// File[] listOfFiles = folder.listFiles();
	//
	// for (int i = 0; i < listOfFiles.length; i++) {
	// if (listOfFiles[i].isFile()) {
	// System.out.println("File " + listOfFiles[i].getName());
	// hehe.add(new Pair(path + "/" + listOfFiles[i].getName(),
	// readFile(path + "/" + listOfFiles[i].getName(),
	// StandardCharsets.UTF_8)));
	// } else if (listOfFiles[i].isDirectory()) {
	// String newPath = path + "/" + listOfFiles[i].getName();
	// System.out.println("Directory " + listOfFiles[i].getName());
	// return recur(hehe, newPath);
	//
	// }
	// }
	//
	// return null;
	// }
	//
	// static String readFile(String path, Charset encoding) {
	// byte[] encoded = null;
	// try {
	// encoded = Files.readAllBytes(Paths.get(path));
	// } catch (IOException e) {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// }
	// return new String(encoded, encoding);
	// }

}

class Pair {
	String key;
	String value;

	Pair(String key, String value) {
		this.key = key;
		this.value = value;

	}

	public void setKey(String input) {
		this.key = input;
	}

	public void setValue(String input) {
		this.value = input;
	}

	public String getPair() {
		return "<" + key + "," + value + ">";
	}

}
