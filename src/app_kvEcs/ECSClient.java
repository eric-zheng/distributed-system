package app_kvEcs;

import java.lang.*;

import common.*;

import java.io.*;
import java.util.*;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.net.Socket;

import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import java.util.concurrent.CountDownLatch;

public class ECSClient {

	private ArrayList<String> activeList = new ArrayList<String>();
	private ArrayList<String> hibernationList = new ArrayList<String>();
	private boolean isRunning = false;
	private boolean initialized = false;

	private String workingDirectory = "";
	private String ip = "";

	private ZooKeeper zk = null;
	private CountDownLatch conn = null;
	private Process process = null;

	private final byte[] zero = new String("0").getBytes();
	private final byte[] one = new String("1").getBytes();

	private Metadata meta = null;

	public ECSClient() {
		System.out.print("Running ZooKeeper server...");
		/* start the zookeeper server */
		try {
			process = Runtime.getRuntime().exec("./zookeeper/bin/zkServer.sh start");
		} catch (Exception e) {
			System.out.println("FATAL: failed to start zookeeper server");
			shutdown();
		}
		System.out.println("OK");

		System.out.print("Connectiong to ZooKeeper server...");
		/* connect to zookeeper server */
		conn = new CountDownLatch(1);
		try {
			zk = new ZooKeeper("localhost:2181", 5000, new Watcher() {
				public void process(WatchedEvent event) {
					if (event.getState() == KeeperState.SyncConnected) {
						System.out.println("OK");
						conn.countDown();
					}
				}
			});
			conn.await();
		} catch (Exception e) {
			System.out.println("FATAL: failed to connect to zookeeper server");
			shutdown();
		}

		System.out.print("Reading configuration file...");
		Scanner sc = null;
		try {
			sc = new Scanner(new File("ecs.config"));
		} catch (FileNotFoundException e) {
			System.out.println("FATAL: ecs.config not found");
			shutdown();
		}
		System.out.println("OK");

		try {
			ip = InetAddress.getLocalHost().getHostAddress();

		} catch (UnknownHostException e) {
			e.printStackTrace();
		}

		/* load all the possible servers */
		System.out.print("Load server data...");
		try {

			process = Runtime.getRuntime().exec("pwd"); // get the current working directory
			BufferedReader strCon = new BufferedReader(new InputStreamReader(process.getInputStream()));
			String line;
			while ((line = strCon.readLine()) != null) {
				workingDirectory += line;
			}

			while (sc.hasNext()) {
				// load a tuple
				String name = sc.next();
				String host = sc.next();
				String port = sc.next();

				if (host.equals("127.0.0.1")) {
					host = ip;
				}

				String znodeName = host + ":" + port;

				hibernationList.add(znodeName); // initially all nodes are hibernated
			}
		} catch (Exception e) {
			System.out.println("FATAL: failed to load config");
			shutdown();
		}
		System.out.println("OK");

		System.out.print("Initializing essential configuration node...");

		try {
			if (zk.exists("/ack", false) == null) { // global ack path
				zk.create("/ack", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
			if (zk.exists("/metadata", false) == null) { // global metadata path
				zk.create("/metadata", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
			if (zk.exists("/alive", false) == null) { // alive nodes
				zk.create("/alive", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
		} catch (Exception e) {
			System.out.println("FAILED!");
			shutdown();
		}

		System.out.println("OK");

		System.out.print("Setting up alive watcher...");
		/* set up alive watcher */
		try {
			zk.getChildren("/alive", new Watcher() {
				public void process(WatchedEvent event) {
					/*
					 * try { zk.getChildren("/alive", this, null); } catch (KeeperException |
					 * InterruptedException e1) { // TODO Auto-generated catch block
					 * e1.printStackTrace(); }
					 */

					if (event.getType() == Event.EventType.NodeChildrenChanged) {
						if (event.getState() == Event.KeeperState.SyncConnected) {
							try {

								List<String> temp = zk.getChildren("/alive", false);

								// try
								try {
									zk.getChildren("/alive", this, null);
								} catch (KeeperException | InterruptedException e1) {
									// TODO Auto-generated catch block
									e1.printStackTrace();
								}

								ArrayList<String> children = new ArrayList<String>(temp);

								if (children.size() >= activeList.size()) {
									return;
								}

								System.out.println("Failure detected: trying to recover");

								System.out.print("Locking Servers...");
								for (String server : children) {
									lockServer(server);
								}
								System.out.println("OK");

								System.out.print("Trying To Recover...");

								recovery(children);

								System.out.println("OK");

								/* update metadata for everyone */
								System.out.print("Updating Metadata...");
								meta = new Metadata(children);
								zk.setData("/metadata", meta.sendMeta().getBytes(), -1);
								for (String actName : children) {
									updateMeta(actName);
								}
								System.out.println("OK");

								System.out.print("Unlocking Servers...");
								for (String server : children) {
									unlockServer(server);
								}
								System.out.println("OK");

								// find all the nodes that died and push them back to hibernation list
								int count = 0;
								for (String prevact : activeList) {
									boolean found = false;
									for (String curact : children) {
										if (prevact.equals(curact)) {
											found = true;
											break;
										}
									}
									if (!found) { // in prev not in current, put in hibernation list, and remove all
													// related nodes in the zookeeper
										hibernationList.add(prevact);
										String zname = prevact;
										zk.delete(("/" + zname + "/state"), -1);
										zk.delete(("/" + zname + "/shutdown"), -1);
										zk.delete(("/" + zname + "/lock"), -1);
										zk.delete(("/" + zname + "/updatemd"), -1);
										zk.delete(("/" + zname + "/migrate"), -1);
										zk.delete(("/" + zname + "/rmdata"), -1);
										zk.delete(("/" + zname), -1);
										count++;
									}
								}

								activeList = new ArrayList<String>();
								for (String s : children) {
									activeList.add(s);
								}

								// System.out.println(activeList);
								for (int i = 0; i < count; i++) {
									addNode(10, "FIFO");
								}
								System.out.println("");
								System.out.print(">");

							} catch (Exception e) {
								e.printStackTrace();
							}
						}
					}

				}
			}, null);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println("FAILED!");
			e.printStackTrace();
			shutdown();
		}
		System.out.println("OK");

		System.out.println("All setups are ready!");

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

	public boolean checkNode(String name) {
		try {
			while (zk.exists("/" + name + "/state", false) == null) {

			}
			while (zk.exists("/" + name + "/shutdown", false) == null) {

			}
			while (zk.exists("/" + name + "/updatemd", false) == null) {

			}
			while (zk.exists("/" + name + "/lock", false) == null) {

			}
			while (zk.exists("/" + name + "/migrate", false) == null) {

			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return true;

	}

	private boolean runServer(String name, int cSize, String policy) {
		if (addressFree(name) == false) {
			return false;
		}
		try {
			String host = name.split(":")[0];
			String port = name.split(":")[1];
			process = Runtime.getRuntime().exec("ssh -n " + host + " nohup java -jar " + workingDirectory
					+ "/ms3-server.jar " + port + " " + cSize + " " + policy + " " + name + " " + ip + ":2181 &");
		} catch (Exception e) {
			e.printStackTrace();
		}

		return checkNode(name);
	}

	public void waitForReply(int num) {
		try {
			while (zk.getChildren("/ack", false).size() != num) { // wait for everyone to reply
			}

			/* delete all children and reset ack */
			List<String> children = zk.getChildren("/ack", false);
			for (String child : children) {
				zk.delete("/ack/" + child, -1);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/* communication protocol section */

	/*------------------------the api section------------------------*/

	public boolean initService(int nodeNum, int cSize, String policy) {
		if (initialized) {
			System.out.println("System already initialized and cannot be initialized again!");
			return false;
		}

		if (hibernationList.size() < nodeNum) {
			System.out.println(
					"Not enough avaliable machines. Current avaliable number of machines: " + hibernationList.size());
			return false;
		}

		policy = policy.toUpperCase();
		if ((policy.equals("FIFO") == false) && (policy.equals("LRU") == false) && (policy.equals("LFU") == false)) {
			System.out.println("Invalid replacement policy: " + policy);
			return false;
		}
		if (cSize < 0) {
			System.out.println("Invalid cache size: " + cSize);
			return false;
		}
		if (nodeNum < 3) {
			System.out.println("Invalid node number: " + nodeNum);
			System.out.println("Node number is at least 3");
			return false;
		}

		int numStarted = 0;

		try {
			/* assemble active list */
			while (numStarted < nodeNum) {
				if (hibernationList.size() == 0) {
					System.out.println("WARNING: not enough server available");
					break;
				}
				String name = hibernationList.remove(hibernationList.size() - 1);
				if (runServer(name, cSize, policy)) { // successfully started the server
					numStarted++;
					activeList.add(name);
				} else {
					System.out.println("WARNING: Server " + name
							+ " is not avaliable. Please check if the address:port is available. Address removed from the configuration list.");
				}
			}

			/* calculate new metadata and upload it */
			meta = new Metadata(activeList);
			String metaString = meta.sendMeta();
			zk.setData("/metadata", metaString.getBytes(), -1);

		} catch (Exception e) {
			System.out.println("FATAL: failed to start kvserver");
			shutdown();
		}

		initialized = true;
		System.out.println(
				"Service initiated: nodeNum=" + numStarted + ", cacheSize=" + cSize + ", replacementPolicy=" + policy);
		return true;
	}

	public void start() {
		if (!initialized) {
			System.out.println("Service is not initialized!");
			return;
		}

		if (isRunning) {
			System.out.println("Service is already running!");
			return;
		}

		try {
			for (String name : activeList) {
				zk.setData("/" + name + "/state", one, -1);
			}
			waitForReply(activeList.size());
		} catch (Exception e) {
			System.out.println("WARNING: failed to set state to start");
			e.printStackTrace();
			return;
		}

		isRunning = true;
		System.out.println("Service successfully started!");
	}

	public void stop() {
		if (!initialized) {
			System.out.println("Service is not initialized!");
			return;
		}

		if (!isRunning) {
			System.out.println("Service is already stopped!");
			return;
		}

		try {
			for (String name : activeList) {
				zk.setData("/" + name + "/state", zero, -1);
			}
			waitForReply(activeList.size());
		} catch (Exception e) {
			System.out.println("WARNING: failed to set state to stop");
			e.printStackTrace();
			return;
		}

		isRunning = false;
		System.out.println("Service successfully stopped!");
	}

	public void shutdown() {
		/* communicate with all KVServer to shutdown */
		System.out.print("Shutting down All Server...");
		try {

			for (String name : activeList) {
				zk.setData("/" + name + "/shutdown", one, -1); // initiate a shutdown broadcast
			}

			waitForReply(activeList.size());
		} catch (Exception e) {
			System.out.println("FATAL: shutdown communication failed");
			e.printStackTrace();
		}

		System.out.println("OK");

		/* close zookeeper connection and shutdown the zookeeper server */
		System.out.print("Shutting down ZooKeeper Server...");

		try {
			zk.close();
		} catch (Exception e) {
			System.out.println("WARNING: failed to disconnect from zookeeper server");
		}

		try {
			process = Runtime.getRuntime().exec("./zookeeper/bin/zkServer.sh stop");
		} catch (Exception e) {
			System.out.println("FATAL: failed to stop zookeeper server");
			System.exit(0);
		}

		System.out.println("OK");
	}

	/*------------------------Server level control APIS-------------------------------*/
	public void startServer(String name) throws KeeperException, InterruptedException {
		zk.setData("/" + name + "/state", one, -1);
		waitForReply(1);
	}

	public void stopServer(String name) throws KeeperException, InterruptedException {
		zk.setData("/" + name + "/state", zero, -1);
		waitForReply(1);
	}

	public void lockServer(String name) throws KeeperException, InterruptedException {
		zk.setData("/" + name + "/lock", one, -1);
		waitForReply(1);
	}

	public void unlockServer(String name) throws KeeperException, InterruptedException {
		zk.setData("/" + name + "/lock", zero, -1);
		waitForReply(1);
	}

	public void moveData(String sender, String receiver, String startRange, String endRange)
			throws KeeperException, InterruptedException {
		String command = sender + "_" + receiver + "_" + startRange + "_" + endRange;
		zk.setData("/" + sender + "/migrate", command.getBytes(), -1);
		zk.setData("/" + receiver + "/migrate", command.getBytes(), -1);
		while (new String(zk.getData("/" + sender + "/migrate", false, null)).equals("0") == false) {
		}
	}

	public void updateMeta(String name) throws KeeperException, InterruptedException {
		zk.setData("/" + name + "/updatemd", one, -1);
		waitForReply(1);
	}

	public void shutdownServer(String name) throws KeeperException, InterruptedException {
		zk.setData("/" + name + "/shutdown", one, -1);
		waitForReply(1);
	}

	public void removeServerRange(String name, String startRange, String endRange)
			throws KeeperException, InterruptedException {
		String range = startRange + "_" + endRange;
		zk.setData("/" + name + "/rmdata", range.getBytes(), -1);
		while (new String(zk.getData("/" + name + "/rmdata", false, null)).equals("0") == false) {
		}
	}

	public void removeServerAll(String name) throws KeeperException, InterruptedException {
		removeServerRange(name, "00000000000000000000000000000000", "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF");
	}

	public void recovery(ArrayList<String> active) throws KeeperException, InterruptedException { // for every current
																									// node,asks all the
																									// other nodes for
																									// the data in its
																									// replication range
		ArrayList<String> currentAlive = new ArrayList<String>(zk.getChildren("/alive", false));
		if (active != null) {
			currentAlive = active;
		}

		meta = new Metadata(currentAlive);

		for (String receiver : currentAlive) {

			for (String sender : currentAlive) {
				if (receiver.equals(sender) == false) { // does not ask itself
					String range = meta.getReplicaRange(receiver);

					String[] parts = range.split(":");
					moveData(sender, receiver, parts[0], parts[1]);
				}
			}
		}

		// call add node

	}

	public void printDebug(String debug) {
		System.out.println("--------------DEBUG START--------------");
		System.out.println(debug);
		System.out.println("--------------DEBUG END--------------");
	}

	/*-------------------------------------------------------------------------*/

	public void addNode(int cSize, String policy) {
		if (!initialized) {
			System.out.println("Service is not initialized!");
			return;
		}

		if (hibernationList.size() == 0) {
			System.out.println("No available machine to use");
			return;
		}

		policy = policy.toUpperCase();
		if ((policy.equals("FIFO") == false) && (policy.equals("LRU") == false) && (policy.equals("LFU") == false)) {
			System.out.println("Invalid replacement policy: " + policy);
			return;
		}

		if (cSize < 0) {
			System.out.println("Invalid cache size: " + cSize);
			return;
		}

		/* write metadata to zookeeper(but is not broadcasted) */
		String name = null;

		int numStarted = 0;
		try {
			/* run and start the server */
			System.out.print("Starting Server...");

			while (numStarted < 1) {
				if (hibernationList.size() == 0) {
					System.out.println("WARNING: not enough server available");
					return;
				}

				name = hibernationList.remove(hibernationList.size() - 1);

				if (runServer(name, cSize, policy)) { // successfully started the server
					numStarted++;
					activeList.add(name);
				} else {
					System.out.println("WARNING: Server " + name
							+ " is not avaliable. Please check if the address:port is available. Address removed from the configuration list.");
				}
			}

			System.out.println("OK");

			meta = new Metadata(activeList);
			// System.out.println("new meta: "+meta.sendMeta());
			zk.setData("/metadata", meta.sendMeta().getBytes(), -1);

			if (isRunning == true) {
				System.out.print("Starting Service...");
				startServer(name);
				System.out.println("OK");
			}

			/* lock the servers */
			System.out.print("Locking Servers...");
			for (String server : activeList) {
				lockServer(server);
			}
			System.out.println("OK");

			/* start migration */
			System.out.print("Migrating Data...");
			recovery(activeList);
			System.out.println("OK");

			/* update metadata for everyone */
			System.out.print("Updating Metadata...");
			for (String actName : activeList) {
				updateMeta(actName);
			}
			System.out.println("OK");

			/* remove invalid data */
			for (String server : activeList) {
				String range = meta.getReplicaRange(server);
				String[] parts = range.split(":");
				removeServerRange(server, parts[1], parts[0]); // reversed so that remove everything not in range
			}

			/* release write lock of sender */
			System.out.print("Releasing Lock...");
			for (String server : activeList) {
				unlockServer(server);
			}
			System.out.println("OK");
		} catch (Exception e) {
			e.printStackTrace();
		}

		System.out.println("Node " + name + " added; with cacheSize=" + cSize + ", replacementPolicy=" + policy);

	}

	public void removeNode() {
		if (!initialized) {
			System.out.println("Service is not initialized!");
			return;
		}

		if (activeList.size() <= 3) {
			System.out.println("The system must have at least three machines to use. Stop");
			return;
		}

		// write metadata to zookeeper(but is not broadcasted)
		String name = activeList.remove(activeList.size() - 1);

		hibernationList.add(name);

		try {
			meta = new Metadata(activeList);
			zk.setData("/metadata", meta.sendMeta().getBytes(), -1);

			/* lock the servers */
			System.out.print("Locking Servers...");
			for (String server : activeList) {
				lockServer(server);
			}
			System.out.println("OK");

			/* start migration */
			System.out.print("Migrating Data...");
			recovery(activeList);
			System.out.println("OK");

			/* update metadata for everyone */
			System.out.print("Updating Metadata...");
			for (String actName : activeList) {
				updateMeta(actName);
			}
			System.out.println("OK");

			/* remove invalid data */
			System.out.print("Removing Data...");
			for (String server : activeList) {
				String range = meta.getReplicaRange(server);
				String[] parts = range.split(":");
				removeServerRange(server, parts[1], parts[0]); // reversed so that remove everything not in range
			}

			/* remove all data for deleted server */
			removeServerAll(name);
			System.out.println("OK");

			/* release write lock of sender */
			System.out.print("Releasing Lock...");
			for (String server : activeList) {
				unlockServer(server);
			}
			System.out.println("OK");

			// shutdown the sender
			System.out.print("Shutting Down Sender...");
			shutdownServer(name);
			System.out.println("OK");
		} catch (Exception e) {
			e.printStackTrace();
		}

		System.out.println("Node " + name + " removed.");

	}

	/*-----------------------end of api section------------------------*/

	/* main routine */
	public static void main(String[] args) {
		try {
			new LogSetup("logs/ecs/ecs.log", Level.OFF);
		} catch (Exception e) {
			System.out.println("WARNING: Failed to setup logger");
		}

		ECSClient ecs = new ECSClient();

		/* we are ready to begin the main loop */
		Scanner in = new Scanner(System.in);
		System.out.println("");
		System.out.println("---------- Usage ----------");
		System.out.println("Initialize Service: ");
		System.out.println("	init <number of nodes> <cache size> <cache replacement policy>");
		System.out.println("Start Service: ");
		System.out.println("	start");
		System.out.println("Stop Service: ");
		System.out.println("	stop");
		System.out.println("Add a Node: ");
		System.out.println("	add <cache size> <cache replacement policy>");
		System.out.println("Remove a Node: ");
		System.out.println("	remove");
		System.out.println("Shutdown The Service; ");
		System.out.println("	shutdown");

		while (true) {
			System.out.println("");
			System.out.print(">");
			String command = in.next();

			if (command.equals("shutdown")) {
				break;
			}

			if (command.equals("init")) {
				int nodeNum = in.nextInt();
				int cacheSize = in.nextInt();
				String replacementPolicy = in.next();
				ecs.initService(nodeNum, cacheSize, replacementPolicy);
			} else if (command.equals("start")) {
				ecs.start();
			} else if (command.equals("stop")) {
				ecs.stop();
			} else if (command.equals("add")) {
				int cacheSize = in.nextInt();
				String replacementPolicy = in.next();
				ecs.addNode(cacheSize, replacementPolicy);
			} else if (command.equals("remove")) {
				ecs.removeNode();
			} else if (command.equals("help")) {
				System.out.println("----------HELP MENU----------");
				System.out.println("Initialize Service: ");
				System.out.println("	init <number of nodes> <cache size> <cache replacement policy>");
				System.out.println("Start Service: ");
				System.out.println("	start");
				System.out.println("Stop Service: ");
				System.out.println("	stop");
				System.out.println("Add a Node: ");
				System.out.println("	add <cache size> <cache replacement policy>");
				System.out.println("Remove a Node: ");
				System.out.println("	remove");
				System.out.println("Shutdown The Service; ");
				System.out.println("	shutdown");
			} else {
				System.out.println("Invalid command. Type help to get command list");
			}
		}

		ecs.shutdown();
	}

	/* testing functions */
	public static void printprocess(Process process) throws Exception {
		BufferedReader strCon = new BufferedReader(new InputStreamReader(process.getInputStream()));
		String line;
		while ((line = strCon.readLine()) != null) {
			System.out.println("java print:" + line);
		}
	}
}
