package app_kvClient;

import java.io.BufferedReader;
import Table.Table;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;
import java.util.ArrayList;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import client.ClientSocketListener;
import client.KVStore;

import common.messages.KVMImplement;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;

public class KVClient implements ClientSocketListener {

	private static Logger logger = Logger.getRootLogger();
	private static final String PROMPT = "KVstoreClient> ";
	private BufferedReader stdin;
	private KVStore client = null;
	private boolean stop = false;

	private String serverAddress;
	private int serverPort;

	public void run() {

		// testing
		// client = new KVStore("127.0.0.1", 4214);
		// client.addListener(this);

		while (!stop) {
			stdin = new BufferedReader(new InputStreamReader(System.in));
			System.out.print(PROMPT);

			try {
				String cmdLine = stdin.readLine();
				this.handleCommand(cmdLine);
			} catch (IOException e) {
				stop = true;
				printError("CLI does not respond - Application terminated ");
			}
		}
	}

	/**
	 * major parser for handling command-line, it's a java shell
	 * 
	 * @param cmdLine command-line input
	 */
	private void handleCommand(String cmdLine) {
		// seperate all the stuff with strings
		String[] tokens = cmdLine.split("\\s+");

		if (tokens[0].equals("quit")) {
			stop = true;
			disconnect();
			System.out.println(PROMPT + "Application exit!");

		} else if (tokens[0].equals("connect")) {
			if (tokens.length == 3) {
				try {
					serverAddress = tokens[1];
					serverPort = Integer.parseInt(tokens[2]);
					connect(serverAddress, serverPort);
				} catch (NumberFormatException nfe) {
					printError("No valid address. Port must be a number!");
					logger.info("Unable to parse argument <port>", nfe);
				} catch (UnknownHostException e) {
					printError("Unknown Host!");
					logger.info("Unknown Host!", e);
				} catch (IOException e) {
					printError("Could not establish connection!");
					logger.warn("Could not establish connection!", e);
				}
			} else {
				printError("Invalid number of parameters!");
			}

		} else if (tokens[0].equals("put")) {
			if (tokens.length >= 2) {
				if (client != null && client.isRunning()) {
					StringBuilder msg = new StringBuilder();
					String key = tokens[1];
					for (int i = 2; i < tokens.length; i++) {
						msg.append(" ");
					}

					try {
						String[] input = cmdLine.split(" ", 3);

						if (input.length > 2) {
							putMessage(key, input[2]);
						} else {
							putMessage(key, null);
						}
					} catch (Exception e) {
						logger.error("Put Failed");
						e.printStackTrace();
					}

				} else {
					printError("Not connected!");
				}
			} else {
				printError("No value passed!");
			}

		} else if (tokens[0].equals("get")) {
			if (tokens.length == 2) {
				if (client != null && client.isRunning()) {
					try {
						getMessage(tokens[1]);
					} catch (Exception e) {
						logger.error("Get Failed");
					}
				} else {
					printError("Not connected!");
				}
			} else if (tokens.length < 2) {
				printError("No value passed!");
			} else {
				printError("Too many keys!");
			}
		} else if (tokens[0].equals("meta")) {
			client.printConnectionMeta();
		} else if (tokens[0].equals("current")) {
			client.printConnectionString();
		}

		else if (tokens[0].equals("disconnect")) {
			disconnect();

		} else if (tokens[0].equals("logLevel")) {
			if (tokens.length == 2) {
				String level = setLevel(tokens[1]);
				if (level.equals(LogSetup.UNKNOWN_LEVEL)) {
					printError("No valid log level!");
					printPossibleLogLevels();
				} else {
					System.out.println(PROMPT + "Log level changed to level " + level);
				}
			} else {
				printError("Invalid number of parameters!");
			}

		} else if (tokens[0].equals("help")) {
			printHelp();
		}

		/////////////////////////////////////////////////////////////////////////////
		else if (tokens[0].equals("subscribe")) {
			// subscribe a key
			if (tokens.length == 2) {
				if (client != null && client.isRunning()) {
					try {
						subscribe(tokens[1]);
					} catch (Exception e) {
						logger.error("Subscribe Failed");
					}
				} else {
					printError("Not connected! Please connect the server first!");
				}
			} else if (tokens.length < 2) {
				printError("Too few arguments! Please provide the key.");
			} else {
				printError("Too many arguments!");
			}
		} else if (tokens[0].equals("unsubscribe")) {
			// unsubscribe the key
			if (tokens.length == 2) {
				if (client != null && client.isRunning()) {
					try {
						unsubscribe(tokens[1]);
					} catch (Exception e) {
						logger.error("Get Failed");
					}
				} else {
					printError("Not connected! Please connect the server first!");
				}
			} else if (tokens.length < 2) {
				printError("Too few arguments! Please provide the key.");
			} else {
				printError("Too many arguments!");
			}
		} else if (tokens[0].equals("CREATE")) { // creating a table.
			// expected grammar: CREATE <tablename> <column names>
			if (tokens.length < 3) {
				printError("Invalid number of parameters!");
				return;
			}
			String tableName = tokens[1];
			ArrayList<String> col = new ArrayList<String>();

			for (int i = 2; i < tokens.length; i++) {
				col.add(tokens[i]);
			}

			Table t = new Table(tableName, col);
			silentPutMessage(t.getKey(), t.getValue());

		} else if (tokens[0].equals("INSERT")) { // add to a table
			// expected grammar: ADD <tablename> <row>
			if (tokens.length < 3) {
				printError("Invalid number of parameters!");
				return;
			}
			String tableName = tokens[1];
			ArrayList<String> row = new ArrayList<String>();

			for (int i = 2; i < tokens.length; i++) {
				row.add(tokens[i]);
			}

			String content = silentGetMessage(Table.tableNameToKey(tableName));
			if (content == null) {
				printError("Table does not exist!");
				return;
			}

			Table t = new Table(tableName, content);
			if (!t.add(row)) {
				printError("Row dimension does not match with table!");
				return;
			}

			t.printTable();

			silentPutMessage(t.getKey(), t.getValue());
		} else if (tokens[0].equals("SELECT")) { // select from a table, filter on it and store it back if necessary
			// expected grammar: SELECT <column names> FROM <table name> WHERE <filter
			// string> TOBE <table name>
			int i = 1;
			ArrayList<String> col = new ArrayList<String>();
			String tableName = null;
			String filter = null;
			String targetName = null;

			if (i >= tokens.length) {
				printError("Invalid number of parameters!");
				return;
			}

			while (!tokens[i].equals("FROM")) {
				col.add(tokens[i]);
				i++;
				if (i >= tokens.length) {
					printError("Invalid number of parameters!");
					return;
				}
			}

			i++;
			if (i >= tokens.length) {
				printError("Invalid number of parameters!");
				return;
			}
			tableName = tokens[i];

			i++;
			if (i < tokens.length && tokens[i].equals("WHERE")) {
				filter = "";
				i++;
				if (i >= tokens.length) {
					printError("Invalid number of parameters!");
					return;
				}
				while (i < tokens.length && !tokens[i].equals("TOBE")) {
					filter = filter + tokens[i] + " ";
					i++;
				}
			}

			if (i < tokens.length && tokens[i].equals("TOBE")) {
				i++;
				if (i >= tokens.length) {
					printError("Invalid number of parameters!");
					return;
				}
				targetName = tokens[i];
			}

			/* everything parsed. here we will do the actual work! */
			String content = silentGetMessage(Table.tableNameToKey(tableName));
			if (content == null) {
				printError("Table does not exist!");
				return;
			}

			Table t = new Table(tableName, content);

			if (!t.filter(col, filter)) {
				printError("Incorrect filter");
				return;
			}

			t.printTable();

			if (targetName != null) {
				silentPutMessage(Table.tableNameToKey(targetName), t.getValue());
			}
		} else if (tokens[0].equals("REMOVE")) {
			if (tokens.length < 2) {
				printError("Invalid number of parameters!");
				return;
			}

			String tableName = tokens[1];

			silentPutMessage(Table.tableNameToKey(tableName), null);

		} else {
			printError("Unknown command, type help for usage instruction");
		}
	}

	/**
	 * subscribe a given key in the system notify the client if the value of the key
	 * is changed or the key is deleted
	 * 
	 * @param key the key to subscribe
	 */
	private void subscribe(String key) {
		if (key.length() < 20) {
			try {
				if (client.subscribe(key)) {
					System.out.println("Subscribe key: <" + key + "> correctly.");
				} else {
					System.out.println("Subscribe failed.");
				}
			} catch (Exception e) {
				System.out.println("Subscribe error.");
				e.printStackTrace();
			}
		} else {
			System.out.println("Key is too long, max 20 bytes for key");
		}

	}

	/**
	 * unsubscribe a given key the client will not receive new notifications
	 * regarding the key
	 * 
	 * @param key
	 */
	private void unsubscribe(String key) {
		if (key.length() < 20) {
			try {
				if (client.unsubscribe(key)) {
					// System.out.println("Unsubscribe key: <" + key + "> correctly.");
				} else {
					System.out.println("Unsubscribe failed.");
				}
			} catch (Exception e) {
				System.out.println("Unsubscribe error.");
				e.printStackTrace();
			}
		} else {
			System.out.println("Key is too long, max 20 bytes for key");
		}

	}

	///////////////////////////////////////////////////////////////////////////////

	// connect to server
	/**
	 * set up connection to a given ip and port
	 * 
	 * @param address ip address
	 * @param port    port associate with the ip
	 * @throws UnknownHostException cause by creating the socket
	 * @throws IOException          cause if cannot set<ClientSocketListiner>
	 */
	private void connect(String address, int port) throws UnknownHostException, IOException {
		if (port > 1024 && port < 65535) {
			client = new KVStore(address, port);
			client.addListener(this);
			try {
				client.connect();
				System.out.println("Connection established at " + address + ":" + port);

			} catch (Exception e) {
				printError("start connection failed");
			}
		} else {
			System.out.println("Port number out of range");

		}
	}

	// disconnect
	/**
	 * disconnect from current connection
	 */
	private void disconnect() {
		if (client != null) {
			client.disconnect();
			System.out.println("Connection closed at " + client.getAddr() + ":" + client.getPort());
			client = null;
		}
	}

	/**
	 * print usage instruction
	 */
	// print usage instruction
	private void printHelp() {
		StringBuilder sb = new StringBuilder();
		sb.append(PROMPT).append("KVSTORE CLIENT HELP (Usage):\n");
		sb.append(PROMPT);
		sb.append("::::::::::::::::::::::::::::::::");
		sb.append("::::::::::::::::::::::::::::::::\n");
		sb.append(PROMPT).append("connect <host> <port>");
		sb.append("\t establishes a connection to a server\n");
		sb.append(PROMPT).append("get <key>");
		sb.append("\t get a tuple with given key\n");
		sb.append(PROMPT).append("put <key> <value>");
		sb.append("\t\t put a tuple to the server \n");
		sb.append(PROMPT).append("subscribe <key>"); // new command to add
		sb.append("\t\t subscribe a key and receive notification about the key \n");
		sb.append(PROMPT).append("unsubscribe <key>"); // new command to add
		sb.append("\t\t unsubscribe the key \n");
		sb.append(PROMPT).append("SELECT <columns> FROM <table name> WHERE <filter constrains> TOBE <new table name>"); // new
																														// command
																														// to
																														// add
		sb.append("\t\t select data from a table. WHERE and TOBE are optional. \n");

		sb.append(PROMPT).append("CREATE <table name> <columns>"); // new command to add
		sb.append("\t\t Create a table with <table name> and columns as <columns> \n");

		sb.append(PROMPT).append("INSERT <table name> <data>"); // new command to add
		sb.append("\t\t insert a row of data into <table name> \n");

		sb.append(PROMPT).append("REMOVE <table name>"); // new command to add
		sb.append("\t\t remove the table with <table name> \n");

		sb.append(PROMPT).append("disconnect");
		sb.append("\t\t\t disconnects from the server \n");
		sb.append(PROMPT).append("logLevel");
		sb.append("\t\t\t changes the logLevel \n");
		sb.append(PROMPT).append("\t\t\t\t ");
		sb.append("ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF \n");

		sb.append(PROMPT).append("quit ");
		sb.append("\t\t\t exits the program");
		System.out.println(sb.toString());
	}

	private void printPossibleLogLevels() {
		System.out.println(PROMPT + "Possible log levels are:");
		System.out.println(PROMPT + "ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
	}

	private String setLevel(String levelString) {

		if (levelString.equals(Level.ALL.toString())) {
			logger.setLevel(Level.ALL);
			return Level.ALL.toString();
		} else if (levelString.equals(Level.DEBUG.toString())) {
			logger.setLevel(Level.DEBUG);
			return Level.DEBUG.toString();
		} else if (levelString.equals(Level.INFO.toString())) {
			logger.setLevel(Level.INFO);
			return Level.INFO.toString();
		} else if (levelString.equals(Level.WARN.toString())) {
			logger.setLevel(Level.WARN);
			return Level.WARN.toString();
		} else if (levelString.equals(Level.ERROR.toString())) {
			logger.setLevel(Level.ERROR);
			return Level.ERROR.toString();
		} else if (levelString.equals(Level.FATAL.toString())) {
			logger.setLevel(Level.FATAL);
			return Level.FATAL.toString();
		} else if (levelString.equals(Level.OFF.toString())) {
			logger.setLevel(Level.OFF);
			return Level.OFF.toString();
		} else {
			return LogSetup.UNKNOWN_LEVEL;
		}
	}

	/**
	 * handle new message from server
	 */
	@Override
	public void handleNewMessage(KVMImplement msg) {

		if (!stop) {
			String state = msg.getStringStatus();
			if (state.equals("GET_SUCCESS")) {
				System.out.println(state + "<" + msg.getKey() + ">");
			} else if (state.equals("PUT_SUCCESS")) {
				System.out.println(state + "<" + msg.getKey() + "," + msg.getValue() + ">");
			} else if (state.equals("DELETE_SUCCESS")) {
				System.out.println(state + "<" + msg.getKey() + "," + msg.getValue() + ">");
			} else if (state.equals("GET_ERROR")) {
				System.out.println(state + "<" + msg.getKey() + ">");
			} else if (state.equals("PUT_ERROR")) {
				System.out.println(state + "<" + msg.getKey() + "," + msg.getValue() + ">");
			} else if (state.equals("DELETE_SUCCESS")) {
				System.out.println(state + "<" + msg.getKey() + ">");
			} else if (state.equals("DELETE_ERROR")) {
				System.out.println(state + "<" + msg.getKey() + ">");
			} else {
				System.out.println("Unrecognized state");
			}

			System.out.print(PROMPT);

		}

	}

	/**
	 * handleCurrent Status
	 */
	@Override
	public void handleStatus(SocketStatus status) {
		if (status == SocketStatus.CONNECTED) {

		} else if (status == SocketStatus.DISCONNECTED) {
			System.out.print(PROMPT);
			System.out.println("Connection terminated: " + serverAddress + " / " + serverPort);

		} else if (status == SocketStatus.CONNECTION_LOST) {
			System.out.println("Connection lost: " + serverAddress + " / " + serverPort);
			System.out.print(PROMPT);
		}

	}

	private void putMessage(String key, String value) {

		if (value == null) { // delete request
			try {

				client.put(key, value);
			} catch (Exception e) {
				e.getCause();
				e.getStackTrace();
				System.out.println("ClientPUT ERROR");
			}
		} else {
			if (key.length() <= 20 && value.length() <= 122880) {
				try {
					client.put(key, value);
				} catch (Exception e) {

					e.getCause();
					e.getStackTrace();
					System.out.println(e.getMessage() + e.getCause() + e.getStackTrace());
				}
			} else {
				System.out.println("Key or Value too large, max 20bytes for key, max 120kbytes for value");
			}
		}
	}

	private String getMessage(String key) {
		KVMessage message = null;
		if (key.length() < 20) {
			try {
				message = client.get(key);
			} catch (Exception e) {
				System.out.println("ClientGET ERROR");
				e.printStackTrace();
			}
		} else {
			System.out.println("Key too large, max 20bytes for key");

		}

		if (message == null || message.getStatus() == StatusType.GET_ERROR) {
			return null;
		}
		return message.getValue();
	}

	private void silentPutMessage(String key, String value) {

		if (value == null) { // delete request
			try {

				client.silentPut(key, value);
			} catch (Exception e) {
				e.getCause();
				e.getStackTrace();
				System.out.println("ClientPUT ERROR");
			}
		} else {
			if (key.length() <= 20 && value.length() <= 122880) {
				try {
					client.silentPut(key, value);
				} catch (Exception e) {

					e.getCause();
					e.getStackTrace();
					System.out.println(e.getMessage() + e.getCause() + e.getStackTrace());
				}
			} else {
				System.out.println("Key or Value too large, max 20bytes for key, max 120kbytes for value");
			}
		}
	}

	private String silentGetMessage(String key) {
		KVMessage message = null;
		if (key.length() < 20) {
			try {
				message = client.silentGet(key);
			} catch (Exception e) {
				System.out.println("ClientGET ERROR");
				e.printStackTrace();
			}
		} else {
			System.out.println("Key too large, max 20bytes for key");

		}

		if (message == null || message.getStatus() == StatusType.GET_ERROR) {
			return null;
		}
		return message.getValue();
	}

	private void printError(String error) {
		System.out.println(PROMPT + "Error! " + error);
		logger.error("Error:" + error);
	}

	/**
	 * Main entry point for the echo server application.
	 * 
	 * @param args contains the port number at args[0].
	 */
	public static void main(String[] args) {
		try {
			new LogSetup("logs/client/client.log", Level.ERROR);
			KVClient app = new KVClient();
			app.run();
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		}
	}

}
