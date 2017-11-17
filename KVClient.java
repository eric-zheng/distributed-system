package app_kvClient;

import java.io.BufferedReader;





import java.io.IOException;
import java.io.InputStreamReader;
import java.net.UnknownHostException;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import client.ClientSocketListener;
import client.KVStore;


//import common.messages.TextMessage;
//import common.messages.KVMessage.StatusType;
import common.messages.KVMImplement;


public class KVClient implements ClientSocketListener {


	private static Logger logger = Logger.getRootLogger();
	private static final String PROMPT = "KVstoreClient> ";
	private BufferedReader stdin;
	private KVStore client = null;
	private boolean stop = false;
	
	private String serverAddress;
	private int serverPort;
	
	public void run() {
		while(!stop) {
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
	 * @param cmdLine command-line input
	 */
	private void handleCommand(String cmdLine) {
		//seperate all the stuff with strings 
		String[] tokens = cmdLine.split("\\s+");

		if(tokens[0].equals("quit")) {	
			stop = true;
			disconnect();
			System.out.println(PROMPT + "Application exit!");
		
		} else if (tokens[0].equals("connect")){
			if(tokens.length == 3) {
				try{
					serverAddress = tokens[1];
					serverPort = Integer.parseInt(tokens[2]);
					connect(serverAddress, serverPort);
				} catch(NumberFormatException nfe) {
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
			
		} 	
		else if (tokens[0].equals("put")){
			if(tokens.length >= 2 ){
				if(client != null && client.isRunning()){
					StringBuilder msg = new StringBuilder();
					String key = tokens[1];
					for (int i = 2; i<tokens.length;i++){
						msg.append(" ");
					}

						try {
							String[] input = cmdLine.split(" ", 3);

							if(input.length > 2){
							putMessage(key,input[2]);}
							else{
							putMessage(key,null);	
							}
						} catch (Exception e) {
							logger.error("Put Failed");
							e.printStackTrace();
						}

					
				}else{
					printError("Not connected!");
				}
			}else{
				printError("No value passed!");
			}
			
		}
		else if (tokens[0].equals("get")){
			if(tokens.length == 2 ){
				if(client != null && client.isRunning()){
					try {
						getMessage(tokens[1]);
					} catch (Exception e) {
						logger.error("Get Failed");
					}
				}else{
					printError("Not connected!");
				}
			}else if (tokens.length < 2){
				printError("No value passed!");
			}
			else{
				printError("Too many keys!");
			}
			
		}
		
		
		
		else if(tokens[0].equals("disconnect")) {
			disconnect();
			
		} else if(tokens[0].equals("logLevel")) {
			if(tokens.length == 2) {
				String level = setLevel(tokens[1]);
				if(level.equals(LogSetup.UNKNOWN_LEVEL)) {
					printError("No valid log level!");
					printPossibleLogLevels();
				} else {
					System.out.println(PROMPT + 
							"Log level changed to level " + level);
				}
			} else {
				printError("Invalid number of parameters!");
			}
			
		} else if(tokens[0].equals("help")) {
			printHelp();
		} else {
			printError("Unknown command, type help for usage instruction");
		}
	}
	
	//connect to server
	/**
	 * set up connection to a given ip and port
	 * @param address ip address
	 * @param port	port associate with the ip
	 * @throws UnknownHostException cause by creating the socket
	 * @throws IOException cause if cannot set<ClientSocketListiner>
	 */
	private void connect(String address, int port) 
			throws UnknownHostException, IOException {
		if(port >1024 && port <65535){
		client = new KVStore(address, port);
		client.addListener(this);
		try {
			client.connect();
			System.out.println("Connection established at "+address+":"+port);
		} catch (Exception e) {
			printError("start connection failed");
		}
		}
		else{
			System.out.println("Port number out of range");
			
		}
	}
	
	//disconnect
	/**
	 * disconnect from current connection
	 */
	private void disconnect() {
		if(client != null) {
			client.disconnect();
			System.out.println("Connection closed at "+ client.getAddr()+":"+client.getPort());
			client = null;
		}
	}
	/**
	 * print usage instruction
	 */
	//print usage instruction
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
		System.out.println(PROMPT 
				+ "Possible log levels are:");
		System.out.println(PROMPT 
				+ "ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF");
	}

	private String setLevel(String levelString) {
		
		if(levelString.equals(Level.ALL.toString())) {
			logger.setLevel(Level.ALL);
			return Level.ALL.toString();
		} else if(levelString.equals(Level.DEBUG.toString())) {
			logger.setLevel(Level.DEBUG);
			return Level.DEBUG.toString();
		} else if(levelString.equals(Level.INFO.toString())) {
			logger.setLevel(Level.INFO);
			return Level.INFO.toString();
		} else if(levelString.equals(Level.WARN.toString())) {
			logger.setLevel(Level.WARN);
			return Level.WARN.toString();
		} else if(levelString.equals(Level.ERROR.toString())) {
			logger.setLevel(Level.ERROR);
			return Level.ERROR.toString();
		} else if(levelString.equals(Level.FATAL.toString())) {
			logger.setLevel(Level.FATAL);
			return Level.FATAL.toString();
		} else if(levelString.equals(Level.OFF.toString())) {
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
	public void handleNewMessage(KVMImplement msg){

		if(!stop){
			String state = msg.getStringStatus();
			if(state.equals("GET_SUCCESS")){
				System.out.println(state+"<"+msg.getKey()+">");			
			}
			else if (state.equals("PUT_SUCCESS")){
				System.out.println(state+"<"+msg.getKey()+","+msg.getValue()+">");		
			}
			else if (state.equals("DELETE_SUCCESS")){
				System.out.println(state+"<"+msg.getKey()+","+msg.getValue()+">");				
			}
			else if(state.equals("GET_ERROR")){
				System.out.println(state+"<"+msg.getKey()+">");
			}
			else if(state.equals("PUT_ERROR")){
				System.out.println(state+"<"+msg.getKey()+","+msg.getValue()+">");
			}
			else if(state.equals("DELETE_SUCCESS")){
				System.out.println(state+"<"+msg.getKey()+">");
			}
			else if(state.equals("DELETE_ERROR")){
				System.out.println(state+"<"+msg.getKey()+">");
			}
			else{
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
		if(status == SocketStatus.CONNECTED) {

		} else if (status == SocketStatus.DISCONNECTED) {
			System.out.print(PROMPT);
			System.out.println("Connection terminated: " 
					+ serverAddress + " / " + serverPort);
			
		} else if (status == SocketStatus.CONNECTION_LOST) {
			System.out.println("Connection lost: " 
					+ serverAddress + " / " + serverPort);
			System.out.print(PROMPT);
		}
		
	}

	private void putMessage(String key, String value) {
		
		if (value == null) {	// delete request
			try {
				client.put(key,value);
			} catch (Exception e) {
				e.getCause();
				e.getStackTrace();
				System.out.println("ClientPUT ERROR");
			}
		}
		else {
			if(key.length() <= 20 && value.length() <= 122880) {
				try {
					client.put(key,value);
				} catch (Exception e) {
					e.getCause();
					e.getStackTrace();
					System.out.println("ClientPUT ERROR");
				}
			}
			else{
				System.out.println("Key or Value too large, max 20bytes for key, max 120kbytes for value");
			}
		}
	}
	
	private void getMessage(String key){
		if(key.length()<20){
			try {
				client.get(key);
			} catch (Exception e) {
				System.out.println("ClientGET ERROR");
			}
		}
		else{
			System.out.println("Key too large, max 20bytes for key");	
			
		}
		
	}
	
	private void printError(String error){
		System.out.println(PROMPT + "Error! " +  error);
		logger.error("Error:" + error);
	}
	

    /**
     * Main entry point for the echo server application. 
     * @param args contains the port number at args[0].
     */
    public static void main(String[] args) {
    	try {
			new LogSetup("logs/client.log", Level.ERROR);
			KVClient app = new KVClient();
			app.run();
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		}
    }

}
