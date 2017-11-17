package client;
import java.util.concurrent.TimeUnit;

import Helper.*;
import common.*;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;

import common.messages.TextMessage;

//import common.messages.KVMessage.StatusType;
import common.messages.KVMImplement;

import common.*;

import java.io.IOException;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.log4j.Logger;

import client.ClientSocketListener.SocketStatus;

import common.*;
public class KVStore extends Thread implements KVCommInterface {

	private Logger logger = Logger.getRootLogger();
	private Set<ClientSocketListener> listeners;
	private boolean running;

	private Socket clientSocket;
	private OutputStream output;
	private InputStream input;

	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 1024 * BUFFER_SIZE;

	private String address = null;

	private int port = 0;
	
	private Metadata localMeta = null;
	
	private Timer subTimer = null;				// the timer used for subscribe
	private SubscribeTimerTask task = null;		// the task to do upon timeout
	private HashMap<String, String> subscribe_list;		// list of keys to subscribe
	private MutableBoolean ableToRunSubscribe;	// indicate whether the program is able to run subscribe operations
	private MutableBoolean ableToRunOther;		// indicate whether the program is able to run other operations
	
	/**
	 * Initialize KVStore with address and port of KVServer
	 * 
	 * @param address
	 *            the address of the KVServer
	 * @param port
	 *            the port of the KVServer
	 */
	public KVStore(String address, int port) {
		this.address = address;
		this.port = port;
		listeners = new HashSet<ClientSocketListener>();
		this.subscribe_list = new HashMap<String, String>();
		this.ableToRunSubscribe = new MutableBoolean(true);
		this.ableToRunOther = new MutableBoolean(true);
		
		// add the subscribe thread here
		set_subscribe_task();
	}

	@Override
	public void connect() throws Exception {
		clientSocket = new Socket(this.address, this.port);
		listeners = new HashSet<ClientSocketListener>();

		output = clientSocket.getOutputStream();
		input = clientSocket.getInputStream();
		setRunning(true);
		logger.info("Connection established");

	}

	@Override
	public void disconnect() {
		subTimer.cancel();			// stop the subscribe timer
		closeConnection();
	}
	
	/**
	 * Put key,value into a KVMImplement object, send it to server, and wait for result
	 */
	@Override
	public KVMessage put(String key, String value) throws Exception {

		while (ableToRunOther.booleanValue() == false) {
			// spin lock if unable to run the function
		}
		// disable subscribe functionality
		ableToRunSubscribe.setFalse();
		
		if((key.length() > 20) || (value!=null && value.length() > 122880)){
			return null;
		}
		//for this, after we generate the correct ,information , we need to search for the correct host.
		
		//localmeta == null :first time client run, connected to a server, but does not have any metadata
		//sent request, don't calculate md5, probably end up with a server stop, then renew the metadata
		
		KVMImplement kvmi = null;
		int serverNotStart = 0;

		if(this.localMeta == null){
			if (value != null && value != "null") {				
				kvmi = new KVMImplement("PUT", key, value);
				byte[] msgBytes = kvmi.getmsgbyte();
				output.write(msgBytes, 0, msgBytes.length);
				output.flush();
				logger.info("ClientPush: " + key + "," + value + "\t");
			} else {				
				kvmi = new KVMImplement("DELETE_SUCCESS", key, value);
				byte[] msgBytes = kvmi.getmsgbyte();
				output.write(msgBytes, 0, msgBytes.length);
				output.flush();
				logger.info("ClientPush: " + key + "," + value + "\t");	
			}			
		}
		else{
			if (value != null && value != "null") {			
				kvmi = new KVMImplement("PUT", key, value);
				byte[] msgBytes = kvmi.getmsgbyte();
				output.write(msgBytes, 0, msgBytes.length);
				output.flush();
				logger.info("ClientPush: " + key + "," + value + "\t");
			} else {
				kvmi = new KVMImplement("DELETE_SUCCESS", key, value);
				byte[] msgBytes = kvmi.getmsgbyte();
				output.write(msgBytes, 0, msgBytes.length);
				output.flush();
				logger.info("ClientPush: " + key + "," + value + "\t");
	
			}
		}
		
		KVMImplement newMsg = null;
		int done = 0;
		while(done == 0) {
			//assume if server is not responsible or have stopped, the new metadata will be inside key string
			try {
				TextMessage latestMsg = receiveMessage();
				//System.out.println("Received is" + latestMsg.getMsg());
				
				
				newMsg = new KVMImplement(latestMsg);
				//System.out.println("received message: "+newMsg.getTextMessage().getMsg());
				
				if(newMsg.getStringStatus().equals("SERVER_NOT_RESPONSIBLE")
						|| newMsg.getStringStatus().equals("SERVER_STOPPED") 
						|| newMsg.getStringStatus().equals("SERVER_WRITE_LOCK")) {
					
					if (newMsg.getStringStatus().equals("SERVER_NOT_RESPONSIBLE")) {
						this.localMeta = new Metadata(newMsg.getKey());
					}
					
					if (newMsg.getStringStatus().equals("SERVER_STOPPED") 
							|| newMsg.getStringStatus().equals("SERVER_WRITE_LOCK")) {
						if(serverNotStart == 0 && newMsg.getStringStatus().equals("SERVER_STOPPED")){
							serverNotStart = 1;
							System.out.println("Server stopped. Please check that server is up and wait patiently...");
						}
						TimeUnit.MILLISECONDS.sleep(5000);
					}
				}
				else {
					done = 1;
					printResult(newMsg);
				}
			} catch (Exception e) {
				e.getCause();
				e.getStackTrace();
			}
			
			if (done == 0) {
				// need to resend
				if (newMsg.getStringStatus().equals("SERVER_STOPPED")||newMsg.getStringStatus().equals("SERVER_WRITE_LOCK")) {
					byte[] msgBytes = kvmi.getmsgbyte();
					output.write(msgBytes, 0, msgBytes.length);
					output.flush();

				}
				if(newMsg.getStringStatus().equals("SERVER_NOT_RESPONSIBLE")){
					//System.out.println("function called");
					findCoordServer(key);
					byte[] msgBytes = kvmi.getmsgbyte();
					output.write(msgBytes, 0, msgBytes.length);
					output.flush();

				}
			}
		}
		//System.out.println(newMsg.getStatus()== StatusType.PUT_SUCCESS);
		
		ableToRunSubscribe.setTrue();
		return newMsg;
	}
	
	/**
	 * get a tuple based on a given key
	 */
	@Override
	public KVMessage get(String key) throws Exception {

		while (ableToRunOther.booleanValue() == false) {
			// spin lock if unable to run the function
		}
		// disable subscribe functionality
		ableToRunSubscribe.setFalse();
		
		if((key.length() > 20)){
			return null;
		}
		
		KVMImplement kvmi = new KVMImplement("GET", key, null);
		byte[] msgBytes = kvmi.getmsgbyte();		
		if(this.localMeta == null){
			output.write(msgBytes, 0, msgBytes.length);
			output.flush();
			logger.info("ClientGet: " + key + "\t");	
			
		}
		else{
			output.write(msgBytes, 0, msgBytes.length);
			output.flush();
			logger.info("ClientGet: " + key + "\t");	
		}

		KVMImplement newMsg = null ;
		int done = 0 ;
		int serverNotStart = 0;
		
		while (done == 0){
		TextMessage latestMsg = receiveMessage();
		newMsg = new KVMImplement(latestMsg);
			if(newMsg.getStringStatus().equals("SERVER_NOT_RESPONSIBLE") || newMsg.getStringStatus().equals("SERVER_STOPPED")){
				if (newMsg.getStringStatus().equals("SERVER_NOT_RESPONSIBLE")) {
					this.localMeta = new Metadata(newMsg.getKey());
				}
				if(serverNotStart == 0 && newMsg.getStringStatus().equals("SERVER_STOPPED")){
					serverNotStart = 1;
					System.out.println("Server stopped. Please check if the server is up and wait patiently");
				}
				TimeUnit.MILLISECONDS.sleep(500);
			}
			else if(newMsg.getStringStatus().equals("SERVER_WRITE_LOCK")){
				System.out.println("It's a get request, should not block by a put");
			}
			else{
				done = 1;
				printResult(newMsg);
			}
		if(done == 0){
			if(newMsg.getStringStatus().equals("SERVER_NOT_RESPONSIBLE")){
				findReplicaServer(key);
			}
			
			output.write(msgBytes, 0, msgBytes.length);
			output.flush();
			
		}
		}
		
		ableToRunSubscribe.setTrue();
		return newMsg;
	}
	
	
	public KVMessage silentPut(String key, String value) throws Exception {
		
		while (ableToRunOther.booleanValue() == false) {
			// spin lock if unable to run the function
		}
		// disable subscribe functionality
		ableToRunSubscribe.setFalse();
		
		if((key.length() > 20) || (value!=null && value.length() > 122880)){
			return null;
		}
		//for this, after we generate the correct ,information , we need to search for the correct host.
		
		//localmeta == null :first time client run, connected to a server, but does not have any metadata
		//sent request, don't calculate md5, probably end up with a server stop, then renew the metadata
		
		KVMImplement kvmi = null;
		int serverNotStart = 0;

		if(this.localMeta == null){
			if (value != null && value != "null") {				
				kvmi = new KVMImplement("PUT", key, value);
				byte[] msgBytes = kvmi.getmsgbyte();
				output.write(msgBytes, 0, msgBytes.length);
				output.flush();
				logger.info("ClientPush: " + key + "," + value + "\t");
			} else {				
				kvmi = new KVMImplement("DELETE_SUCCESS", key, value);
				byte[] msgBytes = kvmi.getmsgbyte();
				output.write(msgBytes, 0, msgBytes.length);
				output.flush();
				logger.info("ClientPush: " + key + "," + value + "\t");	
			}			
		}
		else{
			if (value != null && value != "null") {			
				kvmi = new KVMImplement("PUT", key, value);
				byte[] msgBytes = kvmi.getmsgbyte();
				output.write(msgBytes, 0, msgBytes.length);
				output.flush();
				logger.info("ClientPush: " + key + "," + value + "\t");
			} else {
				kvmi = new KVMImplement("DELETE_SUCCESS", key, value);
				byte[] msgBytes = kvmi.getmsgbyte();
				output.write(msgBytes, 0, msgBytes.length);
				output.flush();
				logger.info("ClientPush: " + key + "," + value + "\t");
	
			}
		}
		
		KVMImplement newMsg = null;
		int done = 0;
		while(done == 0) {
			//assume if server is not responsible or have stopped, the new metadata will be inside key string
			try {
				TextMessage latestMsg = receiveMessage();
				//System.out.println("Received is" + latestMsg.getMsg());
				
				
				newMsg = new KVMImplement(latestMsg);
				//System.out.println("received message: "+newMsg.getTextMessage().getMsg());
				
				if(newMsg.getStringStatus().equals("SERVER_NOT_RESPONSIBLE")
						|| newMsg.getStringStatus().equals("SERVER_STOPPED") 
						|| newMsg.getStringStatus().equals("SERVER_WRITE_LOCK")) {
					
					if (newMsg.getStringStatus().equals("SERVER_NOT_RESPONSIBLE")) {
						this.localMeta = new Metadata(newMsg.getKey());
					}
					
					if (newMsg.getStringStatus().equals("SERVER_STOPPED") 
							|| newMsg.getStringStatus().equals("SERVER_WRITE_LOCK")) {
						if(serverNotStart == 0 && newMsg.getStringStatus().equals("SERVER_STOPPED")){
							serverNotStart = 1;
							System.out.println("Server stopped. Please check that server is up and wait patiently...");
						}
						TimeUnit.MILLISECONDS.sleep(5000);
					}
				}
				else {
					done = 1;
				}
			} catch (Exception e) {
				e.getCause();
				e.getStackTrace();
			}
			
			if (done == 0) {
				// need to resend
				if (newMsg.getStringStatus().equals("SERVER_STOPPED")||newMsg.getStringStatus().equals("SERVER_WRITE_LOCK")) {
					byte[] msgBytes = kvmi.getmsgbyte();
					output.write(msgBytes, 0, msgBytes.length);
					output.flush();

				}
				if(newMsg.getStringStatus().equals("SERVER_NOT_RESPONSIBLE")){
					//System.out.println("function called");
					findCoordServer(key);
					byte[] msgBytes = kvmi.getmsgbyte();
					output.write(msgBytes, 0, msgBytes.length);
					output.flush();

				}
			}
		}
		//System.out.println(newMsg.getStatus()== StatusType.PUT_SUCCESS);
		ableToRunSubscribe.setTrue();
		return newMsg;
	}
	
	public KVMessage silentGet(String key) throws Exception {
		
		while (ableToRunOther.booleanValue() == false) {
			// spin lock if unable to run the function
		}
		// disable subscribe functionality
		ableToRunSubscribe.setFalse();
		
		if((key.length() > 20)){
			return null;
		}
		
		KVMImplement kvmi = new KVMImplement("GET", key, null);
		byte[] msgBytes = kvmi.getmsgbyte();		
		if(this.localMeta == null){
			output.write(msgBytes, 0, msgBytes.length);
			output.flush();
			logger.info("ClientGet: " + key + "\t");	
			
		}
		else{
			output.write(msgBytes, 0, msgBytes.length);
			output.flush();
			logger.info("ClientGet: " + key + "\t");	
		}

		KVMImplement newMsg = null ;
		int done = 0 ;
		int serverNotStart = 0;
		
		while (done == 0){
		TextMessage latestMsg = receiveMessage();
		newMsg = new KVMImplement(latestMsg);
			if(newMsg.getStringStatus().equals("SERVER_NOT_RESPONSIBLE") || newMsg.getStringStatus().equals("SERVER_STOPPED")){
				if (newMsg.getStringStatus().equals("SERVER_NOT_RESPONSIBLE")) {
					this.localMeta = new Metadata(newMsg.getKey());
				}
				if(serverNotStart == 0 && newMsg.getStringStatus().equals("SERVER_STOPPED")){
					serverNotStart = 1;
					System.out.println("Server stopped. Please check if the server is up and wait patiently");
				}
				TimeUnit.MILLISECONDS.sleep(500);
			}
			else if(newMsg.getStringStatus().equals("SERVER_WRITE_LOCK")){
				System.out.println("It's a get request, should not block by a put");
			}
			else{
				done = 1;
			}
		if(done == 0){
			if(newMsg.getStringStatus().equals("SERVER_NOT_RESPONSIBLE")){
				findReplicaServer(key);
			}
			
			output.write(msgBytes, 0, msgBytes.length);
			output.flush();
			
		}
		}
		ableToRunSubscribe.setTrue();
		return newMsg;
	}


	/**
	 * close current connection
	 */
	public synchronized void closeConnection() {
		logger.info("try to close connection ...");

		try {
			tearDownConnection();
			for (ClientSocketListener listener : listeners) {
				listener.handleStatus(SocketStatus.DISCONNECTED);
			}
		} catch (IOException ioe) {
			logger.error("Unable to close connection!");
		}
	}

	private void tearDownConnection() throws IOException {
		setRunning(false);
		logger.info("tearing down the connection ...");
		if (clientSocket != null) {
			// input.close();
			// output.close();
			clientSocket.close();
			clientSocket = null;
			logger.info("connection closed!");
		}
	}

	/**
	 * receive byte array and parse it into string
	 * @return byte[]
	 * @throws IOException read invalid
	 */
	private TextMessage receiveMessage() throws IOException {

		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];

		/* read first char from stream */
		byte read = (byte) input.read();
		boolean reading = true;

		while (read != 13 && reading) {/* carriage return */
			/* if buffer filled, copy to msg array */
			if (index == BUFFER_SIZE) {
				if (msgBytes == null) {
					tmp = new byte[BUFFER_SIZE];
					System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
				} else {
					tmp = new byte[msgBytes.length + BUFFER_SIZE];
					System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
					System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
							BUFFER_SIZE);
				}

				msgBytes = tmp;
				bufferBytes = new byte[BUFFER_SIZE];
				index = 0;
			}

			/* only read valid characters, i.e. letters and numbers */
			// if((read > 31 && read < 127)) {
			bufferBytes[index] = read;
			index++;
			// }

			/* stop reading is DROP_SIZE is reached */
			if (msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
				reading = false;
			}

			/* read next char from stream */
			read = (byte) input.read();
		}

		if (msgBytes == null) {
			tmp = new byte[index];
			System.arraycopy(bufferBytes, 0, tmp, 0, index);
		} else {
			tmp = new byte[msgBytes.length + index];
			System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
			System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
		}

		msgBytes = tmp;

		/* build final String */
		TextMessage msg = new TextMessage(msgBytes);
		logger.info("Receive message: '" + msg.getMsg() + "'");
		return msg;
	}

	/**
	 * output proper result from the server
	 * @param msg KVMImplement msg class that received from server
	 */
	private void printResult(KVMImplement msg) {
		String state = msg.getStringStatus();
		if (state.equals("GET_SUCCESS")) {
			System.out.println(state + "<" + msg.getKey() + ","
					+ msg.getValue() + ">");
		} else if (state.equals("PUT_SUCCESS")) {
			System.out.println(state + "<" + msg.getKey() + ","
					+ msg.getValue() + ">");
		} else if (state.equals("GET_ERROR")) {
			System.out.println(state + "<" + msg.getKey() + ">");
		} else if (state.equals("PUT_ERROR")) {
			System.out.println(state + "<" + msg.getKey() + ","
					+ msg.getValue() + ">");
		} else if (state.equals("DELETE_SUCCESS")) {
			System.out.println(state + "<" + msg.getKey() + ">");
		} else if (state.equals("DELETE_ERROR")) {
			System.out.println(state + "<" + msg.getKey() + ">");
		} else if (state.equals("PUT_UPDATE")) {
			System.out.println(state + "<" + msg.getKey() + ","
					+ msg.getValue() + ">");
		} else {
			//System.out.println(msg.getStringStatus());
			System.out.println("Please reconnect to the server. Cannot connect automatically.");
		}

	}
	
	//assume we have metadata, find the corresponding server in the metadata
	private void findCoordServer(String key){
		// System.out.println("currentMeta: "+localMeta.sendMeta());

		Entry newServer = this.localMeta.searchCoord(Helper.getMD5String(key));
		System.out.println("\nBefore: " + this.address + ":" + this.port);
		closeConnection();
		if (newServer == null) {
			System.out.println("Error:cannot find correct server");
			return;
		} else {
			this.address = newServer.getAddressString();
			this.port = newServer.getPortInt();
		}
		try {
			System.out.println("Trying to connect to: " + this.address + ":"
					+ this.port + "...");
			connect();
			System.out.println("Connection Succeed\n");
		} catch (Exception e) {
			System.out.println("Cannot connect to new server");

		}
		
		
	}
	
	private void findReplicaServer(String key){
		// System.out.println("currentMeta: "+localMeta.sendMeta());

		Entry newServer = this.localMeta.searchReplica(Helper.getMD5String(key),null);
		System.out.println("\nBefore: " + this.address + ":" + this.port);
		closeConnection();
		if (newServer == null) {
			System.out.println("Error:cannot find correct server");
			return;
		} else {
			this.address = newServer.getAddressString();
			this.port = newServer.getPortInt();
		}
		try {
			System.out.println("Trying to connect to: " + this.address + ":"
					+ this.port + "...");
			connect();
			System.out.println("Connection Succeed\n");
		} catch (Exception e) {
			System.out.println("Cannot connect to new server");

		}
		
		
	}
	
//	public String findServer(){
//		
//		
//	}
	public void printConnectionMeta(){
		if(localMeta == null){
			System.out.println("Client meta is null");
		}
		else{
			System.out.println(this.localMeta.sendMeta());
		}
	}
	
	public void printConnectionString(){
		if(this.address == null){
			System.out.println(this.address+":"+this.port);
		}
		
	}
	
	public void setRunning(boolean run) {
		running = run;
	}

	public boolean isRunning() {
		return running;
	}

	public void addListener(ClientSocketListener listener) {
		listeners.add(listener);
	}

	public String getAddr() {
		return this.address;
	}

	public int getPort() {
		return this.port;
	}
	
	
	
	
	///////////////////// new functions /////////////////////

	public KVMImplement get_no_output(String key, boolean from_timetask) throws Exception {

		if ((key.length() > 20)) {
			return null;
		}

		KVMImplement kvmi = new KVMImplement("GET", key, null);
		byte[] msgBytes = kvmi.getmsgbyte();
		if (this.localMeta == null) {
			output.write(msgBytes, 0, msgBytes.length);
			output.flush();
		} else {
			output.write(msgBytes, 0, msgBytes.length);
			output.flush();
		}

		KVMImplement newMsg = null;
		int done = 0;
		int serverNotStart = 0;

		while (done == 0) {
			TextMessage latestMsg = receiveMessage();
			newMsg = new KVMImplement(latestMsg);
			if (newMsg.getStringStatus().equals("SERVER_NOT_RESPONSIBLE")
					|| newMsg.getStringStatus().equals("SERVER_STOPPED")) {
				if (newMsg.getStringStatus().equals("SERVER_NOT_RESPONSIBLE")) {
					this.localMeta = new Metadata(newMsg.getKey());
				}
				if (serverNotStart == 0
						&& newMsg.getStringStatus().equals("SERVER_STOPPED")) {
					serverNotStart = 1;
					// do not print out the message
					
					if (from_timetask == true) {
						// from TimerTask function
						// should give up the current checking and return to KVStore main loop
						
						return null;		// returning null indicates this situation in TimerTask
					}
					else {
						// from subscribe function
						// print out message to the user
						System.out.println("Server stopped. Please check if the server is up and wait patiently");
					}
					
				}
				TimeUnit.MILLISECONDS.sleep(500);
			} else if (newMsg.getStringStatus().equals("SERVER_WRITE_LOCK")) {
				System.out.println("It's a get request, should not be blocked by a write lock");
			} else {
				done = 1;
			}
			if (done == 0) {
				if (newMsg.getStringStatus().equals("SERVER_NOT_RESPONSIBLE")) {
					findReplicaServer_no_output(key);
				}
				output.write(msgBytes, 0, msgBytes.length);
				output.flush();
			}
		}
		return newMsg;
	}
	
	
	private void findReplicaServer_no_output(String key) {

		Entry newServer = this.localMeta.searchReplica(Helper.getMD5String(key), null);
		//System.out.println("\nBefore: " + this.address + ":" + this.port);
		closeConnection();
		if (newServer == null) {
			System.out.println("Error:cannot find correct server");
			return;
		} else {
			this.address = newServer.getAddressString();
			this.port = newServer.getPortInt();
		}
		try {
			//System.out.println("Trying to connect to: " + this.address + ":" + this.port + "...");
			connect();
			//System.out.println("Connection Succeed\n");
		} catch (Exception e) {
			System.out.println("Cannot connect to new server");
		}
	}
	

	private void set_subscribe_task() {
		this.task = new SubscribeTimerTask(this, subscribe_list, ableToRunSubscribe, ableToRunOther);
		this.subTimer = new Timer();

		// Set an initial delay of 5 second, then repeat every 3 seconds.
		subTimer.schedule(task, 5000, 3000);
	}

	public boolean subscribe(String key) throws Exception {
		
		while (ableToRunOther.booleanValue() == false) {
			// spin lock if unable to run the function
		}
		// disable subscribe functionality
		ableToRunSubscribe.setFalse();
		
		// try to access the key from the storage system
		KVMImplement msg = get_no_output(key, false);
		if (msg == null) {
			System.out.println("The key is too long!");
			ableToRunSubscribe.setTrue();
			return false;
		}
		
		String state = msg.getStringStatus();
		if (state.equals("GET_SUCCESS")) {
			// there is a key in storage system
			String value = msg.getValue();
			
			//change subscribe_list
			subscribe_list.put(key, value);
			ableToRunSubscribe.setTrue();
			return true;
		} else if (state.equals("GET_ERROR")) {
			// cannot get the key
			System.out.println("There is no key: <" + key + "> inside the system.");
			System.out.println("Please insert the key first.");
			ableToRunSubscribe.setTrue();
			return false;
		} else {
			// impossible situation
			System.out.println("Wrong!");
			ableToRunSubscribe.setTrue();
			return false;
		}
	}
	
	
	public boolean unsubscribe(String key) {
		
		while (ableToRunOther.booleanValue() == false) {
			// spin lock if unable to run the function
		}
		// disable subscribe functionality
		ableToRunSubscribe.setFalse();
		
		if (subscribe_list.remove(key) == null) {
			// the key was not subscribed before
			System.out.println("The key: <" + key + "> was not subscribed.");
			ableToRunSubscribe.setTrue();
			return false;
		}
		else {
			System.out.println("The key: <" + key + "> was unsubscribed.");
			ableToRunSubscribe.setTrue();
			return true;
		}
	}
	
}

class SubscribeTimerTask extends TimerTask {
	private KVStore kvstore;
	private HashMap<String, String> subscribe_list;		// list of keys to subscribe
	private MutableBoolean ableToRunSubscribe;
	private MutableBoolean ableToRunOther;
	
	public SubscribeTimerTask(KVStore client, HashMap<String, String> subscribe_list, MutableBoolean ableRunSub, MutableBoolean ableToRunOther) {
		this.kvstore = client;
		this.subscribe_list = subscribe_list;
		this.ableToRunSubscribe = ableRunSub;
		this.ableToRunOther = ableToRunOther;
	}
	
	public void run() {
		
		if (this.ableToRunSubscribe.booleanValue() == true) {
			ableToRunOther.setFalse();

			//System.out.println("Timer task executed.");
			
			// Create a list of deleted items
			ArrayList<String> to_delete = new ArrayList<String>();
			
			// Get a set of the entries.
			Set<Map.Entry<String, String>> set = this.subscribe_list.entrySet();
			
			for (Map.Entry<String, String> item : set) {
				// try to fetch the value from the server and compare with the local value
				String key = item.getKey();
				String local_value = item.getValue();
				String remote_value = null;
				
				// test
				//System.out.println("In list: key: <"+key+">; local_value: <"+local_value+">");
				
				// if the value of the key has been changed
				// update the local cache or delete the item in subscribe list
				KVMImplement msg = null;
				try {
					msg = kvstore.get_no_output(key, true);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				if (msg == null) {
					// Actually, impossible to have a too long key here
					// because the subscribe function has already checked it.
					// Receiving null means that the server is stopped.
					// Should give up the current checking and return back the control
					// to the KVStore main loop.
					
					ableToRunOther.setTrue();
					System.out.println("Server Stopped, give back the control.");
					System.out.println();
					return;
				}
				
				String state = msg.getStringStatus();
				if (state.equals("GET_SUCCESS")) {
					// there is a key in storage system
					remote_value = msg.getValue();
					
					if (local_value.equals(remote_value)) {
						// the values are the same
						// do nothing
					}
					else {
						// the value has been changed'
						// notify the user
						//final String PROMPT = "KVstoreClient> ";
						System.out.println("\n::::::::::::::: Notification :::::::::::::::");
						System.out.println("The value of key: <" + key +"> has been changed to <" + remote_value + ">");
						System.out.print("\nKVstoreClient> ");
						
						// update the local cache
						subscribe_list.put(key, remote_value);
					}
				} else if (state.equals("GET_ERROR")) {
					// cannot get the key
					System.out.println("\n::::::::::::::: Notification :::::::::::::::");
					System.out.println("The key: <" + key +"> has been deleted.");
					System.out.println("The system automatically unsubscribes the key: <"+key+">");
					System.out.print("\nKVstoreClient> ");
					
					// cannot delete here directly
					// need to delete outside the for loop
					to_delete.add(key);
				} else {
					// impossible situation
					System.out.println("Wrong!");
				}
			}
			
			// delete items if necessary
			for (String key : to_delete) {
				if (subscribe_list.remove(key) == null) {
					// Wrong !
					System.out.println("Wrong!");
				}
				else {
					// Correct !
				}
			}
			
			ableToRunOther.setTrue();
			//System.out.println();
		}
	}
}










