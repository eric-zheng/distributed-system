package app_kvServer;

import client.ClientSocketListener;
import common.Entry;
import common.Metadata;
import common.messages.*;
import Helper.Helper;

import common.messages.KVMessage.StatusType;
import common.messages.KVMImplement;
import common.messages.TextMessage;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.log4j.*;

import storage.DBResponse;
import storage.Storage;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

/**
 * Connection end point for each client connected to server. Used to receive,
 * process and send data with client.
 */
public class ClientConnection implements Runnable {

	private static Logger logger = Logger.getRootLogger();

	// the KVServer name corresponding to this thread
	// format: <IP>:<port number>
	private String serverName;

	private boolean isOpen;
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 128 * BUFFER_SIZE;

	private Socket clientSocket;
	private InputStream input;
	private OutputStream output;
	private Storage s;

	private MutableBoolean isLocked;
	private MutableBoolean isRunning;
	private Metadata md;

	// pass in the zk
	private ZooKeeper zk;

	/**
	 * Constructs a new CientConnection object for a given TCP socket.
	 * 
	 * @param clientSocket the Socket object for the client connection.
	 */
	public ClientConnection(Socket clientSocket, Storage s, MutableBoolean isLocked, MutableBoolean isRunning,
			Metadata md, String zname, ZooKeeper zk) {
		this.clientSocket = clientSocket;
		this.isOpen = true;
		this.s = s;
		this.isLocked = isLocked;
		this.isRunning = isRunning;
		this.md = md;
		this.serverName = zname;

		this.zk = zk;
	}

	private boolean isRunning() {
		return this.isRunning.booleanValue();
	}

	private boolean isLocked() {
		return this.isLocked.booleanValue();
	}

	void debug_put_data_on_znode() {

		String start_range = "00000000000000000000000000000000";
		String end_range = "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF";

		// create the sending message
		ArrayList<DBResponse> result = null;
		try {
			result = s.getInRange(start_range, end_range);
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		String allData = "";

		for (DBResponse data : result) {
			// send each data in result array
			allData = allData + data.key + ":" + data.value + " - ";
		}

		try {
			if (zk.exists("/" + serverName + "/debug", false) != null) {
				// remove the debug node if it exists previously
				zk.delete("/" + serverName + "/debug", -1);
			}

			zk.create(("/" + serverName + "/debug"), null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			zk.setData(("/" + serverName + "/debug"), allData.getBytes(), -1);
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Initializes and starts the client connection. Loops until the connection is
	 * closed or aborted by the client.
	 */
	public void run() {
		try {
			output = clientSocket.getOutputStream();
			input = clientSocket.getInputStream();

			while (isOpen) {
				try {
					TextMessage latestMsg = receiveMessage();

					if (latestMsg.getMsg().equals("")) {
						// disconnect
						isOpen = false;
						break;
					}
					// Convert TextMessage to KVMessage
					KVMImplement kvmsg = new KVMImplement(latestMsg);

					// Check if the request is to move data
					// Highest priority
					if (kvmsg.getStatus().equals(StatusType.SERVER_STOPPED)
							|| kvmsg.getStatus().equals(StatusType.SERVER_WRITE_LOCK)) {

						// Request from another KVServer to move data
						KVMImplement rtnmsg = null;

						DBResponse result;
						if (kvmsg.getStatus().equals(StatusType.SERVER_WRITE_LOCK) || kvmsg.getValue() == null) {
							// DELETE request
							result = s.put(kvmsg.getKey(), null);

							if (result.success) {
								rtnmsg = new KVMImplement("DELETE_SUCCESS", kvmsg.getKey(), null);
							} else {
								rtnmsg = new KVMImplement("DELETE_ERROR", kvmsg.getKey(), null);
							}
						} else {
							// PUT request
							result = s.put(kvmsg.getKey(), kvmsg.getValue());

							if (result.success && result.isNewTuple) { // insert tuple successfully
								rtnmsg = new KVMImplement("PUT_SUCCESS", kvmsg.getKey(), kvmsg.getValue());
							} else if (result.success && !result.isNewTuple) { // update value successfully
								rtnmsg = new KVMImplement("PUT_UPDATE", kvmsg.getKey(), kvmsg.getValue());
							} else if (!result.success) { // put error
								rtnmsg = new KVMImplement("PUT_ERROR", kvmsg.getKey(), kvmsg.getValue());
							}
						}

						byte[] tempBytes = rtnmsg.getmsgbyte();
						try {
							output.write(tempBytes, 0, tempBytes.length);
							output.flush();
						} catch (IOException e) {
							e.printStackTrace();
						}

						// Return message to the sender
						// sendMessage(rtnmsg.getTextMessage());

						continue;
					} else {
						if (!isRunning()) {
							// server is stopped and not moving data
							KVMImplement rtnmsg = new KVMImplement("SERVER_STOPPED", null, null);
							latestMsg = null;
							TextMessage returnMsg = rtnmsg.getTextMessage();
							sendMessage(returnMsg);
							continue;
						} else if (isLocked() && (kvmsg.getStatus().equals(StatusType.PUT)
								|| kvmsg.getStatus().equals(StatusType.DELETE_SUCCESS))) {
							// write lock is active - send back message to client
							KVMImplement rtnmsg = new KVMImplement("SERVER_WRITE_LOCK", null, null);
							TextMessage returnMsg = rtnmsg.getTextMessage();
							latestMsg = null;
							sendMessage(returnMsg);
							continue;
						}
					}

					// Add log info
					if (kvmsg.getStatus().equals(StatusType.GET)) {
						logger.info("RECEIVE \t<" + clientSocket.getInetAddress().getHostAddress() + ":"
								+ clientSocket.getPort() + ">: '" + kvmsg.getStringStatus() + " " + kvmsg.getKey()
								+ "'");
					} else if (kvmsg.getStatus().equals(StatusType.DELETE_SUCCESS)) {
						logger.info("RECEIVE \t<" + clientSocket.getInetAddress().getHostAddress() + ":"
								+ clientSocket.getPort() + ">: '" + "DELETE" + " " + kvmsg.getKey() + "'");
					} else {
						logger.info("RECEIVE \t<" + clientSocket.getInetAddress().getHostAddress() + ":"
								+ clientSocket.getPort() + ">: '" + kvmsg.getStringStatus() + " " + kvmsg.getKey() + " "
								+ kvmsg.getValue() + "'");
					}

					// Call storage function provided by DB
					// Note to acquire lock before using them
					KVMImplement rtnmsg = null;

					synchronized (s) {

						// Need to check whether the object is in this server

						if (this.md.requestReplicaInRange(this.serverName, kvmsg.getKey())) {
							// within the replica range
							if (kvmsg.getStatus().equals(StatusType.GET)) {
								DBResponse result = s.get(kvmsg.getKey());

								if (result.success) {
									rtnmsg = new KVMImplement("GET_SUCCESS", kvmsg.getKey(), result.value);
								} else {
									rtnmsg = new KVMImplement("GET_ERROR", kvmsg.getKey(), null);
								}
							} else if (kvmsg.getStatus().equals(StatusType.PUT)
									&& this.md.requestCoordInRange(this.serverName, kvmsg.getKey())) {

								// Update itself data
								DBResponse result = s.put(kvmsg.getKey(), kvmsg.getValue());

								// Tell the replicas to put data
								boolean send_reps_success = sendDataToReplicas(false, kvmsg);

								if (send_reps_success) {
									if (result.success && result.isNewTuple) { // insert tuple successfully
										rtnmsg = new KVMImplement("PUT_SUCCESS", kvmsg.getKey(), kvmsg.getValue());
									} else if (result.success && !result.isNewTuple) { // update value successfully
										rtnmsg = new KVMImplement("PUT_UPDATE", kvmsg.getKey(), kvmsg.getValue());
									} else if (!result.success) { // put error
										rtnmsg = new KVMImplement("PUT_ERROR", kvmsg.getKey(), kvmsg.getValue());
									}
								} else {
									rtnmsg = new KVMImplement("PUT_ERROR", kvmsg.getKey(), kvmsg.getValue());
								}

							} else if (kvmsg.getStatus().equals(StatusType.DELETE_SUCCESS)
									&& this.md.requestCoordInRange(this.serverName, kvmsg.getKey())) {
								// Need to delete the tuple based on the key
								DBResponse result = s.put(kvmsg.getKey(), null);

								boolean send_reps_success = sendDataToReplicas(true, kvmsg);

								if (send_reps_success) {
									if (result.success) {
										rtnmsg = new KVMImplement("DELETE_SUCCESS", kvmsg.getKey(), null);
									} else {
										rtnmsg = new KVMImplement("DELETE_ERROR", kvmsg.getKey(), null);
									}
								}
							} else {
								// not in coord range but need to put or delete
								// not responsible
								KVMImplement rtmsg = new KVMImplement("SERVER_NOT_RESPONSIBLE", md.sendMeta(), null);
								TextMessage returnMsg = rtmsg.getTextMessage();
								sendMessage(returnMsg);
								continue;
							}
						} else {
							// This server doesn't responsible for the client request (read or write)
							KVMImplement rtmsg = new KVMImplement("SERVER_NOT_RESPONSIBLE", md.sendMeta(), null);
							TextMessage returnMsg = rtmsg.getTextMessage();
							sendMessage(returnMsg);
							continue;
						}
					}

					// debug function
					// debug_put_data_on_znode();

					// Return message to the client
					// convert KVMessage to TextMessage
					TextMessage returnMsg = rtnmsg.getTextMessage();
					sendMessage(returnMsg);
				} catch (IOException ioe) {
					logger.error("Error! Connection lost!");
					isOpen = false;
				}
			}

		} catch (IOException ioe) {
			logger.error("Error! Connection could not be established!", ioe);

		} finally {

			try {
				if (clientSocket != null) {
					input.close();
					output.close();
					clientSocket.close();
				}
			} catch (IOException ioe) {
				logger.error("Error! Unable to tear down connection!", ioe);
			}
			logger.info("Client disconnected.");
		}
	}

	/**
	 * Send/Delete the data to the following TWO replicas
	 */
	boolean sendDataToReplicas(boolean isDelete, KVMImplement kvmsg) {
		// get the name of next two replicas

		Entry rep1 = this.md.getNextServer(this.serverName);
		Entry rep2 = this.md.getNextServer((rep1.getAddressPortString()));

		String rep1_addr = rep1.getAddressString();
		String rep2_addr = rep2.getAddressString();

		int rep1_port = rep1.getPortInt();
		int rep2_port = rep2.getPortInt();

		boolean returnedValue = false;

		// build sockets and transfer data
		Socket rep1_socket = null, rep2_socket = null;
		OutputStream rep1_output = null, rep2_output = null;
		InputStream rep1_input = null, rep2_input = null;
		Set<ClientSocketListener> rep1_listeners, rep2_listeners;

		try {
			rep1_socket = new Socket(rep1_addr, rep1_port);
			rep2_socket = new Socket(rep2_addr, rep2_port);
			rep1_listeners = new HashSet<ClientSocketListener>();
			rep2_listeners = new HashSet<ClientSocketListener>();
			rep1_output = rep1_socket.getOutputStream();
			rep2_output = rep2_socket.getOutputStream();
			rep1_input = rep1_socket.getInputStream();
			rep2_input = rep2_socket.getInputStream();
		} catch (IOException e) {
			e.printStackTrace();
		}

		// create the sending message
		KVMImplement data_to_send;
		if (!isDelete) {
			// put or update data
			data_to_send = new KVMImplement("SERVER_STOPPED", kvmsg.getKey(), kvmsg.getValue());
		} else {
			// delete data
			data_to_send = new KVMImplement("SERVER_WRITE_LOCK", kvmsg.getKey(), null);
		}

		byte[] msgBytes = data_to_send.getmsgbyte();
		try {
			rep1_output.write(msgBytes, 0, msgBytes.length);
			rep1_output.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}

		TextMessage ackMsg1 = null;
		try {
			ackMsg1 = receiveMessage_withinput(rep1_input);
		} catch (IOException e) {
			e.printStackTrace();
		}

		KVMImplement ack1 = new KVMImplement(ackMsg1);
		if (!(ack1.getStatus().equals(StatusType.PUT_ERROR))) {

			// correct response from the receiver
			// move next data
			try {
				rep2_output.write(msgBytes, 0, msgBytes.length);
				rep2_output.flush();
			} catch (IOException e) {
				e.printStackTrace();
			}

			TextMessage ackMsg2 = null;
			try {
				ackMsg2 = receiveMessage_withinput(rep2_input);
			} catch (IOException e) {
				e.printStackTrace();
			}
			KVMImplement ack2 = new KVMImplement(ackMsg2);
			if (!(ack2.getStatus().equals(StatusType.PUT_ERROR))) {
				// correct response from the receiver
				returnedValue = true;
			}
		}

		// close the client sockets
		try {
			rep1_socket.close();
			rep2_socket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		rep1_socket = null;
		rep2_socket = null;

		return returnedValue;
	}

	/**
	 * Method sends a TextMessage using this socket.
	 * 
	 * @param msg the message that is to be sent.
	 * @throws IOException some I/O error regarding the output stream
	 */
	public void sendMessage(TextMessage msg) throws IOException {
		byte[] msgBytes = msg.getMsgBytes();
		output.write(msgBytes, 0, msgBytes.length);
		output.flush();
	}

	private TextMessage receiveMessage() throws IOException {

		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];

		/* read first char from stream */
		byte read = (byte) input.read();
		boolean reading = true;

		while (/* read != 13 && */ read != 10 && read != -1 && reading) {/* CR, LF, error */
			/* if buffer filled, copy to msg array */
			if (index == BUFFER_SIZE) {
				if (msgBytes == null) {
					tmp = new byte[BUFFER_SIZE];
					System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
				} else {
					tmp = new byte[msgBytes.length + BUFFER_SIZE];
					System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
					System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, BUFFER_SIZE);
				}

				msgBytes = tmp;
				bufferBytes = new byte[BUFFER_SIZE];
				index = 0;
			}

			/* only read valid characters, i.e. letters and constants */
			bufferBytes[index] = read;
			index++;

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

		return msg;
	}

	private TextMessage receiveMessage_withinput(InputStream input) throws IOException {

		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];

		/* read first char from stream */
		byte read = (byte) input.read();
		boolean reading = true;

		while (/* read != 13 && */ read != 10 && read != -1 && reading) {/* CR, LF, error */
			/* if buffer filled, copy to msg array */
			if (index == BUFFER_SIZE) {
				if (msgBytes == null) {
					tmp = new byte[BUFFER_SIZE];
					System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
				} else {
					tmp = new byte[msgBytes.length + BUFFER_SIZE];
					System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
					System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, BUFFER_SIZE);
				}

				msgBytes = tmp;
				bufferBytes = new byte[BUFFER_SIZE];
				index = 0;
			}

			/* only read valid characters, i.e. letters and constants */
			bufferBytes[index] = read;
			index++;

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

		return msg;
	}

}

/*
 * // Add server log info if
 * (rtnmsg.getStatus().equals(StatusType.DELETE_ERROR)) { logger.info("SEND \t<"
 * + clientSocket.getInetAddress().getHostAddress() + ":" +
 * clientSocket.getPort() + ">: '" + "DELETE_ERROR" + " " + rtnmsg.getKey() +
 * "'"); } else if (rtnmsg.getStatus().equals(StatusType.DELETE_SUCCESS)) {
 * logger.info("SEND \t<" + clientSocket.getInetAddress().getHostAddress() + ":"
 * + clientSocket.getPort() + ">: '" + "DELETE_SUCCESS" + " " + rtnmsg.getKey()
 * + "'"); } else if (rtnmsg.getStatus().equals(StatusType.GET_ERROR)) {
 * logger.info("SEND \t<" + clientSocket.getInetAddress().getHostAddress() + ":"
 * + clientSocket.getPort() + ">: '" + "GET_ERROR" + " " + rtnmsg.getKey() +
 * "'"); } else { logger.info("SEND \t<" +
 * clientSocket.getInetAddress().getHostAddress() + ":" + clientSocket.getPort()
 * + ">: '" + rtnmsg.getStringStatus() + " " + rtnmsg.getKey() + " " +
 * rtnmsg.getValue() + "'"); }
 */
