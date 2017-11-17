package app_kvServer;

import storage.*;
import client.ClientSocketListener;
import common.*;
import common.messages.KVMImplement;
import common.messages.KVMessage.StatusType;
import common.messages.TextMessage;

import java.lang.*;
import org.apache.commons.lang3.mutable.*;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import logger.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

public class KVServer extends Thread implements Watcher {
	
	private static Logger logger = Logger.getRootLogger();
	
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 1024 * BUFFER_SIZE;
	
	private int port;
	private int cacheSize;
	private String strategy;
	private Storage s;
	//private boolean running;
	
	private ServerSocket serverSocket;
	
	private String zname;
	private String zkAddr;
	private static final int SESSION_TIMEOUT=5000;
    private ZooKeeper zk;
    private CountDownLatch connectedSignal = new CountDownLatch(1);
    
    private Metadata md;
    
    private MutableBoolean isLocked;
    private MutableBoolean running;
    
	private Socket clientSocket;
	private OutputStream output;
	private InputStream input;
	private Set<ClientSocketListener> listeners;
	
	/**
	 * Start KV Server at given port
	 * @param port given port for storage server to operate
	 * @param cacheSize specifies how many key-value pairs the server is allowed 
	 *           to keep in-memory
	 * @param strategy specifies the cache replacement strategy in case the cache 
	 *           is full and there is a GET- or PUT-request on a key that is 
	 *           currently not contained in the cache. Options are "FIFO", "LRU", 
	 *           and "LFU".
	 * @param zname the corresponding name of this KVServer in the znode tree.
	 *			 Also indicate the IP address and Port number of the server.
	 * @param zkAddr the IP address and port number of the zookeeper server
	 */
	
	
	public KVServer(int port, int cacheSize, String strategy, String zname, String zkAddr) {
		this.port = port;
		this.cacheSize = cacheSize;
		this.strategy = strategy;
		this.zname = zname;
		this.zkAddr = zkAddr;
		this.running = new MutableBoolean(false);
		this.isLocked = new MutableBoolean(false);
	}
	
	private boolean isRunning() {
		return this.running.booleanValue();
	}
	
	private boolean isLocked() {
		return this.isLocked.booleanValue();
	}
	
	private Storage getStorage() {
		return this.s;
	}
	
	@Override
    public void process(WatchedEvent event) {
    	// Watcher of KVServer
        if(event.getState() == KeeperState.SyncConnected){
            connectedSignal.countDown();
        }
    }
	
	/**
	 * Start the server
	 */
	public void run() {
		
        try {
			try {
				initializeServer();
				initializeStorage();
				connectZooKeeper();
				
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (KeeperException e) {
				e.printStackTrace();
			}
		} catch (IOException e1) {
			System.out.println("Initialize Storage IOException.");
		}
        
        if(serverSocket != null) {
	        while (true) {		//isRunning()
	            try {
	                Socket client = serverSocket.accept();
	                ClientConnection connection = 
	                		new ClientConnection(client, s, isLocked, running, md, zname, zk);
	                new Thread(connection).start();
	                
	                logger.info("Connected to " 
	                		+ client.getInetAddress().getHostName() 
	                		+  " on port " + client.getPort());
	                
	            } catch (IOException e) {
	            	logger.error("Error! Unable to establish connection. \n", e);
	            }
	        }
        }
        logger.info("Server stopped.");
	}
	
	/**
	 * Stops the server
	 */
	public void stopServer() {
		running.setValue(false);
		try {
			serverSocket.close();
		} catch (IOException e) {
			logger.error("Error! Unable to close socket on port: " + port, e);
		}
	}
	
	/**
	 * Initialize the storage for the server
	 * @return true if success; false if fail
	 * @throws IOException
	 */
	private boolean initializeStorage() throws IOException {
		logger.info("Initialize storage ...");
		
		if (strategy.equals("FIFO")) {
			s = new Storage(cacheSize, CacheType.FIFO, zname);
		}
		else if (strategy.equals("LRU")) {
			s = new Storage(cacheSize, CacheType.LRU, zname);
		}
		else{
			s = new Storage(cacheSize, CacheType.LFU, zname);
		}
		logger.info("Set up storage with cache size: " 
					+ cacheSize + "; replacement strategy: " + strategy);
		return true;
		
	}
	
	/**
	 * Initialize the server socket
	 * @return true if success; false if fail
	 */
    private boolean initializeServer() {
    	logger.info("Initialize server ...");
    	try {
            serverSocket = new ServerSocket(port);
            logger.info("Server listening on port: " 
            		+ serverSocket.getLocalPort());    
            return true;
        
        } catch (IOException e) {
        	logger.error("Error! Cannot open server socket:");
            if(e instanceof BindException){
            	logger.error("Port " + port + " is already bound!");
            }
            return false;
        }
    }
    
    /**
     * Connect the KVServer to the zookeeper and create new node(s) on the znode tree
     * The function also adds multiple watchers on proper znodes
     * @return true if success; false if fail
     * @throws IOException
     * @throws InterruptedException
     * @throws KeeperException
     */
    private boolean connectZooKeeper() throws IOException, InterruptedException, KeeperException {
    	logger.info("Connect to zookeeper ...");
    	try {
    		zk = new ZooKeeper(zkAddr, SESSION_TIMEOUT, this);
    		connectedSignal.await();
    		
    		// Acquire the metadata from zookeeper tree
    		// get_metadata();
    		
    		// Build an empty metadata
    		ArrayList<String> mySelf = new ArrayList<String>();
    		mySelf.add(zname);
    		this.md = new Metadata(mySelf);
    		
    		// Create the subtree of this KVServer on zookeeper
    		create_znode_subtree();
            
            // Set watcher on the shutdown znode
    		set_shutdown_watcher();
    		
    		// Set watcher on the updatemd znode
            set_updatemd_watcher();
            
            // Set watcher on the lock znode
            set_lock_watcher();
            
            // Set watcher on the migrate znode
            set_migrate_watcher();

            // Set watcher on the rmdata znode
            set_rmdata_watcher();
            
            return true;
        } catch (IOException e) {
        	logger.error("Error! Cannot connect to the zookeeper!");
            return false;
        }
    	
    }
    
    /**
     * Create a subtree rooted at the znode of this KVServer
     * @throws IOException
     * @throws InterruptedException
     * @throws KeeperException
     */
    void create_znode_subtree() throws IOException, InterruptedException, KeeperException {
    	String path_main = "/" + zname;
        if(zk.exists(path_main, false) == null) {
            zk.create(path_main, null/*data*/, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create((path_main+"/state"), "0".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            
            try {
            	// Set watcher on the state znode
            	set_state_watcher();
            } catch (IOException e) {
            	logger.error("Error! Cannot connect to the zookeeper!");
            }
    		
            // Here to add more node in the subtree
            zk.create((path_main+"/shutdown"), "0".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create((path_main+"/updatemd"), "0".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create((path_main+"/lock"), "0".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create((path_main+"/migrate"), "0".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zk.create((path_main+"/rmdata"), "0".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            
            // Create ephemeral node for error detection
            zk.create(("/alive/"+zname), null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            
        }
        logger.info("Created:"+path_main);
    }
    
    /**
     * Fetch and update the metadata
     * @throws IOException
     * @throws InterruptedException
     * @throws KeeperException
     */
    private void get_metadata() throws IOException, InterruptedException, KeeperException {
    	byte metadata[] = null;
    	String path_metadata = "/metadata";
    	metadata = zk.getData(path_metadata, false, null);
    	if(md==null){
    		md=new Metadata(new String(metadata));
    	}
    	else{
    		md.setMetaData(new String(metadata));
    	}
    	
    }
    
    /**
     * Remove the old tuples left in the server based on new metadata
     * @throws IOException 
     */
    private void remove_old_tuples() throws IOException {
    	// get the range based on metadata
    	String[] range = (md.getReplicaRange(zname)).split(":");
    	
    	if (getStorage().removeNotInRange(range[0], range[1])) {
    		// correct, do nothing
    	}
    	else {
    		logger.error("Wrong! Error when deleting old tuples!");
    	}
    }
	
    /**************************** Set multiple watchers ****************************/
    
    /**
	* Set a watcher to response for remove data command from zookeeper
	* Delete the data stored on disk within the range given via the content of rmdata node
    */
    void set_rmdata_watcher() throws IOException, InterruptedException, KeeperException {
    	byte b[] = null;
        String path_rmdata = "/" + zname + "/rmdata";
        b = zk.getData(path_rmdata, new Watcher() {
        	public void process(WatchedEvent event) {
        		if (event.getType() == Event.EventType.NodeDataChanged) {
                    // The data at shutdown node is changed
                    if (event.getState() == Event.KeeperState.SyncConnected) {
                    	String s = null;
                    	try {
							s = new String(zk.getData(("/"+zname+"/rmdata"), false, null));
						} catch (KeeperException | InterruptedException e2) {
							e2.printStackTrace();
						}
						
						// parse s to get the start and end range
						String[] range_info = s.split("_");
                    	String start_range = range_info[0];
                    	String end_range = range_info[1];
                    	
                    	// delete the data within the range
                    	try {
							getStorage().removeNotInRange(end_range, start_range);
						} catch (IOException e2) {
							e2.printStackTrace();
						}

                    	// acknowledge the zookeeper
                    	try {
							zk.setData(("/"+zname+"/rmdata"), "0".getBytes(), -1);
						} catch (KeeperException | InterruptedException e1) {
							e1.printStackTrace();
						}
                    	// need to set the watcher again
                    	try {
							byte b[] = zk.getData(("/"+zname+"/rmdata"), this, null);
						} catch (KeeperException | InterruptedException e) {
							e.printStackTrace();
						}
                    }
                    else {
                    	// Wrong place to go
                    	System.out.println("Wrong!");
                    }
                } else {
                    System.out.println("Wrong! The data at rmdata node is not " +
                    		"changed but the watcher is invoked.");
                }
        	}
        }, null);
    }

    /**
     * Set a watcher to response for shutDown command from zookeeper
     * @throws IOException
     * @throws InterruptedException
     * @throws KeeperException
     */
    void set_shutdown_watcher() throws IOException, InterruptedException, KeeperException {
    	byte b[] = null;
        String path_shutdown = "/" + zname + "/shutdown";
        b = zk.getData(path_shutdown, new Watcher() {
        	public void process(WatchedEvent event) {
        		if (event.getType() == Event.EventType.NodeDataChanged) {
                    // The data at shutdown node is changed
                    if (event.getState() == Event.KeeperState.SyncConnected) {
                    	shutDown();		// shutdown this server
                    }
                    else {
                    	// Wrong place to go
                    	System.out.println("Wrong!");
                    }
                } else {
                    System.out.println("Wrong! The data at shutdown node is not " +
                    		"changed but the watcher is invoked.");
                }
        	}
        }, null);
    }
    
    /**
     * Used to set a watcher on the state znode of each KVServer, to support start and stop command
     * @throws IOException
     * @throws InterruptedException
     * @throws KeeperException
     */
    void set_state_watcher() throws IOException, InterruptedException, KeeperException {
    	byte state[] = null;
        String path_state = "/" + zname + "/state";
        state = zk.getData(path_state, new Watcher() {
        	public void process(WatchedEvent event) {
        		if (event.getType() == Event.EventType.NodeDataChanged) {
                    if (event.getState() == Event.KeeperState.SyncConnected) {
                    	String s = null;
                    	try {
							s = new String(zk.getData(("/"+zname+"/state"), false, null));
						} catch (KeeperException | InterruptedException e2) {
							e2.printStackTrace();
						}
						
                    	if (s.equals("1")) {
                    		// Acquire the metadata from zookeeper tree
                    		try {
								get_metadata();
							} catch (IOException | InterruptedException
									| KeeperException e) {
								e.printStackTrace();
							}
                    		start_KVServer();
                    	}
                    	else if (s.equals("0")) {
                    		stop_KVServer();
                    	}
                    	else {
                    		logger.error("Wrong state value: neither 0 nor 1!");
                    	}
                    	
                    	// acknowledge the zookeeper
                    	try {
							zk.create(("/ack/"+zname), null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
						} catch (KeeperException | InterruptedException e1) {
							e1.printStackTrace();
						}
                    	// need to set the watcher again
                    	try {
							byte state[] = zk.getData(("/"+zname+"/state"), this, null);
						} catch (KeeperException | InterruptedException e) {
							e.printStackTrace();
						}
                    }
                    else {
                    	// Wrong place to go
                    	System.out.println("Wrong!");
                    }
                } else {
                    System.out.println("Wrong! The data at state node is not " +
                    		"changed but the watcher is invoked.");
                }
        	}
        }, null);
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
			if (zk.exists("/"+zname+"/debug", false) == null) {
				// remove the debug node if it exists previously
				zk.delete("/"+zname+"/debug", -1);
			}
			
			zk.create(("/" + zname + "/debug"), null, Ids.OPEN_ACL_UNSAFE,
					CreateMode.PERSISTENT);
			zk.setData(("/" + zname + "/debug"), allData.getBytes(), -1);
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
		}
	}
    
    
    /**
     * Set a watcher on updatemd znode to support update metadata command
     * @throws IOException
     * @throws InterruptedException
     * @throws KeeperException
     */
    void set_updatemd_watcher() throws IOException, InterruptedException, KeeperException {
    	byte b[] = null;
        String path_updatemd = "/" + zname + "/updatemd";
        b = zk.getData(path_updatemd, new Watcher() {
        	public void process(WatchedEvent event) {
        		if (event.getType() == Event.EventType.NodeDataChanged) {
                    if (event.getState() == Event.KeeperState.SyncConnected) {
                    	String s = null;
						try {
							s = new String(zk.getData(("/"+zname+"/updatemd"), false, null));
						} catch (KeeperException e) {
							e.printStackTrace();
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
						
                    	if (s.equals("1")) {
                    		// update metadata and delete the objects not in range
							try {
								get_metadata();
								
							} catch (IOException | InterruptedException
									| KeeperException e1) {
								e1.printStackTrace();
							}
                    		// acknowledge the zookeeper - set it back to 0 and add a child on ack
                    		try {
                    			zk.setData(("/"+zname), md.sendMeta().getBytes(), -1);
								zk.setData(("/"+zname+"/updatemd"), "0".getBytes(), -1);
								zk.create(("/ack/"+zname), null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
							} catch (KeeperException | InterruptedException e) {
								e.printStackTrace();
							}
                    	}
                    	else {
                    		logger.error("Wrong updatemd value: not changed to 1!");
                    	}
                    	
                    	// need to set the watcher again
                    	try {
							byte b[] = zk.getData(("/"+zname+"/updatemd"), this, null);
						} catch (KeeperException | InterruptedException e) {
							e.printStackTrace();
						}
                    }
                    else {
                    	// Wrong place to go
                    	System.out.println("Wrong!");
                    }
                } else {
                    System.out.println("Wrong! The data at updatemd node is not " +
                    		"changed but the watcher is invoked.");
                }
        	}
        }, null);
    }
    
    
    /**
     * Set a watcher on lock znode to support lock and unlock command
     * @throws IOException
     * @throws InterruptedException
     * @throws KeeperException
     */
    void set_lock_watcher() throws IOException, InterruptedException, KeeperException {
    	byte b[] = null;
        b = zk.getData(("/" + zname + "/lock"), new Watcher() {
        	public void process(WatchedEvent event) {
        		if (event.getType() == Event.EventType.NodeDataChanged) {
                    if (event.getState() == Event.KeeperState.SyncConnected) {
                    	String s = null;
                    	try {
							s = new String(zk.getData(("/" + zname + "/lock"), false, null));
						} catch (KeeperException | InterruptedException e2) {
							e2.printStackTrace();
						}
						
                    	if (s.equals("1")) {
                    		lockWrite();
                    	}
                    	else if (s.equals("0")) {
                    		unLockWrite();
                    	}
                    	else {
                    		logger.error("Wrong state value: neither 0 nor 1!");
                    	}
                    	
                    	// acknowledge the zookeeper
                    	try {
							zk.create(("/ack/"+zname), null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
						} catch (KeeperException | InterruptedException e1) {
							e1.printStackTrace();
						}
                    	// need to set the watcher again
                    	try {
							byte state[] = zk.getData(("/" + zname + "/lock"), this, null);
						} catch (KeeperException | InterruptedException e) {
							e.printStackTrace();
						}
                    }
                    else {
                    	// Wrong place to go
                    	System.out.println("Wrong!");
                    }
                } else {
                    System.out.println("Wrong! The data at lock node is not " +
                    		"changed but the watcher is invoked.");
                }
        	}
        }, null);
    }
    
    /**
     * Set a watcher on the migrate znode to support data migration
     * @throws IOException
     * @throws InterruptedException
     * @throws KeeperException
     */
    void set_migrate_watcher() throws IOException, InterruptedException, KeeperException {
    	byte b[] = null;
        b = zk.getData(("/" + zname + "/migrate"), new Watcher() {
        	public void process(WatchedEvent event) {
        		if (event.getType() == Event.EventType.NodeDataChanged) {
                    if (event.getState() == Event.KeeperState.SyncConnected) {
                    	String s = null;
                    	try {
							s = new String(zk.getData(("/" + zname + "/migrate"), false, null));
						} catch (KeeperException | InterruptedException e1) {
							e1.printStackTrace();
						}
						
                    	if (s.equals("0")) {
                    		// not migrate; should not enter here
                    		logger.error("Wrong! migrate znode is 0!");
                    	}
                    	else {
                    		// parse the message and transfer data
                    		// need to deal with sender and receiver separately
                    		
                        	String[] migration_info = s.split("_");
                        	String sender = migration_info[0];
                        	String receiver = migration_info[1];
                        	String start_range = migration_info[2];
                        	String end_range = migration_info[3];
                        	
                        	// determine whether it's a sender or receiver
                        	if (zname.equals(sender)) {
                        		// This KVServer is the sender
                        		
                        		System.out.println("Entered movedata");
                        		
                        		// move data to the receiver
                        		moveData(start_range, end_range, receiver);
                        		
                        		System.out.println("Exit movedata");
                        		
                        		
                        		// finished moving data - acknowledge the zookeeper at sender znode
                            	try {
									zk.setData(("/"+zname+"/migrate"), "0".getBytes(), -1);
								} catch (KeeperException | InterruptedException e) {
									e.printStackTrace();
								}
                        	}
                        	else if (zname.equals(receiver)) {
                        		// This KVServer is the receiver
                        		
                        		// acknowledge the zookeeper at receiver znode
                            	try {
									zk.setData(("/"+zname+"/migrate"), "0".getBytes(), -1);
								} catch (KeeperException | InterruptedException e) {
									e.printStackTrace();
								}
                        	}
                        	else {
                        		// Should not call the watcher
                        		logger.error("Wrong! Not sender or receiver!");
                        	}
                    	}
                    	
                    	// need to set the watcher again
                    	try {
							byte state[] = zk.getData(("/" + zname + "/migrate"), this, null);
						} catch (KeeperException | InterruptedException e) {
							e.printStackTrace();
						}
                    }
                    else {
                    	// Wrong place to go
                    	System.out.println("Wrong!");
                    }
                } else {
                    System.out.println("Wrong! The data at migrate node is not " +
                    		"changed but the watcher is invoked.");
                }
        	}
        }, null);
    }
    
    /**
     * Helper function to receive message during data transfer
     * @return
     * @throws IOException
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
    /*******************************************************************************/
    
    /*********************** Functionalities provided to ECS ***********************/
    
    /**
     * Initialize the KVServer
     * @param metadata
     * @param cacheSize
     * @param type the replacement strategy
     */
    public void initKVServer(Metadata metadata, int cacheSize, String type) {
    	this.md = metadata;
    	this.cacheSize = cacheSize;
    	this.strategy = type;
    }
    
    /**
     * Start the KVServer - from stop state to run state
     */
    public void start_KVServer() {
    	running.setValue(true);
    }
    
    /**
     * Stop the KVServer - from run state to stop state
     */
    public void stop_KVServer() {
    	running.setValue(false);
    }
    
    /**
     * Shutdown the KVServer
     */
    public void shutDown() {
    	// Delete the znode subtree of this KVServer
    	try {
    		zk.delete(("/"+zname+"/state"), -1);
    		zk.delete(("/"+zname+"/shutdown"), -1);
    		zk.delete(("/"+zname+"/lock"), -1);
    		zk.delete(("/"+zname+"/updatemd"), -1);
    		zk.delete(("/"+zname+"/migrate"), -1);
    		zk.delete(("/"+zname+"/rmdata"), -1);
    		zk.delete(("/"+zname), -1);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		} catch (KeeperException e1) {
			e1.printStackTrace();
		}
    	
    	// Create node to ack node
    	try {
			zk.create(("/ack/"+zname), null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch (KeeperException | InterruptedException e) {
			e.printStackTrace();
		}
    	// Shutdown the KVServer
    	stopServer();
    	System.exit(0);
    }
    
    /**
     * Add write lock to this server
     */
    public void lockWrite() {
    	isLocked.setValue(true);
    }
    
    /**
     * Remove write lock on this server
     */
    public void unLockWrite() {
    	isLocked.setValue(false);
    }
    
    /**
     * Transfer data between two KVServers
     * @param start_range
     * @param end_range
     * @param receiver
     */
    public void moveData(String start_range, String end_range, String receiver) {
    	// create new socket and send data
		String[] rece_info = receiver.split(":");
		String address = rece_info[0];
		int port = Integer.parseInt(rece_info[1]);
		
		
		
		try {
    		clientSocket = new Socket(address, port);
    		listeners = new HashSet<ClientSocketListener>();
    		output = clientSocket.getOutputStream();
			input = clientSocket.getInputStream();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
		
		
		
		// create the sending message
		ArrayList<DBResponse> result = null;
		try {
			result = getStorage().getInRange(start_range, end_range);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		for (DBResponse data : result) {
			// send each data in result array
			System.out.println("Key: "+data.key+", value: "+data.value);
			KVMImplement dataMigrate = new KVMImplement("SERVER_STOPPED", data.key, data.value);
			byte[] msgBytes = dataMigrate.getmsgbyte();
			try {
				output.write(msgBytes, 0, msgBytes.length);
				output.flush();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			TextMessage latestMsg = null;
			try {
				// wait for the reply from the receiver
				latestMsg = receiveMessage();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			KVMImplement newMsg = new KVMImplement(latestMsg);
			if (newMsg.getStatus().equals(StatusType.PUT_SUCCESS) 
					|| newMsg.getStatus().equals(StatusType.PUT_UPDATE)) {
				// correct response from the receiver
				// move next data
			}
			else {
				// Should not receive such a response
				logger.error("Wrong! Data transfer receives wrong response type!");
			}
		}
		
		// close the client socket
		try {
			clientSocket.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		clientSocket = null;
    }
    
    /**
     * Update the metadata of this KVServer based on the given md
     * @param md
     */
    public void updateMetadata(Metadata md) {
    	this.md = md;
    }
    
    /*******************************************************************************/
    
    
	/**
	 * Main function
	 * @param args contains the port number at args[0], cache size at args[1], strategy at args[2],
	 * 						 corresponding znode name of this KVServer at args[3],
	 * 						 zookeeper address and port number at args[4].
	 */
    public static void main(String[] args) {
    	try {
			new LogSetup("logs/server/server.log", Level.INFO);
			if(args.length != 5) {
				System.out.println("Error! Wrong number of arguments!");
				System.out.println("Usage: Server <port> <cache size> " +
						"<cache strategy: FIFO, LRU, LFU> " + 
						"<znode name of this server> <zookeeper address>");
			} else {
				int port = Integer.parseInt(args[0]);
				int cacheSize = Integer.parseInt(args[1]);
				String type = args[2];
				String zname = args[3];
				String zkAddr = args[4];
				
				if (type.equals("FIFO") || type.equals("LRU") || type.equals("LFU")) {
					new KVServer(port, cacheSize, type, zname, zkAddr).start();
				}
				else {
					System.out.println("---" + type + "---");
					System.out.println("Error! Invalid replacement strategy!");
					System.out.println("Please use only one of FIFO, LRU, and LFU");
					System.out.println("Usage: Server <port> <cache size> " +
							"<cache strategy: FIFO, LRU, LFU> " +
							"<znode name of this server> <zookeeper address>");
					System.exit(1);
				}
			}
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		} catch (NumberFormatException nfe) {
			System.out.println("Error! Invalid argument <port> or <cache size>! Not a number!");
			System.out.println("Usage: Server <port> <cache size> " +
					"<cache strategy: FIFO, LRU, LFU>");
			System.exit(1);
		}
    }
}








