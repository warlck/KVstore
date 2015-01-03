package kvstore;

import static kvstore.KVConstants.*;

import java.io.IOException;
import java.net.Socket;
/**
 * Implements NetworkHandler to handle 2PC operation requests from the Master/
 * Coordinator Server
 */
public class TPCMasterHandler implements NetworkHandler {

    public long slaveID;
    public KVServer kvServer;
    public TPCLog tpcLog;
    public ThreadPool threadpool;

    // implement me

    /**
     * Constructs a TPCMasterHandler with one connection in its ThreadPool
     *
     * @param slaveID the ID for this slave server
     * @param kvServer KVServer for this slave
     * @param log the log for this slave
     */
    public TPCMasterHandler(long slaveID, KVServer kvServer, TPCLog log) {
        this(slaveID, kvServer, log, 1);
    }

    /**
     * Constructs a TPCMasterHandler with a variable number of connections
     * in its ThreadPool
     *
     * @param slaveID the ID for this slave server
     * @param kvServer KVServer for this slave
     * @param log the log for this slave
     * @param connections the number of connections in this slave's ThreadPool
     */
    public TPCMasterHandler(long slaveID, KVServer kvServer, TPCLog log, int connections) {
        this.slaveID = slaveID;
        this.kvServer = kvServer;
        this.tpcLog = log;
        this.threadpool = new ThreadPool(connections);
    }

    /**
     * Registers this slave server with the master.
     *
     * @param masterHostname
     * @param server SocketServer used by this slave server (which contains the
     *               hostname and port this slave is listening for requests on
     * @throws KVException with ERROR_INVALID_FORMAT if the response from the
     *         master is received and parsed but does not correspond to a
     *         success as defined in the spec OR any other KVException such
     *         as those expected in KVClient in project 3 if unable to receive
     *         and/or parse message
     */
    public void registerWithMaster(String masterHostname, SocketServer server)
            throws KVException {
        // implement me
    	String SlaveInfo = slaveID + "@" + server.getHostname() + ":" + server.getPort();
    	KVMessage Reg = new KVMessage(SlaveInfo);
    	Socket sock = null;
    	try {
    		sock = new Socket(masterHostname, 9090);
    		Reg.sendMessage(sock);
    		KVMessage RegRsp = new KVMessage(sock);
    		if (!RegRsp.getMsgType().equals(RESP) || 
    				!RegRsp.getMessage().equals("Successfully registered " + SlaveInfo)) {
    			throw new KVException(ERROR_INVALID_FORMAT);
    		}
    	} catch (IOException ex) {
        	throw new KVException(ERROR_COULD_NOT_CONNECT);
        } catch (Exception ex) {
        	throw new KVException(ERROR_COULD_NOT_CREATE_SOCKET);
        }
    	try {
    		if (sock != null) {
    			sock.close();
    		}
    	} catch (Exception e) {
    	}
    }

    /**
     * Creates a job to service the request on a socket and enqueues that job
     * in the thread pool. Ignore any InterruptedExceptions.
     *
     * @param master Socket connected to the master with the request
     */
    @Override
    public void handle(Socket master) {
        try {
        	threadpool.addJob(new MasterHandler(master));
        }
        catch (InterruptedException ex) {        	
        }
    }

    /**
     * Runnable class containing routine to service a message from the master.
     */
    private class MasterHandler implements Runnable {

        private Socket master;

        public MasterHandler(Socket master) {
            this.master = master;
        }

        @Override
        public void run() {
            KVMessage req = null;
            KVMessage rsp = null;
            
            try {
            	req = new KVMessage(master);
                String key = req.getKey();
            	
            	if (req.getMsgType().equals(GET_REQ)) {
            		if (key == null || key.length() == 0)
            			rsp = new KVMessage(RESP , ERROR_INVALID_KEY);
            		else if (key.length() > 256)
            			rsp = new KVMessage(RESP , ERROR_OVERSIZED_KEY);
            		else if (!kvServer.hasKey(key))
            			rsp = new KVMessage(RESP , ERROR_NO_SUCH_KEY);
            		else {
            			rsp = new KVMessage(RESP);
            			rsp.setKey(key);
            			rsp.setValue(kvServer.get(key));
            		}
            	}
            }
            catch (Exception e) {
            	return;
            }
            if (rsp != null) {
            	try {
            		rsp.sendMessage(master);
            	}
            	catch (Exception ex) {            		
            	}
            }
        }

    }
    

}
