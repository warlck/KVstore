package kvstore;

import static kvstore.KVConstants.*;

import java.io.IOException;
import java.net.Socket;

/**
 * This NetworkHandler will asynchronously handle the socket connections.
 * It uses a threadPool to ensure that none of it's methods are blocking.
 */
public class TPCClientHandler implements NetworkHandler {

    public TPCMaster tpcMaster;
    public ThreadPool threadPool;

    /**
     * Constructs a TPCClientHandler with ThreadPool of a single thread.
     *
     * @param tpcMaster TPCMaster to carry out requests
     */
    public TPCClientHandler(TPCMaster tpcMaster) {
        this(tpcMaster, 1);
    }

    /**
     * Constructs a TPCClientHandler with ThreadPool of a single thread.
     *
     * @param tpcMaster TPCMaster to carry out requests
     * @param connections number of threads in threadPool to service requests
     */
    public TPCClientHandler(TPCMaster tpcMaster, int connections) {
        // implement me
    	this.tpcMaster = tpcMaster;
    	threadPool = new ThreadPool(connections);
    }

    /**
     * Creates a job to service the request on a socket and enqueues that job
     * in the thread pool. Ignore InterruptedExceptions.
     *
     * @param client Socket connected to the client with the request
     */
    @Override
    public void handle(Socket client) {
        // implement me
    	try {
			threadPool.addJob(new clientHandler(client));
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
    }
    
    // implement me
    private class clientHandler implements Runnable {
    	private Socket client;
    	public clientHandler(Socket client) {
    		this.client = client;
    	}
    	public void run(){
    		KVMessage req = null;
    		KVMessage rsp = null;
    		try {
    			req = new KVMessage(client);
    			
    			if (req.getMsgType().equals(GET_REQ)) {
    				String value = tpcMaster.handleGet(req);
    				rsp = new KVMessage(RESP);
    				rsp.setKey(req.getKey());
    				rsp.setValue(value);
    			} else if (req.getMsgType().equals(PUT_REQ)) {
    				tpcMaster.handleTPCRequest(req, true);
    				rsp = new KVMessage(RESP, SUCCESS);
    			} else if (req.getMsgType().equals(DEL_REQ)) {
    				tpcMaster.handleTPCRequest(req, false);
    				rsp = new KVMessage(RESP, SUCCESS);
    			}
    		} catch (KVException e) {
    			rsp = e.getKVMessage();
    		}
    		
    		if (rsp != null) {
    			try {
    				rsp.sendMessage(client);
    			} catch (Exception e) {
    			}
    		}
    	}
    }

}
