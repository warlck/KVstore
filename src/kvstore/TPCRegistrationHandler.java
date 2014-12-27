package kvstore;

import static kvstore.KVConstants.*;

import java.io.IOException;
import java.net.Socket;

/**
 * This NetworkHandler will asynchronously handle the socket connections.
 * Uses a thread pool to ensure that none of its methods are blocking.
 */
@SuppressWarnings("unused")
public class TPCRegistrationHandler implements NetworkHandler {

    private ThreadPool threadpool;
    private TPCMaster master;

    /**
     * Constructs a TPCRegistrationHandler with a ThreadPool of a single thread.
     *
     * @param master TPCMaster to register slave with
     */
    public TPCRegistrationHandler(TPCMaster master) {
        this(master, 1);
    }

    /**
     * Constructs a TPCRegistrationHandler with ThreadPool of thread equal to the
     * number given as connections.
     *
     * @param master TPCMaster to carry out requests
     * @param connections number of threads in threadPool to service requests
     */
    public TPCRegistrationHandler(TPCMaster master, int connections) {
        this.threadpool = new ThreadPool(connections);
        this.master = master;
    }

    /**
     * Creates a job to service the request on a socket and enqueues that job
     * in the thread pool. Ignore any InterruptedExceptions.
     *
     * @param slave Socket connected to the slave with the request
     */
    @Override
    public void handle(Socket slave) {
        // implement me
    	try {
			threadpool.addJob(new handleRegistration(slave));
		} catch (InterruptedException e) {
		}
    }
    
    // implement me
    public class handleRegistration implements Runnable {
    	public Socket slave;
    	
    	public handleRegistration(Socket slave) {
    		this.slave = slave;
    	}
    	public void run(){
    		KVMessage RegReq = null;
    		KVMessage RegRsp = null;
    		
    		try {
    			RegReq = new KVMessage(slave);
    			if (RegReq.getMsgType().equals(REGISTER)) {
    				master.registerSlave(new TPCSlaveInfo(RegReq.getMessage()));
    				RegRsp = new KVMessage(RESP, "Successfully registered " + RegReq.getMessage());
    			} else {
        			RegRsp = new KVMessage(ERROR_INVALID_FORMAT);
    			}
    		} catch (Exception e) {
    			RegRsp = new KVMessage(ERROR_COULD_NOT_RECEIVE_DATA);
    		}
    		
    		try {
    			RegRsp.sendMessage(slave);
    		} catch (Exception e) {
    			e.printStackTrace();
    		}
    	}
    }
}
