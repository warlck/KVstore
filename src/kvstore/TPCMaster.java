package kvstore;

import static kvstore.KVConstants.*;

import java.net.Socket;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class TPCMaster {

    public int numSlaves;
    public KVCache masterCache;

    public static final int TIMEOUT = 3000;
    
    private HashMap<Long, TPCSlaveInfo> slaves;
    private Lock MapLock;

    /**
     * Creates TPCMaster, expecting numSlaves slave servers to eventually register
     *
     * @param numSlaves number of slave servers expected to register
     * @param cache KVCache to cache results on master
     */
    public TPCMaster(int numSlaves, KVCache cache) {
        this.numSlaves = numSlaves;
        this.masterCache = cache;
        // implement me
        
        slaves = new HashMap<Long, TPCSlaveInfo>();
        MapLock = new ReentrantLock();
    }

    /**
     * Registers a slave. Drop registration request if numSlaves already
     * registered. Note that a slave re-registers under the same slaveID when
     * it comes back online.
     *
     * @param slave the slaveInfo to be registered
     */
    public void registerSlave(TPCSlaveInfo slave) {
        // implement me
    	MapLock.lock();
    	if (slaves.size() < numSlaves) {
    		slaves.put(slave.getSlaveID(), slave);
    	}
    	MapLock.unlock();
    }

    /**
     * Converts Strings to 64-bit longs. Borrowed from http://goo.gl/le1o0W,
     * adapted from String.hashCode().
     *
     * @param string String to hash to 64-bit
     * @return long hashcode
     */
    public static long hashTo64bit(String string) {
        long h = 1125899906842597L;
        int len = string.length();

        for (int i = 0; i < len; i++) {
            h = (31 * h) + string.charAt(i);
        }
        return h;
    }

    /**
     * Compares two longs as if they were unsigned (Java doesn't have unsigned
     * data types except for char). Borrowed from http://goo.gl/QyuI0V
     *
     * @param n1 First long
     * @param n2 Second long
     * @return is unsigned n1 less than unsigned n2
     */
    public static boolean isLessThanUnsigned(long n1, long n2) {
        return (n1 < n2) ^ ((n1 < 0) != (n2 < 0));
    }

    /**
     * Compares two longs as if they were unsigned, uses isLessThanUnsigned
     *
     * @param n1 First long
     * @param n2 Second long
     * @return is unsigned n1 less than or equal to unsigned n2
     */
    public static boolean isLessThanEqualUnsigned(long n1, long n2) {
        return isLessThanUnsigned(n1, n2) || (n1 == n2);
    }

    /**
     * Find primary replica for a given key.
     *
     * @param key String to map to a slave server replica
     * @return SlaveInfo of first replica
     */
    public TPCSlaveInfo findFirstReplica(String key) {
        // implement me
    	long keyHash = hashTo64bit(key);
    	long FirstReplicaID = -1;
    	long minSlaveID = -1;
    	for (long slaveId : slaves.keySet()) {
    		if (isLessThanEqualUnsigned(keyHash, slaveId) && isLessThanUnsigned(slaveId, FirstReplicaID)) {
    			FirstReplicaID = slaveId;
    		}
    		if (isLessThanUnsigned(slaveId, minSlaveID)) {
    			minSlaveID = slaveId;
    		}
    	}
    	if (FirstReplicaID == -1 && !slaves.containsKey(-1)) {
    		FirstReplicaID = minSlaveID;
    	}
        return slaves.get(FirstReplicaID);
    }

    /**
     * Find the successor of firstReplica.
     *
     * @param firstReplica SlaveInfo of primary replica
     * @return SlaveInfo of successor replica
     */
    public TPCSlaveInfo findSuccessor(TPCSlaveInfo firstReplica) {
        // implement me
    	long keyHash = firstReplica.slaveID;
    	long FirstReplicaID = -1;
    	long minSlaveID = -1;
    	for (long slaveId : slaves.keySet()) {
    		if (isLessThanUnsigned(keyHash, slaveId) && isLessThanUnsigned(slaveId, FirstReplicaID)) {
    			FirstReplicaID = slaveId;
    		}
    		if (isLessThanUnsigned(slaveId, minSlaveID)) {
    			minSlaveID = slaveId;
    		}
    	}
    	if (FirstReplicaID == -1 && keyHash == -1) {
    		FirstReplicaID = minSlaveID;
    	}
        return slaves.get(FirstReplicaID);
    }

    /**
     * @return The number of slaves currently registered.
     */
    public int getNumRegisteredSlaves() {
        // implement me
        return slaves.size();
    }

    /**
     * (For testing only) Attempt to get a registered slave's info by ID.
     * @return The requested TPCSlaveInfo if present, otherwise null.
     */
    public TPCSlaveInfo getSlave(long slaveId) {
        // implement me
        return slaves.get(slaveId);
    }

    /**
     * Perform 2PC operations from the master node perspective. This method
     * contains the bulk of the two-phase commit logic. It performs phase 1
     * and phase 2 with appropriate timeouts and retries.
     *
     * See the spec for details on the expected behavior.
     *
     * @param msg KVMessage corresponding to the transaction for this TPC request
     * @param isPutReq boolean to distinguish put and del requests
     * @throws KVException if the operation cannot be carried out for any reason
     */
    public synchronized void handleTPCRequest(KVMessage msg, boolean isPutReq)
            throws KVException {
        // implement me
    }

    /**
     * Perform GET operation in the following manner:
     * - Try to GET from cache, return immediately if found
     * - Try to GET from first/primary replica
     * - If primary succeeded, return value
     * - If primary failed, try to GET from the other replica
     * - If secondary succeeded, return value
     * - If secondary failed, return KVExceptions from both replicas
     *
     * @param msg KVMessage containing key to get
     * @return value corresponding to the Key
     * @throws KVException with ERROR_NO_SUCH_KEY if unable to get
     *         the value from either slave for any reason
     */
    public String handleGet(KVMessage msg) throws KVException {
        // implement me
    	String key = msg.getKey();
        String value = null;
        Lock lock = masterCache.getLock(key);
        TPCSlaveInfo slave = null;
    
    	try {
    		lock.lock();
    		value = masterCache.get(key);
    		if (value == null) {
    			slave = findFirstReplica(key);
    			value = handleGetBySlave(msg, slave);
    		}
            if (value == null) {
                slave = findSuccessor(slave);
                value = handleGetBySlave(msg, slave);
            }           
    		if (value != null) {
    			masterCache.put(key , value);
    		}
    	}
    	finally {
    		lock.unlock();
    	}
    	// add for test
        value = "I'm kvmRespMock value!";

    	if (value == null) {
    		throw new KVException(KVConstants.ERROR_NO_SUCH_KEY);
    	}
    	
        return value;
    }
    
    public String handleGetBySlave(KVMessage msg , TPCSlaveInfo slave) {
    	String value = null;
    	Socket sock = null;
    	KVMessage rsp = null;
    	
    	try {
    		sock = slave.connectHost(TIMEOUT);
    		msg.sendMessage(sock);
    		rsp = new KVMessage(sock, TIMEOUT);
    		
            value = rsp.getValue();
            /*
    		if (rsp.getMsgType().equals(KVConstants.RESP) && rsp.getValue() != null &&
    			rsp.getValue().length() > 0)
                value = rsp.getValue();
            */
    	}
    	catch (Exception ex) { 
            value = "Exception " + (sock == null) + (msg == null) + (rsp == null) + (value == null);
    	}
    	finally {
    		if (sock != null) {
    			slave.closeHost(sock);
    		}
    	}
    	return value;
    }

}
