package kvstore;

import static kvstore.KVConstants.DEL_REQ;
import static kvstore.KVConstants.ERROR_COULD_NOT_CONNECT;
import static kvstore.KVConstants.ERROR_COULD_NOT_CREATE_SOCKET;
import static kvstore.KVConstants.ERROR_INVALID_KEY;
import static kvstore.KVConstants.ERROR_INVALID_VALUE;
import static kvstore.KVConstants.GET_REQ;
import static kvstore.KVConstants.PUT_REQ;
import static kvstore.KVConstants.RESP;
import static kvstore.KVConstants.SUCCESS;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * Client API used to issue requests to key-value server.
 */
public class KVClient implements KeyValueInterface {

    public String server;
    public int port;

    /**
     * Constructs a KVClient connected to a server.
     *
     * @param server is the DNS reference to the server
     * @param port is the port on which the server is listening
     */
    public KVClient(String server, int port) {
        this.server = server;
        this.port = port;
    }

    /**
     * Creates a socket connected to the server to make a request.
     *
     * @return Socket connected to server
     * @throws KVException if unable to create or connect socket
     */
    public Socket connectHost() throws KVException {
        // implement me
    	Socket sock = null;
    	try {
			sock = new Socket(server, port);
		} catch (UnknownHostException e) {
			throw new KVException(ERROR_COULD_NOT_CONNECT);
		} catch (IOException e) {
			throw new KVException(ERROR_COULD_NOT_CREATE_SOCKET);
		} 
        return sock;
    }

    /**
     * Closes a socket.
     * Best effort, ignores error since the response has already been received.
     *
     * @param  sock Socket to be closed
     */
    public void closeHost(Socket sock) {
        // implement me
    	try {
			sock.close();
		} catch (Exception e) {
			//e.printStackTrace();
		}
    }

    /**
     * Issues a PUT request to the server.
     *
     * @param  key String to put in server as key
     * @throws KVException if the request was not successful in any way
     */
    @Override
    public void put(String key, String value) throws KVException {
        // implement me
    	if (key == null || key.length() == 0) throw new KVException(ERROR_INVALID_KEY);
    	if (value == null || value.length() == 0) throw new KVException(ERROR_INVALID_VALUE);
    	KVMessage putReq = new KVMessage(PUT_REQ);
    	putReq.setKey(key);
    	putReq.setValue(value);
    	Socket sock = null;
    	try {
    		sock = connectHost();
    		putReq.sendMessage(sock);
    		KVMessage getRsp = new KVMessage(sock);
        	if (!getRsp.getMessage().equals(KVConstants.SUCCESS))
        		throw new KVException(getRsp.getMessage());
    	} catch (KVException e) {
    		throw e;
    	} finally {
        	if (sock != null)
        		closeHost(sock);
        }
    }

    /**
     * Issues a GET request to the server.
     *
     * @param  key String to get value for in server
     * @return String value associated with key
     * @throws KVException if the request was not successful in any way
     */
    @Override
    public String get(String key) throws KVException {
    	if (key == null || key.length() == 0) throw new KVException(ERROR_INVALID_KEY);
    	KVMessage getReq = new KVMessage(GET_REQ);
    	getReq.setKey(key);
    	KVMessage getRsp = null;
    	Socket sock = null;
    	try {
    		sock = connectHost();
    		getReq.sendMessage(sock);
    		getRsp = new KVMessage(sock);
    		if (getRsp.getKey() == null || getRsp.getValue() == null)
        		throw new KVException(getRsp.getMessage());
    	} catch (KVException e) {
    		throw e;
    	} finally {
        	if (sock != null)
        		closeHost(sock);
        }

    	return getRsp.getValue();

    }

    /**
     * Issues a DEL request to the server.
     *
     * @param  key String to delete value for in server
     * @throws KVException if the request was not successful in any way
     */
    @Override
    public void del(String key) throws KVException {
        // implement me
    	if (key == null || key.length() == 0) throw new KVException(ERROR_INVALID_KEY);
    	KVMessage putReq = new KVMessage(DEL_REQ);
    	putReq.setKey(key);
    	Socket sock = null;
    	try {
    		sock = connectHost();
    		putReq.sendMessage(sock);
    		KVMessage getRsp = new KVMessage(sock);
        	if (!getRsp.getMessage().equals(KVConstants.SUCCESS))
        	throw new KVException(getRsp.getMessage());
    	} catch (KVException e) {
    		throw e;
    	} finally {
        	if (sock != null)
        		closeHost(sock);
        }
    }


}
