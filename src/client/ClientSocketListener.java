package client;

import common.messages.KVMImplement;

public interface ClientSocketListener {

	public enum SocketStatus {
		CONNECTED, DISCONNECTED, CONNECTION_LOST
	};

	public void handleNewMessage(KVMImplement msg);

	public void handleStatus(SocketStatus status);

}
