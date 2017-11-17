package common.messages;

//import common.messages.KVMessage.StatusType;

/**
 * Implement the KVMessage Interface.
 * Perform the conversion between KVMessage and TextMessage.
 */

public class KVMImplement implements KVMessage {
	private StatusType status = null;
	private String key = null;
	private String value = null;

	private TextMessage txtmsg = null;

	
	//convert from string to Enum 
	//need a look up table if increase the functionality
	// to send status, key and value;
	
	/**
	 * constructor for sending convert string to KVMessage type
	 * @param status communication flags
	 * @param key the key of tuple
	 * @param value the value of tuple
	 */
	public KVMImplement (String status,String key, String value) {
		
		if(status.equals("GET")){
			this.status = StatusType.GET;
		}	
		else if(status.equals("PUT")){
			this.status = StatusType.PUT;
		}
		else if(status.equals("GET_ERROR")){
			this.status = StatusType.GET_ERROR;
		}
		else if(status.equals("GET_SUCCESS")){
			this.status = StatusType.GET_SUCCESS;
		}
		else if(status.equals("PUT_SUCCESS")){
			this.status = StatusType.PUT_SUCCESS;
		}
		else if(status.equals("PUT_UPDATE")){
			this.status = StatusType.PUT_UPDATE;
		}
		else if(status.equals("PUT_ERROR")){
			this.status = StatusType.PUT_ERROR;
		}
		else if(status.equals("DELETE_SUCCESS")){
			this.status = StatusType.DELETE_SUCCESS;
		}
		else if(status.equals("DELETE_ERROR")){
			this.status = StatusType.DELETE_ERROR;
		}
		else if(status.equals("SERVER_STOPPED")){
			this.status = StatusType.SERVER_STOPPED;
		}
		else if(status.equals("SERVER_WRITE_LOCK")){
			this.status = StatusType.SERVER_WRITE_LOCK;
		}
		else if(status.equals("SERVER_NOT_RESPONSIBLE")){
			this.status = StatusType.SERVER_NOT_RESPONSIBLE;
		}
		
		this.key = key;
		this.value = value;		
		this.txtmsg = new TextMessage(status + '\0' + key + '\0' + value);		// as;ldkfh;asldjkas;gdik
	}

	/**
	 * receiving type, convert TextMessage type to string
	 * @param receive a TextMessage object
	 */
	public KVMImplement (TextMessage receive){

		String[] tokens = receive.getMsg().split("\0+");

		
		//parsing receive
		if(tokens[0].equals("GET")){
			this.status = StatusType.GET;
		}	
		else if(tokens[0].equals("PUT")){
			this.status = StatusType.PUT;
		}
		else if(tokens[0].equals("GET_ERROR")){
			this.status = StatusType.GET_ERROR;
		}
		else if(tokens[0].equals("GET_SUCCESS")){
			this.status = StatusType.GET_SUCCESS;
		}
		else if(tokens[0].equals("PUT_SUCCESS")){
			this.status = StatusType.PUT_SUCCESS;
		}
		else if(tokens[0].equals("PUT_UPDATE")){
			this.status = StatusType.PUT_UPDATE;
		}
		else if(tokens[0].equals("PUT_ERROR")){
			this.status = StatusType.PUT_ERROR;
		}
		else if(tokens[0].equals("DELETE_SUCCESS")){
			this.status = StatusType.DELETE_SUCCESS;
		}
		else if(tokens[0].equals("DELETE_ERROR")){
			this.status = StatusType.DELETE_ERROR;
		}
		else if(tokens[0].equals("SERVER_STOPPED")){
			this.status = StatusType.SERVER_STOPPED;
		}
		else if(tokens[0].equals("SERVER_WRITE_LOCK")){
			this.status = StatusType.SERVER_WRITE_LOCK;
		}
		else if(tokens[0].equals("SERVER_NOT_RESPONSIBLE")){
			this.status = StatusType.SERVER_NOT_RESPONSIBLE;
		}
		
		//this nend to be modified to fit the new command
		if (tokens.length == 3) {
			if (tokens[1] != null) {
				this.key = tokens[1];
			}
			
			if (tokens[2] != null) {
				this.value = tokens[2];
			}
		}
		else if (tokens.length == 2) {
			this.key = tokens[1];
		}
		
		this.txtmsg = receive;
	}
	
	//for sending
	public byte[] getmsgbyte (){
		return txtmsg.getMsgBytes();
		
	}
	
	public TextMessage getTextMessage() {
		return this.txtmsg;
	}
	
	
	public String getKey(){
		return this.key;
	}
	
	/**
	 * @return the value that is associated with this message, 
	 * 		null if not value is associated.
	 */
	public String getValue(){
		return this.value;
		
	}
	
	/**
	 * @return a status string that is used to identify request types, 
	 * response types and error types associated to the message.
	 */
	public StatusType getStatus(){
		return this.status;
		
	}
	
	//get status type in string
	public String getStringStatus(){
		StringBuilder str = new StringBuilder();
		
		if(this.status == StatusType.GET){
			str.append("GET");
		}	
		else if(this.status == StatusType.PUT){
			str.append("PUT");;
		}
		else if(this.status == StatusType.GET_ERROR){
			str.append("GET_ERROR");;
		}
		else if(this.status == StatusType.GET_SUCCESS){
			str.append("GET_SUCCESS");;
		}
		else if(this.status == StatusType.PUT_SUCCESS){
			str.append("PUT_SUCCESS");;
		}
		else if(this.status == StatusType.PUT_UPDATE){
			str.append("PUT_UPDATE");;
		}
		else if(this.status == StatusType.PUT_ERROR){
			str.append("PUT_ERROR");;
		}
		else if(this.status == StatusType.DELETE_SUCCESS){
			str.append("DELETE_SUCCESS");;
		}
		else if(this.status == StatusType.DELETE_ERROR){
			str.append("DELETE_ERROR");;
		}
		else if(this.status == StatusType.SERVER_STOPPED){
			str.append("SERVER_STOPPED");;
		}
		else if(this.status == StatusType.SERVER_WRITE_LOCK){
			str.append("SERVER_WRITE_LOCK");;
		}
		else if(this.status == StatusType.SERVER_NOT_RESPONSIBLE){
			str.append("SERVER_NOT_RESPONSIBLE");;
		}
		return str.toString();
	}
}
