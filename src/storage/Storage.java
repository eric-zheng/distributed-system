package storage;

import java.lang.*;

import java.util.*;
import java.io.*;
import java.security.*;



public class Storage {
	
	
	private DBCache cache;
	private FileInputStream fromDisk;
	private FileOutputStream toDisk;
	public String dataFileName;
	public String tempFileName;
	
	public Storage(int size, CacheType type, String zname)throws IOException{
		dataFileName=zname+"DBdata.dat";
		tempFileName=zname+"DBdatatemp____.dat";
		
		cache=new DBCache(type,size);
		File tempFile = new File(dataFileName);
		if(!(tempFile.exists()&&tempFile.isFile())){		//if the file does not exist then create one!
			tempFile.createNewFile();
		}
		fromDisk=null;
		toDisk=null;
	}
	
	
	//Disk Accessing Functions. should only be used by get, put, and delete
	
	
	/*
	 *A tuple in file is defined in the following format:
	 *-keyString+"\00"+valueString+"\00"
	 *
	 * 
	 * !!!Important assumption!!!: 	we are assuming that a string should not contain a null character ever! which makes sense. 
	 * 								Also we are assuming that the string is within correct range of size 
	 * */
	
	/**
	 * Store a key-value tuple in a specified file location
	 * @param key the key to write
	 * @param value the value to write
	 * @param source destination file location
	 * @throws IOException
	 */
	
	private void storeTuple(String key,String value,String source)throws IOException{		//store a tuple in the disk
		File tempFile = new File(source);
		if(!(tempFile.exists()&&tempFile.isFile())){		//if the file does not exist then create one!
			tempFile.createNewFile();
		}
		
		if(toDisk==null){
			toDisk=new FileOutputStream(source,true);
		}
		
		toDisk.write(key.getBytes());
		toDisk.write(0);
		toDisk.write(value.getBytes());
		toDisk.write(0);
		
	}
	
	
	/**
	 * read a tuple from a specified source file
	 * @param source the source file to read from 
	 * @return a response of the key value pair
	 * @throws IOException
	 */
	private DBResponse getTuple(String source)throws IOException{		//get a tuple from the disk. If used consecutively it will continue to get the next tuple
		File tempFile = new File(source);
		if(!(tempFile.exists()&&tempFile.isFile())){		//if the file does not exist then create one!
			tempFile.createNewFile();
		}
		
		if(fromDisk==null){
			fromDisk=new FileInputStream(source);
		}
		
		DBResponse ans=new DBResponse();
		
		
		while(true){		//read key
			int temp=fromDisk.read();
			if(temp==0){		//end of the key
				break;
			}
			if(temp==-1){		//error occured, end
				ans.success=false;
				return ans;
			}
			ans.key=ans.key+(char)temp;
		}
		
		while(true){		//read value
			int temp=fromDisk.read();
			if(temp==0){		//end of the value
				break;
			}
			if(temp==-1){		//error occured, end
				ans.success=false;
				return ans;
			}
			ans.value=ans.value+(char)temp;
		}
		ans.success=true;
		
		return ans;
	}
	
	/**
	 * removes a tuple in the database file with specified key 
	 * @param key the key of tuple to delete
	 * @return true if a tuple is removed
	 * @throws IOException
	 */
	
	private boolean removeTuple(String key)throws IOException{			//very high cost! We definitely need better design at the persistent file system but not now!
		//just making sure both files exist!
		File tempFile = new File(dataFileName);
		if(!(tempFile.exists()&&tempFile.isFile())){		
			tempFile.createNewFile();
		}
		tempFile = new File(tempFileName);
		if(!(tempFile.exists()&&tempFile.isFile())){		
			tempFile.createNewFile();
		}
		
		DBResponse temp;
		
		boolean ans=false;
		
		resetStream();
		while(true){
			temp=getTuple(dataFileName);
			if(temp.key.equals(key)){
				ans=true;
				continue;
			}
			if(temp.success==false){
				break;
			}
			storeTuple(temp.key, temp.value, tempFileName);
		}
		
		File dataFile=new File(dataFileName);
		if(!dataFile.delete()){		//unsuccessful removal of the original data file!
			return false;
		}
		
		tempFile=new File(tempFileName);
		if(!tempFile.renameTo(dataFile)){
			return false;
		}
		
		return ans;
	}
	
	/**
	 * reset both in and out streams of storage object
	 * @throws IOException
	 */
	private void resetStream()throws IOException{		//reset both streams to null and close them
		if(fromDisk!=null){
			fromDisk.close();
		}
		if(toDisk!=null){
			toDisk.close();
		}
		fromDisk=null;
		toDisk=null;
	}
	
	/*
	 * THESE ARE THE ONLY FUNCTIONS THAT THE USERS SHOULD USE!!!!!!
	 * */
	
	/*------------------------------API section---------------------------*/
	
	/**
	 * Get a key-value pair from the storage object
	 * @param key key of wanted pair
	 * @return	a DBResponse object contains the key-value pair. Also indicating if the operation is successful
	 * @throws IOException
	 */
	public DBResponse get(String key)throws IOException{
		resetStream();
		//we first tries to find the content in cache
		
		DBResponse result=cache.get(key);
		if(result.success){		//if found in cache, return
			return result;
		}
		
		//else we have to search through the whole file
		while(true){
			result=getTuple(dataFileName);
			if(result.success==false){		//possibly EOF
				return result;
			}
			if(result.key.equals(key)){		//found ya!
				cache.set(result.key,result.value);	//not in cache so we refresh the cache
				return result;
			}
		}
	}
	
	
	/**
	 * Try to insert, update, or delete a key-value pair to the storage object 
	 * @param key key of the tuple
	 * @param value value of the tuple
	 * @return a DBResponse indicating insert/update/delete is successful or not
	 * @throws IOException
	 */
	public DBResponse put(String key, String value)throws IOException{
		resetStream();

		cache.set(key,value);

		boolean success=removeTuple(key);
		DBResponse result=new DBResponse();
		result.key=key;
		result.value=value;
		result.success=false;
		result.isNewTuple=false;
		
		if((!success)&&(value==null)){
			return result;
		}
		
		if(value==null){
			result.success=true;
			return result;
		}
		
		resetStream();
		storeTuple(key, value, dataFileName);
		result.success=true;
		result.isNewTuple=!success;
		return result;
	}
	
	private boolean withinRange(String start, String end, String target){
		//start> stop
		
		//"FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF"
		String bound="FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF";
		String zero="00000000000000000000000000000000";
		
		if(start.compareTo(end)>0){				//go around
			
			if((start.compareTo(target) <0 && bound.compareTo(target)>=0)
					||(zero.compareTo(target) <=0 && end.compareTo(target)>=0))
				return true;
			else
				return false;
		}
		else{
			if((start.compareTo(target)<0 && end.compareTo(target)>=0) || (start.compareTo(end)==0 && start.compareTo(target)==0) ){
				return true;
			}
			else{
				return false;
			}
		}
	}
	
	private String getMD5String(String input) {
		byte[] bytesOfMessage = input.getBytes();
		byte[] thedigest = null;
		try{
			MessageDigest md = MessageDigest.getInstance("MD5");
			thedigest = md.digest(bytesOfMessage);
		}catch (Exception e){
			System.out.println("FATAL: MD5 not supported");
			System.exit(0);
		}
		String ret="";
		for(byte b:thedigest){
			ret=ret+String.format("%02X",b);
		}
		return ret;
	}
	
	
	public ArrayList<DBResponse> getInRange(String start,String end)throws IOException{
		ArrayList<DBResponse> result=new ArrayList<DBResponse>();
		
		
		File tempFile = new File(dataFileName);
		if(!(tempFile.exists()&&tempFile.isFile())){		
			tempFile.createNewFile();
		}
		
		DBResponse temp;
		
		resetStream();
		while(true){
			temp=getTuple(dataFileName);
			if(temp.success==false){
				break;
			}
			
			if(withinRange(start, end, getMD5String(temp.key))){
				result.add(temp);
			}
		}

		return result;
		
	}
	
	public boolean removeNotInRange(String start, String end)throws IOException{
		File tempFile = new File(dataFileName);
		if(!(tempFile.exists()&&tempFile.isFile())){		
			tempFile.createNewFile();
		}
		tempFile = new File(tempFileName);
		if(!(tempFile.exists()&&tempFile.isFile())){		
			tempFile.createNewFile();
		}
		
		cache.clear();
		
		DBResponse temp;
		
		boolean ans=false;
		
		resetStream();
		while(true){
			temp=getTuple(dataFileName);
			if(temp.success==false){
				break;
			}
			if(!withinRange(start, end, getMD5String(temp.key))){
				ans=true;
				continue;
			}

			storeTuple(temp.key, temp.value, tempFileName);
		}
		
		File dataFile=new File(dataFileName);
		if(!dataFile.delete()){		//unsuccessful removal of the original data file!
			return false;
		}
		
		tempFile=new File(tempFileName);
		if(!tempFile.renameTo(dataFile)){
			return false;
		}
		
		
		return ans;
	}
	
	/*-----------------------------end of API section---------------------------*/
	
	
	
	
	/*
	 * TESTING SECTION: ONLY USED FOR DEBUG PURPOSE. THE USER DO NOT NEED TO USE THESE FUNCTIONS
	 * 
	 **/
	
	
	/**
	 * dumps the content of the storage object(both cache and disk)
	 * @throws IOException
	 */
	public void dumpStorage()throws IOException{
		
		System.out.println("--------------Dump Start---------------");
		System.out.println("Cache Dump: ");
		System.out.println(cache);
		System.out.println("Disk Dump: ");
		DBResponse temp;
		resetStream();
		while(true){
			temp=getTuple(dataFileName);
			if(!temp.success){
				break;
			}
			System.out.println(temp);
		}
		System.out.println("");
		System.out.println("--------------Dump End---------------");
	}
	
	
	/*a mini testing shell*/
	
	/*
	 * Instructions: 
	 * 
	 * g ky: try to get value with key=ky
	 * p ky value: store the key value pair in the DB
	 * d ky: delete all existing records with key=ky
	 * q: exit the shell
	 * */
	
	/*
	public static void main(String []args)throws IOException{
		Storage s=new Storage(10, CacheType.FIFO);
		
		s.put("123","123");
		s.put("456","456");
		
		System.out.println(s.getMD5String("123"));
		System.out.println(s.getMD5String("456"));
		
		ArrayList<DBResponse> res=s.getInRange("250CF8B51C773F3F8DC8B4BE867A9A02", "250CF8B51C773F3F8DC8B4BE867A9A02");
		
		for(DBResponse d:res){
			System.out.println("key: "+d.key+", value: "+d.value);
		}
		
		s.removeNotInRange("250CF8B51C773F3F8DC8B4BE867A9A01","202CB962AC59075B964B07152D234B70");
		
		s.dumpStorage();
	}
	*/
	
}