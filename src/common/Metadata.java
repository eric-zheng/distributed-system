package common;

import java.math.BigInteger;
import java.util.*;
import Helper.Helper;
import common.*;

//MD5 is 32 bytes long string

//ip address formula will be: xxx.xxx.xxx.xxx:XXXXX:start:end_xxx.xxx.xxx.xxx:XXXX:start:end  X:portNumber  each message split by '|'
//last one will not use separate mark "_"
//MD5 is 32 bytes long string

//ip address formula will be: xxx.xxx.xxx.xxx:XXXXX:start:end_xxx.xxx.xxx.xxx:XXXX:start:end  X:portNumber  each message split by '|'
//last one will not use separate mark "_"
public class Metadata {

	ArrayList<Entry> metaData;
	String sendMeta;

	// input is a metadatastring
	public Metadata(String metaDataString) {

		this.metaData = new ArrayList<Entry>();

		for (String entry : metaDataString.split("_")) {
			String[] part = entry.split(":");
			metaData.add(new Entry(part[0], part[1], part[2], part[3], part[4],
					part[5]));
		}
	}

	public void setMetaData(String metaDataString) {
		this.metaData = new ArrayList<Entry>();

		for (String entry : metaDataString.split("_")) {
			String[] part = entry.split(":");
			metaData.add(new Entry(part[0], part[1], part[2], part[3], part[4],
					part[5]));
		}
	}

	// this constructor is to parse the input array list, sort in order,
	// calculate their range and load them into the metaData
	// need to change , assume at lease three server
	public Metadata(ArrayList<String> nodes) {
		
		// assume nodes are in form of
		ArrayList<String> replicaStart = new ArrayList<String>();
		this.metaData = new ArrayList<Entry>();
		for (String oneNode : nodes) {
			String[] hold = oneNode.split(":");
			metaData.add(new Entry(hold[0], hold[1], "0", Helper
					.getMD5String(oneNode), "0", Helper.getMD5String(oneNode)));
		}
		
		Collections.sort(metaData);

		String start = "00000000000000000000000000000000";
		BigInteger hold;
		BigInteger one = new BigInteger("1");
		BigInteger result;
		for (Entry abc : metaData) {

			replicaStart.add(start);
			abc.setCoordStart(start);
			abc.setReplicaStart("00000000000000000000000000000000");
			start = abc.getCoordEndString();
			one = new BigInteger("1");
			hold = new BigInteger(start, 16);

			result = hold.add(one);
			start = result.toString(16);
			start = start.toUpperCase();

			if (start.length() > 32) {
				start = start.substring(start.length() - 32);
			}

			if (start.length() < 32) {
				while (start.length() < 32) {
					start = "0" + start;
				}
			}

			if (start.length() != 32) {
				System.out.println("Wrong string length in METADATA");
			}
			// hold = Long.parseLong(start, 16) ;
			// start = Long.toHexString(hold);

		}

		one = new BigInteger("1");
		hold = new BigInteger(start, 16);
		start = hold.toString(16);
		start = start.toUpperCase();
		if (start.length() > 32) {
			start = start.substring(start.length() - 32);
		}
		if (start.length() < 32) {
			while (start.length() < 32) {
				start = "0" + start;
			}
		}
		if (start.length() != 32) {
			System.out.println("Wrong string length in METADATA");
		}
		this.metaData.get(0).setCoordStart(start);
		replicaStart.add(start);
		replicaStart.remove(0);
		Collections.rotate(replicaStart, -(metaData.size() - 3));

		ListIterator<Entry> it1 = metaData.listIterator();
		Iterator<String> it2 = replicaStart.iterator();
		// set start for all the
		for (int i = 0; i < metaData.size() && i < metaData.size(); i++) {
			metaData.get(i).setReplicaStart(replicaStart.get(i));
			// System.out.println("Add one");
			// System.out.println("R Start string is"+metaData.get(i).getReplicaStartString());
			// System.out.println("R End string is"+metaData.get(i).getReplicaEndString());
			// metaData.get(i).setReplicaEnd(Helper.getMD5String(metaData.get(i).getAddressPortString()));
		}
		// System.out.println("Check Meta");
		// for(Entry wtf : metaData){
		// System.out.println(wtf.getEntryString());
		// }
		// Collections.sort(metaData);

	}

	// input is address:port string
	// public Entry removeNode(String node){
	//
	// String start = null;
	// String end = null;
	// String address = null;
	// String port = null;
	//
	// //find the entry that is related to the adding or removing node
	// for (Entry nodeOI: this.metaData){
	// if(nodeOI.inRange(Helper.getMD5String(node)) == true){
	// start = nodeOI.getStartString();
	// }
	// }
	// if(start == null){
	// System.out.println("Did not find the range and name while trying to remove node");
	// }else{
	//
	// BigInteger find= new BigInteger(Helper.getMD5String(node),16); // md5 end
	// for the new node
	// BigInteger one = new BigInteger("1");
	// BigInteger damn = find.add(one);
	// String search = damn.toString(16);
	// search = search.toUpperCase(); //next bit, which should be the start of
	// the next node
	//
	//
	// for(Entry nodeOI: this.metaData){
	// if(nodeOI.inRange(search)){
	// address = nodeOI.getAddressString();
	// port = nodeOI.getPortString();
	// }
	// }
	// if(address== null){
	// System.out.println("Did not find the next server while trying to remove node");
	// }
	// else{
	// Entry last = new Entry(address,port,start,Helper.getMD5String(node));
	// return last;
	// }
	//
	// }
	// return null;
	//
	//
	// }
	//
	// public Entry addNode(String node){
	//
	// String start = null;
	// String end = null;
	// String address = null;
	// String port = null;
	//
	// for (Entry nodeOI: this.metaData){
	// if(nodeOI.inRange(Helper.getMD5String(node)) == true){
	// start = nodeOI.getStartString();
	// end = nodeOI.getEndString();
	// address = nodeOI.getAddressString();
	// port = nodeOI.getPortString();
	// }
	// }
	//
	// // System.out.println("Range found is "+address+port+end);
	// if(end == null){
	// System.out.println("Did not find the range and name while trying to add node");
	// }
	// else{
	//
	//
	// // System.out.println("start is" +start);
	// // System.out.println("MD5 is "+ Helper.getMD5String(node));
	// //next bit, which should be the start of the next node
	// Entry last = new Entry(address,port,start,Helper.getMD5String(node));
	// return last;
	//
	// }
	//
	// return null;
	// }

	// input is IP:port
	public Entry getNextServer(String input) {

		BigInteger search = new BigInteger(Helper.getMD5String(input), 16);
		BigInteger one = new BigInteger("1");
		BigInteger rs = search.add(one);
		String realrs = rs.toString(16);
		realrs = realrs.toUpperCase();

		if (realrs.length() > 32) {
			realrs = realrs.substring(realrs.length() - 32);
		}

		if (realrs.length() < 32) {
			while (realrs.length() < 32) {
				realrs = "0" + realrs;
			}
		}

		// we move the range from the current input to the next range, if we
		// find the entry, it will be the next one
		// System.out.println("Search is:"+realrs);
		return searchCoord(realrs);

	}

	public boolean requestReplicaInRange(String server, String request) {

		Entry result = searchReplica(Helper.getMD5String(request), server);

		if (result == null) {
			return false;
		} else {
			if (result.getAddressPortString().equals(server)) {
				return true;
			} else {
				return false;
			}
		}
	}

	// given a range, return any entry that is inside the range
	public Entry searchReplica(String MDFive, String server) {
		for (Entry iter : metaData) {
			if (iter.inReplicaRange(MDFive)) {
				if (iter.getAddressPortString().equals(server)
						|| server == null) {
					return iter;
				}
			}
		}
		return null;
	}

	public boolean requestCoordInRange(String server, String request) {
		Entry result = searchCoord(Helper.getMD5String(request));
		if (result == null) {
			System.out.println("Error: request in Range did not find the proper server with given address and port");
			return false;
		} else {
			if (result.getAddressPortString().equals(server)) {
				return true;
			} else {
				return false;
			}
		}
	}

	public Entry searchCoord(String MDFive) {
		for (Entry iter : metaData) {
			if (iter.inCoordRange(MDFive)) {
				return iter;
			}
		}
		System.out.println("find nothing");
		return null;
	}

	// generate metadata string for sending
	public String sendMeta() {
		String send = new String();
		for (Entry iter : metaData) {
			send = send.concat(iter.getEntryString());
			send = send.concat("_");
		}
		return send.substring(0, send.length() - 1);
	}

	// test function, adding a specific metadata
	public void addMeta(String a, String b, String c, String d, String e,
			String f) {
		metaData.add(new Entry(a, b, c, d, e, f));
		System.out.println("addSuccess");
	}

	/**
	 * return the range of given server in metadata, return a string in format
	 * of "start:end"
	 * 
	 * @param server
	 * @return
	 */
	public String getReplicaRange(String server) {

		for (Entry iter : metaData) {
			if (iter.inReplicaRange(Helper.getMD5String(server))
					&& iter.getAddressPortString().equals(server)) {
				return iter.getReplicaStartString() + ":"
						+ iter.getReplicaEndString();
			}
		}
		return null;
	}

	public String getCoordRange(String server) {

		for (Entry iter : metaData) {
			if (iter.inCoordRange(Helper.getMD5String(server))) {
				return iter.getCoordStartString() + ":"
						+ iter.getCoordEndString();
			}
		}
		return null;
	}
	
	public ArrayList<Entry> getMetaEntry(){
		
		return this.metaData;
	}
	


}
