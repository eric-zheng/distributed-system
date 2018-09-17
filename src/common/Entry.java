package common;

public class Entry implements Comparable<Entry> {

	private String address;
	private String port;
	Range coordRange;
	Range replicaRange;

	// construct an Entry with given address, port, start, end
	public Entry(String address, String port, String start, String end, String startTwo, String endTwo) {
		this.coordRange = new Range(start, end);
		this.replicaRange = new Range(startTwo, endTwo);
		this.address = address;
		this.port = port;
	}

	// check if the input is inside this.range
	// input is a md5
	public boolean inCoordRange(String inputRange) {
		return coordRange.withInRange(inputRange);
	}

	public boolean inReplicaRange(String inputRange) {
		return replicaRange.withInRange(inputRange);
	}

	// return a string that constructed with proper rules to be sent out
	public String getEntryString() {
		String entryString = address;
		entryString = entryString.concat(":");
		entryString = entryString.concat(port);
		entryString = entryString.concat(":");
		entryString = entryString.concat(coordRange.getStartRange());
		entryString = entryString.concat(":");
		entryString = entryString.concat(coordRange.getEndRange());
		entryString = entryString.concat(":");
		entryString = entryString.concat(replicaRange.getStartRange());
		entryString = entryString.concat(":");
		entryString = entryString.concat(replicaRange.getEndRange());
		// System.out.println("Entry:"+entryString);
		return entryString;
	}

	// helper functions
	public String getAddressPortString() {
		return this.address + ":" + this.port;
	}

	public String getAddressString() {
		return this.address;
	}

	public String getPortString() {
		return this.port;
	}

	public int getPortInt() {
		return Integer.parseInt(port);
	}

	public String getCoordEndString() {
		return coordRange.getEndRange();
	}

	public String getCoordStartString() {
		return coordRange.getStartRange();
	}

	public void setCoordStart(String start) {
		this.coordRange.setStartRange(start);
	}

	// comparing, sort will follow this
	// overriding compareTo in order to sort the entry based on start range
	@Override
	public int compareTo(Entry comparestu) {
		// int compareage=((Entry)comparestu));
		/* For Ascending order */
		return this.getCoordEndString().compareTo(comparestu.getCoordEndString());

		/* For Descending order do like this */
		// return compareage-this.studentage;
	}

	public String getReplicaEndString() {
		return replicaRange.getEndRange();

	}

	public String getReplicaStartString() {
		return replicaRange.getStartRange();
	}

	public void setReplicaStart(String start) {
		this.replicaRange.setStartRange(start);

	}

	public void setReplicaEnd(String end) {
		this.replicaRange.setEndRange(end);
	}
}

// class that store start range and end range

class Range {
	private String start;
	private String end;

	public Range(String start, String end) {
		this.start = start;
		this.end = end;
	}

	// check if the given target is inside this.range
	public boolean withInRange(String target) {

		String bound = "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF";
		String zero = "00000000000000000000000000000000";

		boolean ans;

		if (start.compareTo(end) > 0) { // go around

			if ((start.compareTo(target) <= 0 && bound.compareTo(target) >= 0)
					|| (zero.compareTo(target) <= 0 && end.compareTo(target) >= 0))
				ans = true;
			else
				ans = false;
		} else {
			if ((start.compareTo(target) <= 0 && end.compareTo(target) >= 0)
					|| (start.compareTo(end) == 0 && start.compareTo(target) == 0)) {
				ans = true;
			} else {
				ans = false;
			}
		}

		return ans;
	}

	public String getStartRange() {
		return this.start;
	}

	public String getEndRange() {
		return this.end;
	}

	public void setStartRange(String startRange) {
		this.start = startRange;
	}

	public void setEndRange(String endRange) {
		this.end = endRange;
	}

	public String toString() {
		return start + ":" + end;
	}

}
