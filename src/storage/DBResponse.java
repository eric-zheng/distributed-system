package storage;

public class DBResponse {
	public String key;
	public String value;
	public boolean success;
	public boolean isNewTuple;

	DBResponse(String k, String v, boolean s) {
		key = k;
		value = v;
		success = s;
	}

	DBResponse() {
		key = new String();
		value = new String();
		success = false;
		isNewTuple = false;
	}

	public String toString() {
		if (success) {
			return "key: " + key + ",value: " + value + ",isNewTuple: " + isNewTuple;
		} else {
			return "";
		}
	}

}