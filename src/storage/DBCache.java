package storage;

import java.lang.*;

/*
 * currently this is designed for a write through policy
 * */

class DBCache {

	/* metadata */
	private int size;
	private CacheType type;

	/*
	 * for now we use a array for actual cache. maybe later we will change this for
	 * better performance
	 */
	private DBCacheNode cache[];
	private int time; // timestamp use by FIFO and LRU

	DBCache(CacheType t, int s) {
		type = t;
		size = s;
		cache = new DBCacheNode[size];
		for (int i = 0; i < size; i++) {
			cache[i] = null;
		}
		time = 0;
	}

	/**
	 * search through the cache and try to find a matching tuple with the specified
	 * key
	 * 
	 * @param key the key of the desired tuple
	 * @return a response object containing the pair
	 */
	public DBResponse get(String key) { // if the value pair does not exist then return a false response
		// checkCache();

		DBResponse ans = new DBResponse();

		for (int i = 0; i < size; i++) {
			if ((cache[i] != null) && (cache[i].key.equals(key))) { // found a key matching!
				ans.key = key;
				ans.value = cache[i].value;
				ans.success = true;

				if (type == CacheType.FIFO) {
					// do nothing
				} else if (type == CacheType.LRU) {
					cache[i].indicator = time;
					time++;
				} else { // LFU
					cache[i].indicator++;
				}
				return ans;
			}
		}

		ans.success = false;
		return ans;
	}

	/**
	 * insert a tuple into the cache. if the cache is full then eviction algorithm
	 * is used
	 * 
	 * @param key   the key of the tuple
	 * @param value the value of the tuple
	 * @return true if insertion is successful
	 */
	public boolean set(String key, String value) {
		// checkCache();

		for (int i = 0; i < size; i++) {
			if (cache[i] == null) {
				continue;
			}

			if (cache[i].key.equals(key)) { // found key

				if (value == null) { // storing a null value means delete!
					cache[i] = null;
				} else {
					cache[i].value = value;
					if (type == CacheType.FIFO) {
						// do nothing
					} else if (type == CacheType.LRU) {
						cache[i].indicator = time;
						time++;
					} else { // LFU
						cache[i].indicator++;
					}
				}
				return true;
			}

		}

		if (value == null) {
			return false;
		}

		for (int i = 0; i < size; i++) { // else we try to find an empty spot in the cache to fill the info in
			if (cache[i] == null) {
				cache[i] = new DBCacheNode();
				cache[i].key = key;
				cache[i].value = value;
				if (type == CacheType.FIFO) {
					cache[i].indicator = time;
					time++;
				} else if (type == CacheType.LRU) {
					cache[i].indicator = time;
					time++;
				} else { // LFU
					cache[i].indicator = 0;
				}
				return true;
			}
		}

		// else we use the eviction algorithm to find a spot that works
		int min = cache[0].indicator;
		int pos = 0;

		for (int i = 0; i < size; i++) { // evict the entry with the minimum indicator
			if (cache[i].indicator < min) {
				min = cache[i].indicator;
				pos = i;
			}
		}

		cache[pos] = null;

		// then we insert again

		cache[pos] = new DBCacheNode();
		cache[pos].key = key;
		cache[pos].value = value;
		if (type == CacheType.FIFO) {
			cache[pos].indicator = time;
			time++;
		} else if (type == CacheType.LRU) {
			cache[pos].indicator = time;
			time++;
		} else { // LFU
			cache[pos].indicator = 0;
		}

		return true;
	}

	/**
	 * remove a tuple with the specified key from the cache
	 * 
	 * @param key key of the wanted cache
	 * @return true if the tuple is found and deleted
	 */
	public boolean delete(String key) { // returns true if the content is deleted
		for (int i = 0; i < size; i++) {
			if ((cache[i].key != null) && (cache[i].key.equals(key))) {
				cache[i] = null;
				return true;
			}
		}
		return false;
	}

	public void clear() {
		for (int i = 0; i < size; i++) {
			cache[i] = null;
		}
	}

	/* getter and setters */
	public int getSize() {
		return size;
	}

	public CacheType getType() {
		return type;
	}

	/* debug tools */
	/**
	 * checks if the cache is consistent or not
	 */
	private void checkCache() { // cache consistency checker. purely used for debug
		if (time < 0) { // overflowed! clear the cache and reset the timer!
			time = 0;
			for (int i = 0; i < size; i++) {
				cache[i] = null;
			}
		}
		for (int i = 0; i < size; i++) {
			if (cache[i] != null) {
				if ((cache[i].key == null) || (cache[i].value == null) || (cache[i].indicator) < 0) {
					return;
				}
			}
		}

	}

	public String toString() {
		String ans = new String();
		for (int i = 0; i < size; i++) {
			if (cache[i] != null) {
				ans = ans + "indicator: " + cache[i].indicator + ", key: " + cache[i].key + ", value: " + cache[i].value
						+ "\n";
			}
		}
		return ans;
	}

}

class DBCacheNode {
	public int indicator; // different functionality for different types of cache
	public String key;
	public String value;
}