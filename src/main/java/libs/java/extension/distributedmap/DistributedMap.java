package libs.java.extension.distributedmap;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import libs.java.extension.distributedmap.redis.DistributedRedisMap;

/**
 * A decorated map with support to distributed operations on map. Current
 * implementation uses redis (See {@link DistributedRedisMap}) as shared storage
 * for map data.
 * 
 *
 * Basic features this map support - <br>
 * 1. Limit on Data which user want to be available in local map. Use value 0,
 * if you don't want any data stored locally in process <br>
 * 2. Data available to in memory is updated data even if updated in another
 * process (eventually lastest updated data)<br>
 * 3. Data propagation can take some time, so don't depend on accurate data in
 * all the process <br>
 * 4. Local data key limit does not mean that data is available only on local,
 * it is always stored in shared memory (Redis). At any moment all data is
 * always available in Redis <br>
 * 5. If redis has data already available, new process does not overwrite old
 * data, if you want afresh for that particular map, clear that either using map
 * operation or directly at Redis <br>
 * 6. Any update, deletion of key is propagated to all the communicating process
 * <br>
 * 7. On read of data which is not available in local map, it is copied from
 * shared memory (if available) to local (if local key limit allows) and
 * returned. <br>
 * 8. "<>", "><", "~~" patterns are used to publish events, and used in parsing,
 * these should not be part of key or value fields
 * 
 * See Word Document (How it works) for details of map operations.
 * 
 * @author Kuldeep
 *
 * @param <K>
 *            it is always a String. It avoid serialization/deserialization of
 *            key on operation, because most of map operations work on key, it
 *            makes it fast to use predefined type. Second current
 *            implementation uses Redis as data store which has inbuilt hash
 *            support for string key.
 * @param <V>
 *            Value of the key. It can be any java type. Currently it is
 *            converted to JSON before storing to Redis which makes it easier to
 *            read/view data directly at redis
 *
 *            <br>
 *            Simple usage - <br>
 *            Map<String, Data> map = DistributedMap.newMap("mymap", new
 *            HashMap<String, Data>(), 1000 (local key limit), Data.class,
 *            redisip, redisport); <br>
 *            use normal map operations.
 * 
 */
public abstract class DistributedMap<K, V> implements Map<String, V> {

	/**
	 * underlying map
	 */
	protected Map<String, V> underlyingMap;
	/**
	 * Max number of keys stored locally
	 */
	protected int localKeyLimit;

	/**
	 * Local key size
	 */
	protected int size;
	/**
	 * Name for the map, used to associate hash key with redis
	 */
	protected String name;

	/**
	 * Constructor, assigns different variables
	 * 
	 * @param name
	 *            name of the key/map
	 * @param underlyingMap
	 *            underlying map
	 * @param localKeyLimit
	 *            local key limit
	 * @throws IllegalStateException
	 *             if passed map is null
	 */
	public DistributedMap(String name, Map<String, V> underlyingMap, int localKeyLimit) {
		if (underlyingMap == null) {
			throw new IllegalStateException("Passed map can't be null");
		}
		this.localKeyLimit = localKeyLimit;
		this.underlyingMap = underlyingMap;
		this.name = name;
	}

	/**
	 * Clears only underlying map
	 */
	public void clearLocal() {
		underlyingMap.clear();
	}

	/**
	 * Returns only underlying map local size
	 * 
	 * @return size
	 */
	public int sizeLocal() {
		return underlyingMap.size();
	}

	/**
	 * If key is present locally
	 * 
	 * @param key
	 *            key
	 * @return true/false
	 */
	public boolean containsKeyLocal(Object key) {
		return underlyingMap.containsKey(key);

	}

	/**
	 * If value is present locally
	 * 
	 * @param value
	 *            value to check
	 * @return true/false
	 */
	public boolean containsValueLocal(Object value) {
		return underlyingMap.containsValue(value);
	}

	/**
	 * Return local entry set
	 * 
	 * @return local entry set
	 */
	public Set<java.util.Map.Entry<String, V>> entrySetLocal() {
		return underlyingMap.entrySet();
	}

	/**
	 * return value for key available locally
	 * 
	 * @param key
	 *            key
	 * @return value if available at local
	 */
	public V getLocal(Object key) {
		return underlyingMap.get(key);
	}

	/**
	 * Return if map is empty locally
	 * 
	 * @return true/false
	 */
	public boolean isEmptyLocal() {
		return underlyingMap.isEmpty();
	}

	/**
	 * Return key set of map from local
	 * 
	 * @return Ket {@link Set}
	 */
	public Set<String> keySetLocal() {
		return underlyingMap.keySet();
	}

	/**
	 * Put data in local
	 * 
	 * @param key
	 *            key
	 * @param value
	 *            value
	 * @return old data for key
	 */
	public V putLocal(String key, V value) {

		if (!containsKeyLocal(key)) {
			if (size < localKeyLimit) {
				size++;
				return underlyingMap.put(String.valueOf(key), value);
			}
		} else {
			return underlyingMap.put(String.valueOf(key), value);
		}
		return null;

	}

	/**
	 * Put all the data to local map 
	 * @param m
	 *            map
	 * 
	 */
	public void putAllLocal(Map<? extends String, ? extends V> m) {
		if (m.size() < (localKeyLimit - size)) {
			underlyingMap.putAll(m);
		}else{
			// put iterating
			for (String key:m.keySet()){
				if (localKeyLimit <= size){
					break;
				}
				putLocal(key, m.get(key));
			}
		}
		
	}

	/**
	 * Remove key from local map
	 * 
	 * @param key
	 *            key to be removed
	 * @return value
	 */
	public V removeLocal(Object key) {
		V out = underlyingMap.remove(key);
		if (out != null) {
			size--;
		}
		return out;
	}

	/**
	 * Returns collection of values from local
	 * 
	 * @return values
	 */
	public Collection<V> valuesLocal() {
		return underlyingMap.values();
	}

	/**
	 * Factory method creates redis map
	 * 
	 * @param mapName
	 *            name for the map
	 * @param rootMap
	 *            underlying map
	 * @param localKeyLimit
	 *            local key limit
	 * @param type
	 *            class type of value, used to convert to JSON
	 * @param host
	 *            redis host
	 * @param port
	 *            redis port
	 * @return {@link DistributedRedisMap}
	 */
	public static <V> Map<String, V> newMap(String mapName, Map<String, V> rootMap, int localKeyLimit, Class<V> type,
			String host, int port) {
		return newMap(mapName, rootMap, localKeyLimit, type, host, port, null);
	}

	/**
	 * Factory method creates redis map
	 * 
	 * @param mapName
	 *            name for the map
	 * @param rootMap
	 *            underlying map
	 * @param localKeyLimit
	 *            local key limit
	 * @param type
	 *            class type of value, used to convert to JSON
	 * @param host
	 *            redis host
	 * @param port
	 *            redis port
	 * @param password
	 *            password if any of redis
	 * @return {@link DistributedRedisMap}
	 */
	public static <V> Map<String, V> newMap(String mapName, Map<String, V> rootMap, int localKeyLimit, Class<V> type,
			String host, int port, String password) {
		if (mapName == null || mapName.length() < 1) {
			throw new IllegalStateException("Required map name is missing");
		}
		// if map name is already exist in redis, clear redis key to
		// reinitialize, it would not overwrite

		Map<String, V> map = new DistributedRedisMap<String, V>(mapName, rootMap, localKeyLimit, type, host, port,
				password);

		return map;
	}

	@Override
	public String toString() {
		return underlyingMap.toString();
	}

}
