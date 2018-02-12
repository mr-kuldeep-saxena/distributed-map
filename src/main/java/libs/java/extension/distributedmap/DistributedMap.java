package libs.java.extension.distributedmap;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import libs.java.extension.distributedmap.redis.DistributedRedisMap;

public abstract class DistributedMap<K, V> implements Map<K, V> {

	protected Map<String, V> underlyingMap;
	protected int localKeyLimit, size;
	/**
	 * Name for the map, used to associate key with redis
	 */
	protected String name;

	public DistributedMap(String name, Map<String, V> underlyingMap, int localKeyLimit) {
		if (underlyingMap == null) {
			throw new IllegalStateException("Passed map can't be null");
		}
		this.localKeyLimit = localKeyLimit;
		this.underlyingMap = underlyingMap;
		this.name = name;
	}

	public void clearLocal() {
		underlyingMap.clear();
	}

	public int sizeLocal() {
		return underlyingMap.size();
	}

	public boolean containsKeyLocal(Object key) {
		return underlyingMap.containsKey(key);

	}

	public boolean containsValueLocal(Object value) {
		return underlyingMap.containsValue(value);
	}

	public Set<java.util.Map.Entry<String, V>> entrySetLocal() {
		return underlyingMap.entrySet();
	}

	public V getLocal(Object key) {
		return underlyingMap.get(key);
	}

	public boolean isEmptyLocal() {
		return underlyingMap.isEmpty();
	}

	public Set<String> keySetLocal() {
		return underlyingMap.keySet();
	}

	/**
	 * To-Do - Should contain logic to keep key limit value
	 * 
	 * @param key
	 * @param value
	 * @return
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

	public void putAllLocal(Map<? extends String, ? extends V> m) {
		underlyingMap.putAll(m);
	}

	public V removeLocal(Object key) {
		V out = underlyingMap.remove(key);
		if (out != null) {
			size--;
		}
		return out;
	}

	public Collection<V> valuesLocal() {
		return underlyingMap.values();
	}

	public static <V> Map<String, V> newMap(String mapName, Map<String, V> rootMap, int localKeyLimit, Class<V> type,
			String host, int port) {
		return newMap(mapName, rootMap, localKeyLimit, type, host, port, null);
	}

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
