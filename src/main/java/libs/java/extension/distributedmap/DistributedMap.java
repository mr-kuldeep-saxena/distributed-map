package libs.java.extension.distributedmap;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public abstract class DistributedMap<K, V> implements Map<String, V> {

	protected Map<String, V> underlyingMap;

	public DistributedMap(Map<String, V> underlyingMap) {
		if (underlyingMap == null) {
			throw new IllegalStateException("Passed map can't be null");
		}
		this.underlyingMap = underlyingMap;
	}

	public void clear() {
		underlyingMap.clear();
	}

	public boolean containsKey(Object key) {
		return underlyingMap.containsKey(key);

	}

	public boolean containsValue(Object value) {
		return underlyingMap.containsValue(value);
	}

	public Set<java.util.Map.Entry<String, V>> entrySet() {
		return underlyingMap.entrySet();
	}

	public V get(Object key) {
		return underlyingMap.get(key);
	}

	public boolean isEmpty() {
		return underlyingMap.isEmpty();
	}

	public Set<String> keySet() {
		return underlyingMap.keySet();
	}

	public V put(String key, V value) {
		return underlyingMap.put(key, value);
	}

	public void putAll(Map<? extends String, ? extends V> m) {
		underlyingMap.putAll(m);
	}

	public V remove(Object key) {
		return underlyingMap.remove(key);
	}

	public int size() {
		return underlyingMap.size();
	}

	public Collection<V> values() {
		return underlyingMap.values();
	}

}
