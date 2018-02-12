package libs.java.extension.distributedmap.redis;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;

import libs.java.extension.distributedmap.DistributedMap;
import redis.clients.jedis.Jedis;

/**
 * Only key of type string is allowed. This is to make sure, key created on
 * different system does not gets complicated with different object id for
 * similar instance or to force override of equals and hashcode.
 * 
 * @author Kuldeep
 *
 * @param <String>
 * @param <V>
 */
public class DistributedRedisMap<K, V> extends DistributedMap<String, V> {

	private Jedis jedis;

	private ObjectWriter writer;
	private ObjectReader reader;
	private RedisMapEventPublisher<V> publisher;

	public DistributedRedisMap(String mapName, Map<String, V> rootMap, int localKeyLimit, Class<V> type,
			final String host, int port, String password) {
		super(mapName, rootMap, localKeyLimit);
		jedis = new Jedis(host, port);
		if (password != null) {
			jedis.auth(password);
		}
		ObjectMapper mapper = new ObjectMapper();
		writer = mapper.writer();
		reader = mapper.reader();
		reader = reader.forType(type);
		jedis.connect();
		this.name = mapName;
		super.localKeyLimit = localKeyLimit;

		initPubSubs(host, port, password, rootMap, type);
	}

	private void initPubSubs(String host, int port, String password, Map<String, V> rootMap, Class<V> type) {
		Jedis subscriber = new Jedis(host, port);

		if (password != null) {
			subscriber.auth(password);
		}
		Jedis publisher = new Jedis(host, port);

		if (password != null) {
			subscriber.auth(password);
		}

		new RedisMapEventSubscriber<V>(name, this, type, subscriber).init();
		this.publisher = new RedisMapEventPublisher<V>(name, publisher);
	}

	/**
	 * Clears local as well redis
	 */
	public void clear() {
		super.clearLocal();

		// delete the key
		jedis.del(name);
		publisher.publish(RedisCommand.CLEAR, null, null);

	}

	/**
	 * Expected all keys always in redis, only redis is checked
	 */
	public boolean containsKey(Object key) {
		return jedis.hexists(name, String.valueOf(key));
	}

	/**
	 * Expected all values always in redis, only redis is checked
	 */
	public boolean containsValue(Object value) {
		List<String> values = jedis.hvals(name);
		for (String aValue : values) {
			try {
				V v = reader.readValue(aValue);
				if (v != null && v.equals(value)) {
					return true;
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return false;
	}

	/**
	 * return entry set from redis, all the data
	 */
	public Set<java.util.Map.Entry<String, V>> entrySet() {
		Map<String, String> map = jedis.hgetAll(name);
		if (map == null) {
			return null;
		}
		Set<Entry<String, V>> entrySet = new HashSet<>();
		for (String key : map.keySet()) {
			try {
				Entry<String, V> e = new AbstractMap.SimpleEntry<String, V>(key, reader.readValue(map.get(key)));
				entrySet.add(e);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return entrySet;
	}

	/**
	 * Return value from redis
	 */
	public V get(Object key) {

		String s = jedis.hget(name, String.valueOf(key));
		if (s == null) {
			return null;
		}
		try {
			V value = reader.readValue(s);
			if (value != null) { // update localy as well, should not cause
									// redis update
				super.putLocal(String.valueOf(key), value);
				return value;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;

	}

	public boolean isEmpty() {
		return jedis.hlen(name).intValue() < 1;
	}

	public Set<String> keySet() {
		return jedis.hkeys(name);
	}

	/**
	 * Returns old value only when available in local map, not adding excess
	 * operation to go to redis as of now
	 */
	public V put(String key, V value) {
		V old = null;
		old = super.putLocal(key, value);
		String val = null;
		try {
			val = writer.writeValueAsString(value);
		} catch (Exception e) {
			e.printStackTrace();
		}

		// put to redis, need to serialize, value
		// put json value
		if (val != null) {
			jedis.hset(name, key, val);
			
			publisher.publish(RedisCommand.PUT, key, value);
		}
		return old;
	}

	public void putAll(Map<? extends String, ? extends V> m) {
		super.putAllLocal(m);
		Map<String, String> toPut = new HashMap<>();
		for (String key : m.keySet()) {
			try {
				String value = writer.writeValueAsString(m.get(key));
				toPut.put(key, value);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		jedis.hmset(name, toPut);
		publisher.publishMultiple(RedisCommand.PUT, m);
	}

	public V remove(Object key) {
		V out = super.removeLocal(key);
		jedis.hdel(name, String.valueOf(key));
		publisher.publish(RedisCommand.DELETE, String.valueOf(key), null);
		return out;
	}

	public V removeLocal(Object key) {
		return super.removeLocal(key);
	}

	public int size() {
		return jedis.hlen(name).intValue();
	}

	public Collection<V> values() {
		List<String> values = jedis.hvals(name);
		Collection<V> collection = new ArrayList<>();

		for (String value : values) {
			try {
				collection.add(reader.readValue(value));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		return collection;
	}

}
