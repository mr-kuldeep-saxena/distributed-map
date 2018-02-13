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
 * An implementation of {@link DistributedMap} using redis as shared storage.
 * See Dis
 *
 * @see DistributedMap
 * 
 * @author Kuldeep
 *
 * @param <String>
 * @param <V>
 */
public class DistributedRedisMap<K, V> extends DistributedMap<String, V> {

	/**
	 * Jedis client for redis
	 */
	private Jedis jedis;

	/**
	 * JSON writer
	 */
	private ObjectWriter writer;

	/**
	 * JSON reader
	 */
	private ObjectReader reader;

	/**
	 * Publisher of map event to redis
	 */
	private RedisMapEventPublisher<V> publisher;

	/**
	 * Creates Distributed map
	 * 
	 * @param mapName
	 *            name of map
	 * @param rootMap
	 *            underlying map
	 * @param localKeyLimit
	 *            local key limit
	 * @param type
	 *            class type of V
	 * @param host
	 *            redis host
	 * @param port
	 *            redis port
	 * @param password
	 *            password of redis if any
	 */
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

	/**
	 * Initialized pub/subs
	 * 
	 * @param host
	 *            redis host
	 * @param port
	 *            redis port
	 * @param password
	 *            redis password
	 * @param rootMap
	 *            underlying map
	 * @param type
	 *            class type of V
	 */
	private void initPubSubs(String host, int port, String password, Map<String, V> rootMap, Class<V> type) {
		// New jedis client
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
		// publish event
		publisher.publish(RedisCommand.CLEAR, null, null);

	}

	/**
	 * Expected all keys always in redis, only redis is checked
	 * 
	 * @return true/false
	 */
	public boolean containsKey(Object key) {
		return jedis.hexists(name, String.valueOf(key));
	}

	/**
	 * Expected all values always in redis, only redis is checked
	 * 
	 * @return true/false
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
	 * Return entry set from redis, all the data created by all process
	 * 
	 * @return entry set
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
	 * 
	 * @return value from redis
	 */
	public V get(Object key) {

		// get latest updated value
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

	/**
	 * Return if map is empty or has elements
	 * 
	 * @return true/false
	 */
	public boolean isEmpty() {
		return jedis.hlen(name).intValue() < 1;
	}

	/**
	 * Returns key set from redis
	 * 
	 * @return key {@link Set}
	 */
	public Set<String> keySet() {
		return jedis.hkeys(name);
	}

	/**
	 * Put value to redis.
	 * 
	 * @param key
	 *            key
	 * @param value
	 *            value
	 * @return Returns old value only when available in local map, not adding
	 *         excess operation to go to redis as of now
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
			// generate event to update other processes to update value if
			// cached locally
			publisher.publish(RedisCommand.PUT, key, value);
		}
		return old;
	}

	/**
	 * Put all the elements to map
	 * 
	 * @param m
	 *            map from which to put element to root map
	 */
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

	/**
	 * Remove and return element
	 * @param key to remove 
	 * @return element removed
	 */
	public V remove(Object key) {
		V out = super.removeLocal(key);
		jedis.hdel(name, String.valueOf(key));
		// publish delete event
		publisher.publish(RedisCommand.DELETE, String.valueOf(key), null);
		return out;
	}

	/**
	 * size from redis
	 * @return size
	 */
	public int size() {
		return jedis.hlen(name).intValue();
	}

	/**
	 * Values from redis
	 * @return values
	 */
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
