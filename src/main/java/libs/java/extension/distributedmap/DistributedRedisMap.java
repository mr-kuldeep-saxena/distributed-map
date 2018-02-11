package libs.java.extension.distributedmap;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

/**
 * Only key of type string is allowed. This is to make sure, key created on
 * different system does not gets complicated with different object id for
 * similar instance or to force override of equals and hashcode.
 * 
 * Enable - notify-keyspace-events configuration, this is required to propogate
 * changes made to map
 * -- set value to atleast - 
 * <br>
 * <b>notify-keyspace-events Kh</b>
 * <br>
 * @author Kuldeep
 *
 * @param <String>
 * @param <V>
 */
public class DistributedRedisMap<K, V> extends DistributedMap<String, V> {

	/*	*//**
			 * Redis details
			 */
	/*
	 * private String host, user, password; private int port;
	 */ /**
		 * Name for the map, used to associate key with redis
		 */
	private String name;

	private Jedis jedis;

	protected DistributedRedisMap(Map<String, V> rootMap, final String host, final int port, final String password,
			String mapName) {
		super(rootMap);
		jedis = new Jedis(host, port);
		if (password != null) {
			jedis.auth(password);
		}
		jedis.connect();
		this.name = mapName;
		final Jedis subscriber = new Jedis(host, port);

		new Thread(new Runnable() {

			public void run() {
				if (password != null) {
					subscriber.auth(password);
				}
				System.out.println("Subscribing");
				subscriber.subscribe(new PubSubListener(), "__keyspace@0__:" + name + " h");
			}
		}).start();

	}

	class PubSubListener extends JedisPubSub {
		@Override
		public void onMessage(String channel, String message) {
			System.out.println(channel + " " + message);
		}

		@Override
		public void onPMessage(String pattern, String channel, String message) {
			System.out.println(message);
		}

		@Override
		public void onSubscribe(String channel, int subscribedChannels) {
			System.out.println("Subscribed " + subscribedChannels + " " + channel);
		}

		@Override
		public void onUnsubscribe(String channel, int subscribedChannels) {

		}

		@Override
		public void onPUnsubscribe(String pattern, int subscribedChannels) {

		}

		@Override
		public void onPSubscribe(String pattern, int subscribedChannels) {

		}
	}

	public static <V> Map<String, V> newMap(String mapName, Map<String, V> rootMap, String host, int port) {
		return newMap(mapName, rootMap, host, port, null);
	}

	public static <V> Map<String, V> newMap(String mapName, Map<String, V> rootMap, String host, int port,
			String password) {
		if (mapName == null || mapName.length() < 1) {
			throw new IllegalStateException("Required map name is missing");
		}
		// if map name is already exist in redis, clear redis key to
		// reinitialize, it would not overwrite

		Map<String, V> map = new DistributedRedisMap<String, V>(rootMap, host, port, password, mapName);

		return map;
	}

	public void clear() {
		super.clear();
	}

	public boolean containsKey(Object key) {
		super.containsKey(key);
		return false;
	}

	public boolean containsValue(Object value) {
		super.containsValue(value);
		return false;
	}

	public Set<java.util.Map.Entry<String, V>> entrySet() {
		super.entrySet();
		return null;
	}

	public V get(Object key) {
		super.get(key);
		return null;
	}

	public boolean isEmpty() {
		return super.isEmpty();
	}

	public Set<String> keySet() {
		return super.keySet();
	}

	/**
	 * Returns old value only when available in local map, not adding excess
	 * operation to go to redis as of now
	 */
	public V put(String key, V value) {
		V old = super.put(key, value);
		// put to redis, need to serialize, value

		jedis.hset(name, key, String.valueOf(value));
		return old;
	}

	public void putAll(Map<? extends String, ? extends V> m) {
		super.putAll(m);

	}

	public V remove(Object key) {
		return super.remove(key);
	}

	public int size() {
		return jedis.hgetAll(name).size();
	}

	public Collection<V> values() {
		super.values();
		return null;
	}

}
