package libs.java.extension.distributedmap.redis;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

import libs.java.extension.distributedmap.DistributedMap;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

/**
 * Listen for event from redis and updates local map. Call map in separate
 * thread
 * 
 * @author Kuldeep
 *
 * @param <V>
 *            value
 */
public class RedisMapEventSubscriber<V> extends JedisPubSub implements Runnable {

	/**
	 * Executor with single thread
	 */
	private Executor ex = null;
	/**
	 * Distributed map to update
	 */
	private DistributedMap<String, V> map;
	/**
	 * JSON reader
	 */
	private ObjectReader reader;
	/**
	 * Redis connection
	 */
	private Jedis subscriber;
	/**
	 * Channel/map name
	 */
	private String channel;

	/**
	 * Constructor
	 * 
	 * @param channel
	 *            channel/mapname
	 * @param map
	 *            distributed map
	 * @param type
	 *            value type
	 * @param subscriber
	 *            redis connection
	 */
	public RedisMapEventSubscriber(String channel, DistributedMap<String, V> map, Class<V> type, Jedis subscriber) {
		this.map = map;
		ex = Executors.newFixedThreadPool(1);
		ObjectMapper mapper = new ObjectMapper();
		reader = mapper.reader();
		reader = reader.forType(type);
		this.subscriber = subscriber;
		this.channel = channel;
	}

	/**
	 * Initializes subscriber
	 */
	public void init() {

		Thread subscribe = new Thread(this);
		subscribe.setDaemon(true);
		subscribe.start();
	}

	/**
	 * 
	 * Redis callback on publish event
	 * 
	 * TO-DO ignore event generated from same process, ignore for now, not that
	 * much costly
	 * 
	 * @param channel
	 *            channel/map name
	 * @param message
	 *            received message
	 */
	@Override
	public void onMessage(String channel, String message) {
		if (message == null) {
			return;
		}
		String result[] = message.split("<>");
		if (result == null || result.length != 2) {
			return;
		}
		RedisCommand c = RedisCommand.valueOf(result[0].toUpperCase());
		if (c == RedisCommand.PUT) {
			String elements[] = result[1].split("><");
			Map<String, V> elementsMap = new HashMap<>();
			for (String element : elements) {
				try {
					String keyValue[] = element.split("~~");
					elementsMap.put(keyValue[0], reader.readValue(keyValue[1]));
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			ex.execute(new SubscriberThread(c, elementsMap));
		}
		if (c == RedisCommand.CLEAR) {
			ex.execute(new SubscriberThread(c, null));

		}

		if (c == RedisCommand.DELETE) {
			String elements[] = result[1].split("><");
			Map<String, V> elementsMap = new HashMap<>();
			for (String element : elements) {
				try {
					elementsMap.put(element, null); // only key is there
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			ex.execute(new SubscriberThread(c, elementsMap));

		}
	}

	/**
	 * Thread to update map on command
	 * @author Kuldeep
	 *
	 */
	private class SubscriberThread implements Runnable {

		private RedisCommand command;
		private Map<String, V> elements;

		public SubscriberThread(RedisCommand command, Map<String, V> elements) {
			this.command = command;
			this.elements = elements;

		}

		@Override
		public void run() {
			if (command == RedisCommand.PUT) {
				for (String key : elements.keySet()) {
					if (map.containsKeyLocal(key)) {
						// update if contain same key with new value
						map.putLocal(key, elements.get(key));
					}
				}
			}

			if (command == RedisCommand.DELETE) {
				for (String key : elements.keySet()) {
					if (map.containsKey(key)) {
						// update if contain same key with new value
						map.removeLocal(key);
					}
				}
			}

			if (command == RedisCommand.CLEAR) {
				map.clearLocal();
			}

		}
	}

	@Override
	public void run() {
		subscriber.subscribe(this, channel);
	}

}
