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

public class RedisMapEventSubscriber<V> extends JedisPubSub implements Runnable {

	private Executor ex = null;
	private DistributedMap<String, V> map;
	private ObjectReader reader;
	private Jedis subscriber;
	private String channel;

	public RedisMapEventSubscriber(String channel, DistributedMap<String, V> map, Class<V> type, Jedis subscriber) {
		this.map = map;
		ex = Executors.newFixedThreadPool(1);
		ObjectMapper mapper = new ObjectMapper();
		reader = mapper.reader();
		reader = reader.forType(type);
		this.subscriber = subscriber;
		this.channel = channel;
	}

	public void init() {

		Thread subscribe = new Thread(this);
		subscribe.setDaemon(true);
		subscribe.start();
	}

	/**
	 * TO-DO ignore event generated from same map
	 * @param channel channel id
	 * @param message received message
	 */
	@Override
	public void onMessage(String channel, String message) {
		if (message == null) {
			return;
		}
		System.out.println(message);
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
					if (map.containsKey(key)) {
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
