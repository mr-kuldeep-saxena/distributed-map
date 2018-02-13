package libs.java.extension.distributedmap.redis;

import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import redis.clients.jedis.Jedis;

/**
 * Event publisher for redis. It runs on a separate thread, which does not block
 * caller/operation thread
 * 
 * @author Kuldeep
 *
 * @param <V>
 *            value
 */
public class RedisMapEventPublisher<V> {

	/**
	 * Executor, not block caller
	 */
	private Executor ex = null;
	/**
	 * JSON data writer
	 */
	private ObjectWriter writer;
	/**
	 * Redis connection
	 */
	private Jedis publisher;
	/**
	 * Channel = map name, to identify event are for which map
	 */
	private String channel;

	public RedisMapEventPublisher(String channel, Jedis publisher) {
		ex = Executors.newFixedThreadPool(1);
		ObjectMapper mapper = new ObjectMapper();
		writer = mapper.writer();
		this.publisher = publisher;
		this.channel = channel;
	}

	/**
	 * Publish to redis
	 * @param command command
	 * @param key key
	 * @param element data
	 */
	public void publish(RedisCommand command, String key, V element) {
		if (command == null) {
			return;
		}
		ex.execute(new Runnable() {

			@Override
			public void run() {
				if (command == RedisCommand.CLEAR) {
					publisher.publish(channel, "clear");
					return;
				}
				if (command == RedisCommand.DELETE) {
					publisher.publish(channel, "delete<>" + key);
					return;
				}
				if (command == RedisCommand.PUT) {
					StringBuffer values = new StringBuffer();
					try {
						String value = writer.writeValueAsString(element);
						values.append(key + "~~" + value);
					} catch (Exception e) {
						e.printStackTrace();
					}
					publisher.publish(channel, "put<>" + values.toString());
				}

			}
		});
	}

	/**
	 * Publish event to redis
	 * @param command command
	 * @param elements elements
	 */
	public void publishMultiple(RedisCommand command, Map<? extends String, ? extends V> elements) {
		if (command == null) {
			return;
		}
		ex.execute(new Runnable() {

			@Override
			public void run() {
				if (command == RedisCommand.CLEAR) {
					publisher.publish(channel, "clear");
					return;
				}
				if (command == RedisCommand.DELETE) {
					StringBuffer values = new StringBuffer();
					for (String key : elements.keySet()) {
						try {
							values.append(key + "><");
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
					publisher.publish(channel, "delete<>" + values.toString());
					return;
				}
				if (command == RedisCommand.PUT) {
					StringBuffer values = new StringBuffer();
					for (String key : elements.keySet()) {
						try {
							String value = writer.writeValueAsString(elements.get(key));
							values.append(key + "~~" + value + "><");
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
					System.out.println(values.toString());
					publisher.publish(channel, "put<>" + values.toString());
				}

			}
		});
	}

}
