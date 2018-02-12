package libs.java.extension.distributedmap.redis;

import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import redis.clients.jedis.Jedis;

public class RedisMapEventPublisher<V> {

	private Executor ex = null;
	private ObjectWriter writer;
	private Jedis publisher;
	private String channel;

	public RedisMapEventPublisher(String channel, Jedis publisher) {
		ex = Executors.newFixedThreadPool(1);
		ObjectMapper mapper = new ObjectMapper();
		writer = mapper.writer();
		this.publisher = publisher;
		this.channel = channel;
	}

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
