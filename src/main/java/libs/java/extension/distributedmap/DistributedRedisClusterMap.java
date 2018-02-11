package libs.java.extension.distributedmap;

import java.util.Map;

/**
 * Will see in future version
 * @author Kuldeep
 *
 * @param <String>
 * @param <V>
 */
public class DistributedRedisClusterMap<K,V> extends DistributedMap<String, V> {

	public DistributedRedisClusterMap(Map<String, V> underlyingMap) {
		super(underlyingMap);
		// TODO Auto-generated constructor stub
	}

	
}
