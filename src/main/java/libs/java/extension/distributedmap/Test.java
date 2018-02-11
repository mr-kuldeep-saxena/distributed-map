package libs.java.extension.distributedmap;

import java.util.HashMap;
import java.util.Map;

public class Test {
	public static void main(String[] args) throws Exception{
		System.out.println("Testing");
		Map<String, String> map = DistributedRedisMap.newMap("mymap", new HashMap<String, String>(), "localhost", 6379);
		Thread.sleep(2000);
		System.out.println(map);
		map.put("Hello", "World2");
		System.out.println(map.size());
	}
}
