package libs.java.extension.distributedmap;

import java.util.HashMap;
import java.util.Map;

public class Test {
	public static void main(String[] args) throws Exception {
		new Test();
	}

	Test() throws Exception {
		System.out.println("Testing");
		Map<String, Data> map = DistributedMap.newMap("mymap", new HashMap<String, Data>(),2, Data.class, "192.168.57.129", 6379);
		// Thread.sleep(2000);

		Data a = new Data();
		Data b = new Data();
		a.name = "A";
		a.vv = "Avv";
		b.name = "B";
		b.vv = "Bvv";
		//map.put("Hello1", a);

		Map<String, Data> entities = new HashMap<>();
		entities.put("Hello1", a);
		entities.put("Hello2", b);
		
		map.putAll(entities);
		map.put("Hello3", new Data());
		map.remove("Hello2");
		map.get("Hello3");
		map.clear();
		System.out
				.println("Old map size " + map.size() + " size local " + ((DistributedMap<String, Data>) map).sizeLocal());
		/*
		 * map.remove("Hello1"); map.put("Hello2", "World1");
		 */
		/*System.out.println(map.get("Hello2"));
		map.remove("Hello2");
		map.clear();*/
		Thread.sleep(5000);
		System.out.println(map.values());
		
		
		// map.clear();

	}

	
}
