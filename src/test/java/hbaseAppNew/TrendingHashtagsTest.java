package hbaseAppNew;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import org.junit.Before;
import org.junit.Test;

import junit.framework.Assert;
import master2016.TrendingHashtagsHbase;

public class TrendingHashtagsTest {

	List<HashMap<String, Long>> maps = new ArrayList<HashMap<String, Long>>();
	TrendingHashtagsHbase app = new TrendingHashtagsHbase();

	@Before
	public void setUp() throws Exception {
		HashMap<String, Long> hashMap1 = new HashMap<String, Long>();
		HashMap<String, Long> hashMap2 = new HashMap<String, Long>();
		hashMap1.put("Hillary", 15L);
		hashMap1.put("TrumpB", 5L);
		hashMap1.put("TrumpA", 5L);
		hashMap2.put("Hillary", 5L);
		hashMap2.put("TrumpB", 5L);
		hashMap2.put("TrumpA", 5L);
		maps.add(hashMap1);
		maps.add(hashMap2);

	}

	@Test
	public void testMergingMaps() {
		HashMap<String, Long> newMap = app.mergeMaps(maps);
		Assert.assertEquals(20, newMap.get("Hillary").longValue());
	}

	@Test
	public void testSortingMaps() {
		HashMap<String, Long> newMap = app.mergeMaps(maps);
		List<Entry<String, Long>> listMap = app.sortByComparator(newMap);
		Assert.assertEquals("TrumpA", listMap.get(1).getKey());
		Assert.assertEquals(10L, listMap.get(1).getValue().longValue());
	}

}
