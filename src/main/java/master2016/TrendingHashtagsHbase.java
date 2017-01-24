/**
 * @author Tobias Lindener
 */
package master2016;

import java.io.BufferedReader;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.Collator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.collect.ImmutableMap;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.conf.Configuration;

public class TrendingHashtagsHbase {
	private String GROUP_ID = "10";
	private TableName tableName;
	private Configuration hbaseConf;
	private HTable hbaseTable;

	public enum Query {
		Query1, Query2, Query3
	}

	public TrendingHashtagsHbase(String zookeeper) {
		tableName = TableName.valueOf("twitterStats");
		hbaseConf = HBaseConfiguration.create();
		String[] zookeeperDetails = zookeeper.split(":");
		hbaseConf.set("hbase.zookeeper.quorum", zookeeperDetails[0]);
		hbaseConf.set("hbase.zookeeper.property.clientPort", zookeeperDetails[1]);

	}

	public TrendingHashtagsHbase(String zookeeper, String groupId, String hbaseTableName) {
		this.GROUP_ID = groupId;
		tableName = TableName.valueOf(hbaseTableName);
		hbaseConf = HBaseConfiguration.create();
		String[] zookeeperDetails = zookeeper.split(":");
		hbaseConf.set("hbase.zookeeper.quorum", zookeeperDetails[0]);
		hbaseConf.set("hbase.zookeeper.property.clientPort", zookeeperDetails[1]);

	}

	public TrendingHashtagsHbase() {
		// TODO Auto-generated constructor stub
	}

	private void writeResult(Map<String, Long> intervalTopTopic, Query query, String lang, String startTimestamp,
			String endTimestamp, String output_folder, int N) {
		List<Entry<String, Long>> resultList = sortByComparator(intervalTopTopic);
		int position = 1;
		for (Map.Entry<String, Long> entry : resultList) {
			writeQueryOutput(query, lang, position, entry.getKey(), startTimestamp, endTimestamp, output_folder,
					entry.getValue().toString());
			if (position == N)
				break;
			else
				position++;
		}
	}

	private HashMap<String, Long> executeQuery(String startTimestamp, String endTimestamp, int N, String lang) {
		openTable();
		Scan scan = new Scan(generateKey(startTimestamp), generateKey(endTimestamp));
		scan.addFamily(Bytes.toBytes(lang));
		ResultScanner rs;
		HashMap<String, Long> resultMap = null;
		try {
			rs = hbaseTable.getScanner(scan);
			Result res = rs.next();
			resultMap = new HashMap<String, Long>();

			while (res != null && !res.isEmpty()) {
				byte[] topic_bytes = res.getValue(Bytes.toBytes(lang), Bytes.toBytes("TOPIC"));
				byte[] count_bytes = res.getValue(Bytes.toBytes(lang), Bytes.toBytes("COUNTS"));
				String topic = Bytes.toString(topic_bytes).toString();
				String count = Bytes.toString(count_bytes).toString();
				Long value = (long) Integer.parseInt(count);
				if (resultMap.containsKey(topic)) {

					resultMap.put(topic, resultMap.get(topic) + value);
				} else {
					resultMap.put(topic, value);
				}

				res = rs.next();
			}
			return resultMap;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * Given a language (lang), find the Top-N most used words for the given
	 * language in a time interval defined with a start and end timestamp. Start
	 * and end timestamp are in milliseconds.
	 */
	public void findNMostUsedWordsByLanguageAndTime(String startTimestamp, String endTimestamp, int N, String language,
			String outputFolderPath) {
		String METHOD_NAME = "findNMostUsedWordsByLanguageAndTime";
		System.out.println("Executing " + METHOD_NAME + " {" + language + "} ");
		HashMap<String, Long> resultMap = executeQuery(startTimestamp, endTimestamp, N, language);
		writeResult(resultMap, Query.Query1, language, startTimestamp, endTimestamp, outputFolderPath, N);
	}

	/**
	 * Find the list of Top-N most used words for each language in a time
	 * interval defined with the provided start and end timestamp. Start and end
	 * timestamp are in milliseconds.
	 */
	public void findListOfTopNMostUsedWordsByLanguageAndTime(String startTimestamp, String endTimestamp, int N,
			String[] languages, String outputFolderPath) {
		String METHOD_NAME = "findListOfTopNMostUsedWordsByLanguageAndTime";
		System.out.println("Executing " + METHOD_NAME + " {" + Arrays.toString(languages) + "} ");
		// ensure table is open, since direct hbaseTable access exists outside
		// of query execution
		if (hbaseTable == null) {
			openTable();
		}
		for (int i = 0; i < languages.length; i++) {
			try {
				if (hbaseTable.getTableDescriptor().hasFamily(Bytes.toBytes(languages[i]))) {
					HashMap<String, Long> resultMap = executeQuery(startTimestamp, endTimestamp, N, languages[i]);
					writeResult(resultMap, Query.Query2, languages[i], startTimestamp, endTimestamp, outputFolderPath,
							N);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	/**
	 * Find the Top-N most used words and the frequency of each word regardless
	 * the language in a time interval defined with the provided start and end
	 * timestamp. Start and end timestamp are in milliseconds.
	 */
	public void findTopNMostUsedWordsAndFrequencyByTime(String startTimestamp, String endTimestamp, int N,
			String outputFolderPath) {
		String METHOD_NAME = "findTopNMostUsedWordsAndFrequencyByTime";
		System.out.println("Executing " + METHOD_NAME);
		// ensure table is open, since direct hbaseTable access exists outside
		// of query execution
		if (hbaseTable == null) {
			openTable();
		}

		ArrayList<HashMap<String, Long>> maps = new ArrayList<HashMap<String, Long>>();
		try {
			String[] queryLanguages = new String[hbaseTable.getTableDescriptor().getColumnFamilies().length];
			for (int i = 0; i < hbaseTable.getTableDescriptor().getColumnFamilies().length; i++) {
				queryLanguages[i] = hbaseTable.getTableDescriptor().getColumnFamilies()[i].getNameAsString();
				HashMap<String, Long> resultMap = executeQuery(startTimestamp, endTimestamp, N, queryLanguages[i]);
				maps.add(resultMap);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		HashMap<String, Long> topTopics = mergeMaps(maps);
		writeResult(topTopics, Query.Query3, null, startTimestamp, endTimestamp, outputFolderPath, N);
	}

	public HashMap<String, Long> mergeMaps(List<HashMap<String, Long>> maps) {
		final HashMap<String, Long> topicMap = new HashMap<String, Long>();
		boolean first = true;
		for (Map<String, Long> map : maps) {
			if (first) {
				topicMap.putAll(map);
				first = false;
			} else {
				for (Entry<String, Long> entry : map.entrySet()) {
					if (!topicMap.containsKey(entry.getKey())) {
						topicMap.put(entry.getKey(), entry.getValue());
					} else {
						topicMap.put(entry.getKey(), topicMap.get(entry.getKey()) + entry.getValue());
					}

				}
			}
		}
		return topicMap;
	}

	private void createTable(String[] languages) {
		String METHOD_NAME = "createTable";
		System.out.println("Executing " + METHOD_NAME + " {" + Arrays.toString(languages) + "} ");

		try {
			HBaseAdmin hbaseAdmin = new HBaseAdmin(hbaseConf);
			if (!hbaseAdmin.tableExists(tableName)) {
				HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
				for (int i = 0; i < languages.length; i++) {
					tableDescriptor.addFamily(new HColumnDescriptor(languages[i]));
				}
				hbaseAdmin.createTable(tableDescriptor);
			}
			hbaseAdmin.close();
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void openTable() {
		String METHOD_NAME = "openTable";
		System.out.println("Executing " + METHOD_NAME);
		try {
			HConnection conn = HConnectionManager.createConnection(hbaseConf);
			hbaseTable = new HTable(tableName, conn);

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void insertDataIntoTable(String timestamp, String lang, String hashtag, String counts, int topic_pos) {
		byte[] key = generateKey(timestamp, lang, Integer.toString(topic_pos));
		Get get = new Get(key);
		Result res;
		try {
			res = hbaseTable.get(get);
			if (res != null) {
				Put put = new Put(key);
				put.add(Bytes.toBytes(lang), Bytes.toBytes("TOPIC"), Bytes.toBytes(hashtag));
				put.add(Bytes.toBytes(lang), Bytes.toBytes("COUNTS"), Bytes.toBytes(counts));
				hbaseTable.put(put);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void loadDataInHbase(String dataFolder) {
		String METHOD_NAME = "loadDataInHbase";
		System.out.println("Executing " + METHOD_NAME + " {" + dataFolder + "} ");
		File folder = new File(dataFolder);
		File[] files = folder.listFiles();

		String[] languages = new String[files.length];
		for (int i = 0; i < files.length; i++) {
			File file = files[i];
			if (file.isFile() && file.getName().endsWith(".out")) {
				languages[i] = file.getName().split(".out")[0];
				System.out.println(languages[i]);
			}
		}
		// create new table, but fails gracefully if table already exists
		createTable(languages);
		openTable();
		readData(files);

	}

	private void readData(File[] files) {
		for (int i = 0; i < files.length; i++) {
			File file = files[i];
			// read only "valid files"
			if (file.isFile() && file.getName().endsWith(".out")) {
				try (BufferedReader br = new BufferedReader(new FileReader(file))) {
					for (String line; (line = br.readLine()) != null;) {
						String[] fields = line.split(",");
						System.out.println(line);
						String timestamp = fields[0];
						String lang = fields[1];
						// start after timestamp and language
						int pos = 2;
						// iterate over pairs, be flexible in case of topN
						// approach instead of top3
						int lineNumber = 1;
						while (pos < fields.length) {
							insertDataIntoTable(timestamp, lang, fields[pos++], fields[pos++], lineNumber);
							lineNumber++;
						}
					}
					System.out.println("Data loaded sucessfully");
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	private String constructQueryOutput3(int position, String word, String startTS, String endTS, String frequency) {
		StringBuilder sb = new StringBuilder();
		sb.append(position);
		sb.append(",");
		sb.append(word);
		sb.append(",");
		// sb.append(frequency);
		// sb.append(",");
		sb.append(startTS);
		sb.append(",");
		sb.append(endTS);
		return sb.toString();
	}

	private String constructQueryOutput(int position, String language, String word, String startTS, String endTS,
			String frequency) {
		StringBuilder sb = new StringBuilder();
		sb.append(language);
		sb.append(",");
		sb.append(position);
		sb.append(",");
		sb.append(word);
		sb.append(",");
		// sb.append(frequency);
		// sb.append(",");
		sb.append(startTS);
		sb.append(",");
		sb.append(endTS);
		return sb.toString();
	}

	private void writeQueryOutput(Query query, String language, int position, String word, String startTS, String endTS,
			String outputPath, String frequency) {
		File file = new File(outputPath, GROUP_ID + "_" + query.toString().toLowerCase() + ".out");
		String content = null;
		if (query == Query.Query3) {
			// construct query 3 specific output
			content = constructQueryOutput3(position, word, startTS, endTS, frequency);
		} else {
			content = constructQueryOutput(position, language, word, startTS, endTS, frequency);
		}
		BufferedWriter bw = null;
		try {
			bw = new BufferedWriter(new FileWriter(file, true));
			bw.append(content);
			bw.newLine();
			bw.close();
		} catch (FileNotFoundException ex) {
			ex.printStackTrace();
		} catch (IOException ex) {
			ex.printStackTrace();
		}
		System.out.println("File written");
	}

	// helper methods for hbase access
	private byte[] generateKey(String timestamp) {
		byte[] key = new byte[44];
		System.arraycopy(Bytes.toBytes(timestamp), 0, key, 0, timestamp.length());
		return key;
	}

	private byte[] generateKey(String timestamp, String lang, String topic_pos) {
		byte[] key = new byte[44];
		System.arraycopy(Bytes.toBytes(timestamp), 0, key, 0, timestamp.length());
		System.arraycopy(Bytes.toBytes(lang), 0, key, 20, lang.length());
		System.arraycopy(Bytes.toBytes(topic_pos), 0, key, 20, topic_pos.length());
		return key;
	}

	public static List<Entry<String, Long>> sortByComparator(Map<String, Long> unsortMap) {

		List<Entry<String, Long>> list = new LinkedList<Entry<String, Long>>(unsortMap.entrySet());

		// Sorting the list based on values
		Collections.sort(list, new Comparator<Entry<String, Long>>() {
			public int compare(Entry<String, Long> o1, Entry<String, Long> o2) {

				// compare based on the count of the hashtag
				int value = o2.getValue().compareTo(o1.getValue());
				// additionally compare based on the alphabetical order
				int key = o1.getKey().compareTo(o2.getKey());

				// if the values are equal, compare based on the hashtag option
				if (value != 0)
					return value;
				else
					return key;

			}
		});

		return list;
	}

}
