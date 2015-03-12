package books;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

public class LoadToDBCassandra
{
	private static final String UTF8 = "UTF8";
	private static final String HOST = "localhost";
	private static final int PORT = 9160;
	private static final ConsistencyLevel CL = ConsistencyLevel.ALL;


	private List<File> fileList = new ArrayList<File>();
	private List<String> ignoreWordList = new ArrayList<String>();
	
	private static String id;
	// Word index cache
	private Map<String, HashSet<String>> wordMap = new HashMap<String, HashSet<String>>();	
	private static Mutator<String> mutator;

	public static void main(String[] args) throws TTransportException, IllegalArgumentException, TException, NotFoundException, Exception 
	{
		
		StringSerializer ss = StringSerializer.get();
		Cluster cluster = HFactory.getOrCreateCluster("Test Cluster", "127.0.0.1:9160");
		Keyspace keyspace = HFactory.createKeyspace("dbkeyspace", cluster);
		id = UUID.randomUUID().toString();
		mutator = HFactory.createMutator(keyspace, ss);
		LoadToDBCassandra mainObj = new LoadToDBCassandra();
		// Get the list of words to ignore
		File swf = new File("Data/stopwords.txt");
		mainObj.ignoreWordList = mainObj.getWordsInIgnoreFile(swf);

		// Get files from file system to the array list.
		File dir = new File("Data/books");

		mainObj.getFiles(dir);
		// Ok now we have fileList populated.
		boolean indexInDB = false;
		long startTime = System.currentTimeMillis();
		for (File fRef : mainObj.fileList) {
			System.out.println("processing " + fRef.getName());
			mainObj.generateWordIndex(fRef,indexInDB);
		}
		if(!indexInDB)
			mainObj.persistToDB();
		long totalTime = (System.currentTimeMillis() - startTime);
		System.out.println("Total time taken : " +totalTime);

		cluster.getConnectionManager().shutdown();
	}

	private String getFormattedString(Set<String> inputSet)
	{
		StringBuffer returnString = new StringBuffer("");
		for(String str : inputSet)
		{
			returnString.append(str).append("|");
		}
		return returnString.toString();
	}

	private void persistToDB() throws UnknownHostException {

		for (String word : wordMap.keySet()) {
			//System.out.println("inseting");
			mutator.insert(word, "documents", HFactory.createStringColumn("docs", getFormattedString(wordMap.get(word))));
		}
	}

	/**
	 * Populate the wordMap ( word index cache ) for each file passed to this
	 * method
	 * 
	 * @param fRef
	 * @throws IOException
	 */
	private void generateWordIndex(File fRef, boolean indexInDB) throws IOException {
		for (String word : getWordsInAFile(fRef)) {
			if(word.length() > 5)
				putWordToIndex(word, fRef.getName(), indexInDB);
		}
	}

	/**
	 * 
	 * @param word
	 * @return
	 */
	private Set<String> getWordFromIndex(String word, boolean indexInDB) {
		//if (!indexInDB)
		return wordMap.get(word);
		//		else {
		//
		//			//return getFromDB(word);
		//		}

	}

	private void putWordToIndex(String word, String fRefName,boolean indexInDB) {
		if (!indexInDB) {
			if (getWordFromIndex(word, indexInDB) == null)
				insertNewtoIndex(word, fRefName, indexInDB);
			getWordFromIndex(word, indexInDB).add(fRefName);
		} else {
			Set<String> oldSet;
			if ((oldSet = getWordFromIndex(word, indexInDB)) != null) {
				//Set<String> oldSet = getWordFromIndex(word, true);
				Set<String> newSet = new HashSet<String>();
				newSet.addAll(oldSet);
				newSet.add(fRefName);
				//updateToIndex(word,newSet);
			}
			else
			{
				insertNewtoIndex(word, fRefName, true);
			}
		}

	}

	
	private void insertNewtoIndex(String word, String fRefName,boolean indexInDB) {
		if (!indexInDB)
			wordMap.put(word, new HashSet<String>());
		else {
			
		}
	}

	
	/**
	 * 
	 * @param f
	 * @return
	 * @throws IOException
	 */
	private List<String> getWordsInIgnoreFile(File f) throws IOException {
		List<String> ignoreList = new ArrayList<String>();
		BufferedReader rdr = new BufferedReader(new FileReader(f));
		String line = null;
		try {
			while ((line = rdr.readLine()) != null) {
				if (!line.startsWith("#"))
					ignoreList.add(line.trim().toLowerCase());
			}
		} finally {
			rdr.close();
		}

		return ignoreList;
	}

	/**
	 * Get all the words in a file
	 * 
	 * @param f
	 * @return
	 * @throws IOException
	 */
	private Set<String> getWordsInAFile(File f) throws IOException {
		Set<String> wordSet = new HashSet<String>();
		BufferedReader rdr = new BufferedReader(new FileReader(f));
		String line = null;
		try {
			while ((line = rdr.readLine()) != null) {
				String[] parts = line.trim().split(
						"[\\s,\\.:;\\-#~\\(\\)\\?\\!\\&\\*\\\"\\/\\'\\`]");// doubt
				for (String p : parts) {
					if (!ignoreWordList.contains(p)) {
						wordSet.add(p);
					}
				}
			}
		} finally {
			rdr.close();
		}

		return wordSet;
	}

	/**
	 * @throws IOException
	 * 
	 */

	private void getFiles(File f) {
		if (f == null)
			return;
		if (f.isFile())
			fileList.add(f);
		else {
			discoverFiles(f);
		}
	}

	/**
	 * 
	 * @param dir
	 */
	private void discoverFiles(File dir) {
		System.out.println(dir);
		if (dir == null || dir.isFile())
			return;

		File[] dirs = dir.listFiles(new FileFilter() {

			@Override
			public boolean accept(File f) {
				if (f.getName().startsWith(".")) {
					// ignore
				} else if (f.isFile())
					fileList.add(f);

				else if (f.isDirectory()) {
					discoverFiles(f);
				}

				return false;
			}
		});

	}

}
