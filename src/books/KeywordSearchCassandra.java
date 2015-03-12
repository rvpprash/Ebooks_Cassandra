package books;

import java.net.UnknownHostException;
import java.util.Scanner;
import java.util.UUID;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.QueryResult;


public class KeywordSearchCassandra {
	
	private static String id;
	private static Mutator<String> mutator;
	ColumnQuery<String, String, String> columnQuery = null;
    QueryResult<HColumn<String, String>> result = null;

	public void search(String searchkey) throws UnknownHostException {
		StringSerializer ss = StringSerializer.get();
		Cluster cluster = HFactory.getOrCreateCluster("Test Cluster", "127.0.0.1:9160");
		Keyspace keyspace = HFactory.createKeyspace("dbkeyspace", cluster);
		id = UUID.randomUUID().toString();
		mutator = HFactory.createMutator(keyspace, ss);
		columnQuery = HFactory.createStringColumnQuery(keyspace);
        columnQuery.setColumnFamily("documents").setKey(searchkey).setName("docs");
        result = columnQuery.execute();
        String str = result.get().getValue();
        if(str != null)
        System.out.println("Your search found the below documents: "+ str);
        /*String[] docs = str.split("|");
        System.out.println("Your search found the below documents: ");
        for(String s : docs)
        	System.out.println(s);*/
		
		else
			System.out.println("Sorry! No reults found.");
	}
		
	
	public static void main(String[] args) throws UnknownHostException {
		KeywordSearchCassandra kw = new KeywordSearchCassandra();
		Scanner keyboard = new Scanner(System.in);
		System.out.println("Enter the word to search: ");
		String word = keyboard.nextLine();
		long startTime = System.currentTimeMillis();
		kw.search(word);
		System.out.println("Total time taken: " + (System.currentTimeMillis()-startTime)+ "milliseconds");

	}

}
