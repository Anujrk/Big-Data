import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.*;

public class GenerateLK {
    static String mongoDBURL;
    static String D;
    static String Transaction_NAME;
    static String COLLECTION_NAME;
    static int S;
    static String NEW_COLLECTION;

    private static MongoClient getClient(String u) {
        MongoClient client = null;
        LinkedHashMap<Integer, Integer> iid = new LinkedHashMap<>();
        if (u.equals("None")) client = new MongoClient();
        else client = new MongoClient(new MongoClientURI(u));
        return client;
    }

    private static void GenerateLk() {
        MongoClient client = getClient(mongoDBURL);
        DB db = client.getDB(D);
        MongoDatabase database = client.getDatabase(D);
        MongoCollection<Document> transactions = database
                .getCollection(Transaction_NAME);
        MongoCollection<Document> collection = database
                .getCollection(COLLECTION_NAME);
        DBCollection new_collection = db.getCollection(NEW_COLLECTION);
        LinkedList<ArrayList<Integer>> forcount = new LinkedList<>();
        LinkedHashMap<ArrayList<Integer>, Integer> id_set = new LinkedHashMap<>();
        LinkedHashMap<ArrayList<Integer>, Integer> lk_set = new LinkedHashMap<>();
        List<Document> ids = transactions.find().into(
                new ArrayList<Document>());
        for (Document id : ids) {
            ArrayList<Integer> items = (ArrayList<Integer>) id.get("items");
            forcount.add(items);
        }
        System.out.println(forcount);
        List<Document> id = collection.find().batchSize(10).into(
                new ArrayList<Document>());
        for (Document idtemp : id) {
            Document R_ids = (Document) idtemp.get("_id");
            ArrayList<Integer> id_array = new ArrayList<>();
            for (int i = 0; i < R_ids.size(); i++) {
                Integer R_id = (Integer) R_ids.get("pos_" + i);
                id_array.add(i, R_id);
                Collections.sort(id_array);
            }
            if (!id_set.containsKey(id_array)) {
                Integer count = (Integer) idtemp.get("count");
                id_set.put(id_array, count);
            }
        }
        System.out.println(id_set);
        for (ArrayList<Integer> a1 : id_set.keySet()) {
            int i = 0;
            for (ArrayList<Integer> c1 : forcount) {
                if (c1.containsAll(a1)) {
                    i++;
                }
            }
            System.out.println(a1 + " : "+ i);
            if (i >= S) {
                lk_set.put(a1, i);
            }
        }
        for (ArrayList<Integer> l1 : lk_set.keySet()) {
            BasicDBObject item = new BasicDBObject();
            BasicDBObject N = new BasicDBObject();
            int i = 0;
            for (Integer temp5 : l1) {
                item.put("pos_" + i, temp5);
                i++;
                N.put("_id", item);
            }
            N.put("count", lk_set.get(l1));
            System.out.println(N);
            new_collection.insert(N);
        }
    }


    public static void main(String[] args) {
        mongoDBURL = args[0];
        D = args[1];
        Transaction_NAME = args[2];
        COLLECTION_NAME = args[3];
        NEW_COLLECTION = args[4];
        S = Integer.parseInt(args[5]);
        GenerateLk();
    }
}

