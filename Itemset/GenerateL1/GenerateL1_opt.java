import com.google.common.collect.*;
import com.google.common.primitives.Ints;
import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

public class GenerateL1_opt {

    static String mongoDBURL;
    static String D;
    static String COLLECTION_NAME;
    static String NEW_COLLECTION;
    static int S;

    private static MongoClient getClient(String u) {
        MongoClient client = null;
        LinkedHashMap<Integer, Integer> iid = new LinkedHashMap<>();
        if (u.equals("None")) client = new MongoClient();
        else client = new MongoClient(new MongoClientURI(u));
        return client;
    }

    public static void genrateL1() {
        MongoClient client = getClient(mongoDBURL);
        DB db = client.getDB(D);
        MongoDatabase database = client.getDatabase(D);
        MongoCollection<Document> collection = database
                .getCollection(COLLECTION_NAME);
        DBCollection new_collection = db.getCollection(NEW_COLLECTION);

        Multimap<Integer, Integer> id_set = ArrayListMultimap.create();
        List<Document> ids = collection.find().into(
                new ArrayList<Document>());
        for (Document id : ids) {
            ArrayList<ArrayList<Integer>> id_item = new ArrayList<>();
            List<Integer> items = (List<Integer>) id.get("items");
            for (Integer item : items) {
                if (id_set.keySet().contains(item)) {
                    id_set.put(item, (Integer) id.get("_id"));
                } else {
                    id_set.put(item, (Integer) id.get("_id"));
                }
            }
        }

        int i = 1;
        for (int key : id_set.keySet()) {
            if (id_set.get(key).size() >= S) {
                BasicDBObject N = new BasicDBObject();
                BasicDBObject item = new BasicDBObject("pos_0", key );
                N.put("_id", item);
                N.put("transactions", id_set.get(key));
                N.put("order",i);
                new_collection.insert(N);
                i++;
            }
        }
    }

    public static void main(String[] args) {
        mongoDBURL = args[0];
        D = args[1];
        COLLECTION_NAME = args[2];
        NEW_COLLECTION = args[3];
        S = Integer.parseInt(args[4]);
        genrateL1();
    }


}
