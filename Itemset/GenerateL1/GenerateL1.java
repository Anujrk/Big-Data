import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.*;

public class GenerateL1 {
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

        Map<Integer, Integer> id_set = new HashMap<Integer, Integer>();
        List<Document> ids = collection.find().into(
                new ArrayList<Document>());
        for (Document id : ids) {
            List<Integer> items = (List<Integer>) id.get("items");
            for (Integer item : items) {
                Integer j = id_set.get(item);
                id_set.put(item, (j == null) ? 1 : j + 1);
            }
        }
        for (int key : id_set.keySet()) {
            if (id_set.get(key) >= S) {
                BasicDBObject N = new BasicDBObject();
                BasicDBObject item = new BasicDBObject("pos_0", key );
                N.put("_id", item);
                N.put("count", id_set.get(key));
                new_collection.insert(N);
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
