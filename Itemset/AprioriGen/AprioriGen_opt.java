import com.google.common.collect.*;
import com.google.common.primitives.Ints;
import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.collections4.ListUtils;
import org.bson.Document;

import java.util.*;

public class AprioriGen_opt {
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

    public static void AprioriGen() {
        MongoClient client = getClient(mongoDBURL);
        DB db = client.getDB(D);
        MongoDatabase database = client.getDatabase(D);
        MongoCollection<Document> collection = database
                .getCollection(COLLECTION_NAME);
        DBCollection new_collection = db.getCollection(NEW_COLLECTION);
        Multimap<ArrayList<Integer>, Integer> Lk_id = ArrayListMultimap.create();

        HashMap<ArrayList<Integer>, ArrayList<Integer>> id_set = new HashMap<>();
        List<Document> ids = collection.find().batchSize(1).into(
                new ArrayList<Document>());

        for (Document id : ids) {
            Document R_ids = (Document) id.get("_id");
            ArrayList<Integer> id_array = new ArrayList<>();
            for (int i = 0; i < R_ids.size(); i++) {
                Integer R_id = (Integer) R_ids.get("pos_" + i);
                id_array.add(i, R_id);
                Collections.sort(id_array);
            }
            if (!id_set.containsKey(id_array)) {
                ArrayList<Integer> tid = (ArrayList<Integer>) id.get("transactions");
                Collections.sort(id_array);
                Collections.sort(tid);
                id_set.put(id_array, tid);
            }
        }
            Set<ArrayList<Integer>> copyid = id_set.keySet();
            for (ArrayList<Integer> key : id_set.keySet()) {
                for (ArrayList<Integer> copykey : copyid) {
                    ArrayList<Integer> intersect = (ArrayList<Integer>) ListUtils.intersection(id_set.get(key), id_set.get(copykey));
                    if (key != copykey && (intersect.size() >= S) && key.subList(0, key.size() - 1).equals(copykey.subList(0, copykey.size() - 1))) {
                        ArrayList<Integer> temp1 = new ArrayList<>();
                        temp1.addAll(key);
                        temp1.add(copykey.get(copykey.size() - 1));
                        Collections.sort(temp1);
                        if (!Lk_id.containsKey(temp1)) {
                            for(Integer i : intersect) {
                                Collections.sort(temp1);
                                Lk_id.put(temp1, i);
                            }
                        }
                    }
                }
            }


        int j = 1;
        for (ArrayList<Integer> temp : Lk_id.keySet()) {
            boolean flag = true;
            Set<Integer> temp2 = new LinkedHashSet<>(temp);
            Set<Set<Integer>> combos = Sets.combinations(ImmutableSet.copyOf(temp2), temp.size() - 1);
            for (Set temp3 : combos) {
                ArrayList<Integer> temp4 = new ArrayList<>(temp3);
                if (!id_set.containsKey(temp4)) {
                    flag = false;
                }
            }
            if (flag) {
                BasicDBObject item = new BasicDBObject();
                BasicDBObject N = new BasicDBObject();
                int i = 0;
                for (Integer temp5 : temp) {
                    item.put("pos_" + i, temp5);
                    i++;
                    N.put("_id", item);
                }
                N.put("transactions", Lk_id.get(temp));
                N.put("order",j);
                j++;
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
        AprioriGen();
    }

}
