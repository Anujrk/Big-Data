import com.google.common.collect.ImmutableMultimap;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.collections4.map.LinkedMap;
import org.bson.Document;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class ComputeSilhouetteCoefficient {
    static String mongoDBURL;
    static String D;
    static String COLLECTION_NAME;

    private static MongoClient getClient(String u) {
        MongoClient client = null;
        if (u.equals("None")) client = new MongoClient();
        else client = new MongoClient(new MongoClientURI(u));
        return client;
    }

    public static void SSH() {
        MongoClient client = getClient(mongoDBURL);
        MongoDatabase database = client.getDatabase(D);
        DB db = client.getDB(D);
        MongoCollection<Document> collection = database
                .getCollection(COLLECTION_NAME);
        DBCollection collectionin = db.getCollection(COLLECTION_NAME);
        List<Document> ids = collection.find().into(
                new ArrayList<Document>());
//        Multimap<Integer, ArrayList<Double>> Points = ArrayListMultimap.create();
        ImmutableMultimap.Builder<Integer, ArrayList<Double>> map = ImmutableMultimap.builder();

        LinkedMap<ArrayList<Double>, Integer> Centroids = new LinkedMap<>();

        //Gathering Data into Structures
        for (Document id : ids) {
            if ((Boolean) id.get("isCentroid")) {
                ArrayList<Double> items = (ArrayList<Double>) id.get("point");
                Centroids.put(items, (Integer) id.get("label"));
            } else {
                ArrayList<Double> items = (ArrayList<Double>) id.get("point");
                map.put((Integer) id.get("label"), items);
            }
        }
        ImmutableMultimap<Integer, ArrayList<Double>> Points = map.build();
        System.out.println(Points);


        for (ArrayList<Double> point : Points.values()) {
            double a = 0.0;
            int total = 0;
            double b = Double.POSITIVE_INFINITY;
            double S;
            for (ArrayList<Double> point1 : Points.values()) {
                LinkedList<Integer> temp = new LinkedList(Points.inverse().get(point1));
                LinkedList<Integer> temp2 = new LinkedList(Points.inverse().get(point));
                if (temp.get(0) == temp2.get(0)) {
                    double EucleidDist = 0.0;
                    for (int i = 0; i < point.size(); i++) {
                        EucleidDist += Math.pow(point.get(i) - point1.get(i), 2);
                    }
                    a += Math.sqrt(Math.abs(EucleidDist));
                    total += 1;
                }
            }
            a = a / total;

            for (int i = 0; i < Centroids.size(); i++) {
                LinkedList<Integer> temp2 = new LinkedList(Points.inverse().get(point));
                if (temp2.get(0) != i) {
                    double h =0.0;
                    int total2 = 0;

                    for (ArrayList<Double> tempPoint : Points.get(i)) {
                            double EucleidDist = 0.0;
                            for (int j = 0; j < point.size(); j++) {
                                EucleidDist += Math.pow(point.get(j) - tempPoint.get(j), 2);
                            }
                            h += Math.sqrt(EucleidDist);
                            total2 += 1;
                    }
                    h = h/ total2;
                    if (h<b){
                        b = h;
                    }
                }
            }
            S = (b - a) /Math.max(a,b);
            System.out.println(S);
            collection.updateMany(new Document("point", point).append("isCentroid", false),
                            new Document("$set", new Document("S", S)));
            }
        }

        public static void main (String[]args){
            mongoDBURL = args[0];
            D = args[1];
            COLLECTION_NAME = args[2];
            SSH();
        }

    }
