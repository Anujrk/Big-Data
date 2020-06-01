import com.mongodb.*;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.collections4.map.LinkedMap;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.bson.Document;

import java.util.*;

public class KMeans {

    static String mongoDBURL;
    static String D;
    static String COLLECTION_NAME;
    static int k;
    static int maxEpochs;



    private static MongoClient getClient(String u) {
        MongoClient client = null;
        if (u.equals("None")) client = new MongoClient();
        else client = new MongoClient(new MongoClientURI(u));
        return client;
    }


    public static LinkedList<Double> randomCentroid(MongoClient client,MongoDatabase database,DB db,MongoCollection<Document> collection,DBCollection collectionin){
        BasicDBList ids = (BasicDBList) db.getCollection(COLLECTION_NAME).findOne().get("point");
        int list_size = ids.size();
        LinkedList<Double> tempcent = new LinkedList<>();
        for (int i = 0; i < list_size; i++) {
            Document data = new Document();
            data.append("_id", null);
            data.append("min", new Document("$min", new Document("$arrayElemAt", Arrays.asList("$point", i))));
            Document group = new Document("$group", data);
            AggregateIterable<Document> min = collection.aggregate(Arrays.asList(group));
            Double minimum = 0.0;
            for (Document doc : min) {
                minimum = (Double) doc.get("min");
            }
            Document dataM = new Document();
            dataM.append("_id", null);
            dataM.append("min", new Document("$max", new Document("$arrayElemAt", Arrays.asList("$point", i))));
            Document groupM = new Document("$group", dataM);
            AggregateIterable<Document> max = collection.aggregate(Arrays.asList(groupM));
            Double maximum = 0.0;
            for (Document doc : max) {
                maximum = (Double) doc.get("min");
            }
            double c_i1 = minimum + ((maximum - minimum) * Math.random());
            tempcent.add(c_i1);
        }
        return tempcent;
    }

    private static void Kmeans(MongoClient client,MongoDatabase database,DB db,MongoCollection<Document> collection,DBCollection collectionin) {
        LinkedList<LinkedList<Double>> centroid = new LinkedList<>();
        LinkedMap<ArrayList<Double>, Integer> point_id = new LinkedMap<>();
        List<Document> ids0 = collection.find().into(
                new ArrayList<Document>());
        for (Document id : ids0) {
            Integer id_r = (Integer) id.get("_id");
            ArrayList<Double> items = (ArrayList<Double>) id.get("point");
            point_id.put(items, null);
            collection.updateMany(new Document("_id", id_r),
                    new Document("$set", new Document("isCentroid", false)));
        }
        
        Double sse = 0.0;

        BasicDBList ids = (BasicDBList) db.getCollection(COLLECTION_NAME).findOne().get("point");
        int list_size = ids.size();

        for (int i0 = 1; i0 <= k; i0++) {
            centroid.add(randomCentroid(client,database,db,collection,collectionin));
        }

        for (int itter = 1; itter < maxEpochs; itter++) {
            LinkedList<LinkedList<Double>> prevcentroid = new LinkedList<>(centroid);
            sse = 0.0;
            for (ArrayList<Double> items : point_id.keySet()) {
                LinkedList<Double> distance = new LinkedList<>();
                for (LinkedList<Double> point : centroid) {
                    double eucleidDist = 0.0;
                    for (int i = 0; i < point.size(); i++) {
                        eucleidDist += Math.pow((point.get(i) - items.get(i)), 2);
                    }
                    distance.add(Math.sqrt(eucleidDist));
                }
                if((point_id.get(items) == null ) || (point_id.get(items)!= distance.indexOf(Collections.min(distance)) )) {
                    collection.updateMany(new Document("point", items).append("isCentroid", false),
                            new Document("$set", new Document("label", distance.indexOf(Collections.min(distance)))));
                }
                point_id.replace(items, distance.indexOf(Collections.min(distance)));
            }
            centroid.clear();
            for (int find = 0; find < k; find++) {
                LinkedList<List> for_newCentroid = new LinkedList<>();
                for (ArrayList<Double> keys : point_id.keySet()) {
                    if (point_id.get(keys) == find) {
                        for_newCentroid.add(keys);
                    }
                }
                if (for_newCentroid.size()==0) {
                    centroid.add(randomCentroid(client,database,db,collection,collectionin));
                }
                else{
                    LinkedList<Double> sseCent = prevcentroid.get(find);
                    LinkedList<Double> temps = new LinkedList<>();
                    int size = for_newCentroid.get(0).size();

                    DescriptiveStatistics[] c = new DescriptiveStatistics[size];
                    for (int j=0;j < size;j++){
                        for (int k =0;k<size;k++){
                            c[k] = new DescriptiveStatistics();
                        }
                    for (List<Double> temp : for_newCentroid) {
                        c[j].addValue(temp.get(j));
                        sse += Math.pow((sseCent.get(j) - (double) temp.get(j)), 2);
                    }
                    Double mean = c[j].getGeometricMean();
                    temps.add(mean);
                    }
                    centroid.add(temps);
                }
            }
            if (centroid.equals(prevcentroid)){
                System.out.println("Same");
                for (LinkedList<Double> point : centroid) {
            BasicDBObject N = new BasicDBObject();
            N.put("point", point);
            N.put("label", centroid.indexOf(point));
            N.put("isCentroid", true);
            if(centroid.indexOf(point)==0) {
                N.put("sse", sse);
            }
            collectionin.insert(N);
        }
                break;
            }
//            System.out.println(point_id.size());
            System.out.println(itter +"-"+sse + " : "+ centroid);
            if (itter==maxEpochs-1){
                for (LinkedList<Double> point : centroid) {
                    BasicDBObject N = new BasicDBObject();
                    N.put("point", point);
                    N.put("label", centroid.indexOf(point));
                    N.put("isCentroid", true);
                    if(centroid.indexOf(point)==0) {
                        N.put("sse", sse);
                    }
                    collectionin.insert(N);
                }
            }
        }

    }

    public static void main(String[] args) {
        mongoDBURL = args[0];
        D = args[1];
        COLLECTION_NAME = args[2];
        k = Integer.parseInt(args[3]);
        maxEpochs = Integer.parseInt(args[4]);
        MongoClient client = getClient(mongoDBURL);
        MongoDatabase database = client.getDatabase(D);
         DB db = client.getDB(D);
        MongoCollection<Document> collection = database
                .getCollection(COLLECTION_NAME);
        DBCollection collectionin = db.getCollection(COLLECTION_NAME);
        Kmeans(client,database,db,collection,collectionin);
    }
}
