import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.collections4.CollectionUtils;
import org.bson.Document;

import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

public class ComputeMutualInformation {
    static String mongoDBURL;
    static String D;
    static String COLLECTION_NAME;
    static String File_Name;

    private static MongoClient getClient(String u) {
        MongoClient client = null;
        if (u.equals("None")) client = new MongoClient();
        else client = new MongoClient(new MongoClientURI(u));
        return client;
    }

    private static void MI() throws IOException {
        MongoClient client = getClient(mongoDBURL);
        MongoDatabase database = client.getDatabase(D);
        DB db = client.getDB(D);
        MongoCollection<Document> collection = database
                .getCollection(COLLECTION_NAME);
        DBCollection collectionin = db.getCollection(COLLECTION_NAME);
        List<Document> ids = collection.find().into(
                new ArrayList<Document>());
        Multimap<Integer, ArrayList<Double>> U_label = ArrayListMultimap.create();
        Multimap<Integer, ArrayList<Double>> V_Expected = ArrayListMultimap.create();

        for (Document id : ids) {
            if (!(Boolean) id.get("isCentroid")) {
                ArrayList<Double> items = (ArrayList<Double>) id.get("point");
                U_label.put((Integer) id.get("label"), items);
                V_Expected.put((Integer) id.get("expected"), items);
            }
        }

        double Hu = 0;
        for (Integer key : U_label.keySet()){
            double U =(double) U_label.get(key).size()/U_label.values().size();
            Hu -= (U * Math.log(U));
        }
        System.out.println("Hu :"+ Hu);
        double Hv = 0;
        for (Integer key : V_Expected.keySet()){
            double V =(double) V_Expected.get(key).size()/V_Expected.values().size();
            Hv -= (V * Math.log(V));
        }
        System.out.println("Hv :"+Hv);

        double MI = 0;
       for (Integer U_key : U_label.keySet()){
           for (Integer V_key: V_Expected.keySet()){
               Collection<ArrayList<Double>> union = CollectionUtils.intersection(U_label.get(U_key),V_Expected.get(V_key));
               if (union.size()!=0){
                    MI +=((double)union.size()/U_label.values().size())
                            * Math.log(((double)union.size()/U_label.values().size())
                            /(((double) U_label.get(U_key).size()/U_label.values().size())
                            *((double) V_Expected.get(V_key).size()/V_Expected.values().size())));
               }
           }
       }
        System.out.println("MI :"+ MI);
       try {
           FileWriter file = new FileWriter(File_Name);
           BufferedWriter writer = new BufferedWriter(file);
           writer.write((String.valueOf(Hu)));
           writer.newLine();
           writer.write((String.valueOf(Hv)));
           writer.newLine();
           writer.write((String.valueOf(MI)));
           writer.flush();
       } catch (IOException e) {
           System.out.println("File not found");
       }
    }













/*        BufferedReader reader = new BufferedReader(new FileReader("/Users/anuj/Downloads/1/Kmeans/src/main/java/clustering/expected_0_275.txt"));
        String line = reader.readLine();
        while (line!=null){
            String[] id = line.split(",");
            int ID = Integer.parseInt(id[0]);
            int label = Integer.parseInt(id[1]);
            collection.updateOne(new Document("_id",ID),
                    new Document("$set", new Document("expected", label)));
            line = reader.readLine();
        }
    */

    public static void main(String[] args) throws IOException {
        mongoDBURL = args[0];
        D = args[1];
        COLLECTION_NAME = args[2];
        File_Name = args[3];
        MI();
    }
}
