import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.mongodb.*;
import java.sql.*;
import java.util.Collections;

public class IMDBSQLToMongo {

    static String MS_URL ;

    static String U ;
    static String P ;
    static String MO_URL ;
    static String D;

    static Integer[] forNull = {null};

    private static MongoClient getClient(String u) {
        MongoClient client = null;
        if (u.equals("None"))client = new MongoClient();
        else client = new MongoClient(new MongoClientURI(u));
        return client;
    }

    public static void mongoDbMovie() throws ClassNotFoundException, SQLException {
        Connection con = null;
        MongoClient client = getClient(MO_URL);
        DB db = client.getDB(D);
        DBCollection collection = db.getCollection("Movies");
        con = DriverManager.getConnection(MS_URL, U, P);
        Class.forName("com.mysql.jdbc.Driver");


        PreparedStatement p2 = con.prepareStatement("Select movie.id,genre.name from movie join hasgenre on " +
                "hasgenre.movieId = movie.id join genre on hasgenre.genreId = genre.id order by movie.id;");
        p2.setFetchSize(1000);
        ResultSet rs2 = p2.executeQuery();
        Multimap<Integer,String> genreMap = ArrayListMultimap.create();
        while (rs2.next()){
            genreMap.put(rs2.getInt(1),rs2.getString(2));
        }
        PreparedStatement p1 = con.prepareStatement("Select * from Movie");
        p1.setFetchSize(1000);
        ResultSet rs = p1.executeQuery();
        while(rs.next()){
            BasicDBObject Movies = new BasicDBObject();
            int id = rs.getInt(1);
            String title = rs.getString(3);
            int releaseYear = rs.getInt(4);
            releaseYear= rs.wasNull() ? -1 : releaseYear;
            int runtime =rs.getInt(5);
            runtime = rs.wasNull() ? -1 : runtime;
            float rating =rs.getFloat(6);
            rating = rs.wasNull() ? -1 : rating;
             int numberOfVotes = rs.getInt(7);
            numberOfVotes = rs.wasNull() ? -1 : numberOfVotes;
            Movies.put("_id",id);
            Movies.put("title",title);
            if(releaseYear!=-1){                 //HAve put zero for now ...has too be corrected to null format
            Movies.put("releaseYear",releaseYear);}
            if(runtime!=-1){
            Movies.put("runtime",runtime);}
            if(rating!=-1){
            Movies.put("rating",rating);}
            if(numberOfVotes!=-1){
            Movies.put("numberOfVotes",numberOfVotes);}
            if(genreMap.containsKey(id)){
            Movies.put("genres",genreMap.get(id));}
            else{
                Movies.put("genres",Collections.EMPTY_LIST);
                }
            collection.insert(Movies);
        }
        rs.close();
        con.close();
    }

    public static void mongoDBPerson() throws ClassNotFoundException, SQLException {
        Connection con;
        MongoClient client = getClient(MO_URL);
        DB db = client.getDB("imdb_ibd");
        DBCollection collection = db.getCollection("People");
        con = DriverManager.getConnection(MS_URL, U, P);
        Class.forName("com.mysql.jdbc.Driver");


        PreparedStatement p2 = con.prepareStatement("Select * from actedin");
        PreparedStatement p3 = con.prepareStatement("Select * from composedby");
        PreparedStatement p4 = con.prepareStatement("Select * from directedby");
        PreparedStatement p5 = con.prepareStatement("Select * from editedby");
        PreparedStatement p6 = con.prepareStatement("Select * from producedby");
        PreparedStatement p7 = con.prepareStatement("Select * from writtenby");

        PreparedStatement p1 = con.prepareStatement("Select * from Person;");
        p1.setFetchSize(1000);
        ResultSet rs = p1.executeQuery();
        while(rs.next()) {
            BasicDBObject Person = new BasicDBObject();
            int id = rs.getInt(1);
            String name = rs.getString(3);
            int birthYear = rs.getInt(4);
            birthYear = rs.wasNull() ? -1 : birthYear;
            int deathYear = rs.getInt(5);
            deathYear = rs.wasNull() ? -1 : deathYear;
            Person.put("_id", id);
            Person.put("name", name);
            if (birthYear != -1) {
                Person.put("birthYear", birthYear);
            }
            if (deathYear != -1) {
                Person.put("deathYear", deathYear);
            }
            Person.put("actor", Collections.EMPTY_LIST);
            Person.put("composer", Collections.EMPTY_LIST);
            Person.put("director", Collections.EMPTY_LIST);
            Person.put("editor", Collections.EMPTY_LIST);
            Person.put("producer", Collections.EMPTY_LIST);
            Person.put("writer", Collections.EMPTY_LIST);
            collection.insert(Person);
        }
        rs.close();
        p3.setFetchSize(1000);
        ResultSet rs3 = p3.executeQuery(); //For composer
        Multimap<Integer,Integer> actor = ArrayListMultimap.create();
        while(rs3.next()){
            actor.put(rs3.getInt(1),rs3.getInt(2));
        }rs3.close();
        for(Integer key:actor.keySet()) {
            BasicDBObject searchQuery = new BasicDBObject().append("_id", key);
            if(actor.get(key)!=null){
            BasicDBObject updateDocument = new BasicDBObject("composer",actor.get(key));
                BasicDBObject updateOperationDocument = new BasicDBObject("$set", updateDocument);
                collection.update(searchQuery,updateOperationDocument);
            }
            else{
                BasicDBObject updateDocument = new BasicDBObject("composer",forNull);
                BasicDBObject updateOperationDocument = new BasicDBObject("$set", updateDocument);
                collection.update(searchQuery,updateOperationDocument);
            }
        }
        actor = ArrayListMultimap.create();
        p4.setFetchSize(1000);
        ResultSet rs4 = p4.executeQuery(); //For composer
        while(rs4.next()){
            actor.put(rs4.getInt(1),rs4.getInt(2));
        }rs4.close();
        for(Integer key:actor.keySet()) {
            BasicDBObject searchQuery = new BasicDBObject().append("_id", key);
            if(actor.get(key)!=null){
                BasicDBObject updateDocument = new BasicDBObject("director",actor.get(key));
                BasicDBObject updateOperationDocument = new BasicDBObject("$set", updateDocument);
                collection.update(searchQuery,updateOperationDocument);
            }
            else{
                BasicDBObject updateDocument = new BasicDBObject("director",forNull);
                BasicDBObject updateOperationDocument = new BasicDBObject("$set", updateDocument);
                collection.update(searchQuery,updateOperationDocument);
            }
        }
        actor = ArrayListMultimap.create();
        p2.setFetchSize(1000);
        ResultSet rs2 = p2.executeQuery(); //For actor
        while(rs2.next()){
            actor.put(rs2.getInt(1),rs2.getInt(2));
        }rs2.close();
        for(Integer key:actor.keySet()) {
            BasicDBObject searchQuery = new BasicDBObject("_id", key);
            if(actor.get(key)!=null){
                BasicDBObject updateDocument = new BasicDBObject("actor",actor.get(key));
                BasicDBObject updateOperationDocument = new BasicDBObject("$set", updateDocument);
                collection.update(searchQuery,updateOperationDocument);
            }
            else{
                BasicDBObject updateDocument = new BasicDBObject("actor",forNull);
                BasicDBObject updateOperationDocument = new BasicDBObject("$set", updateDocument);
                collection.update(searchQuery,updateOperationDocument);

            }
        }
        actor = ArrayListMultimap.create();
        p5.setFetchSize(1000);
        ResultSet rs5 = p5.executeQuery(); //For composer
        while(rs5.next()){
            actor.put(rs5.getInt(1),rs5.getInt(2));
        }rs5.close();
        for(Integer key:actor.keySet()) {
            BasicDBObject searchQuery = new BasicDBObject().append("_id", key);
            if(actor.get(key)!=null){
                BasicDBObject updateDocument = new BasicDBObject("editor",actor.get(key));
                BasicDBObject updateOperationDocument = new BasicDBObject("$set", updateDocument);
                collection.update(searchQuery,updateOperationDocument);
            }
            else{
                BasicDBObject updateDocument = new BasicDBObject("editor",forNull);
                BasicDBObject updateOperationDocument = new BasicDBObject("$set", updateDocument);
                collection.update(searchQuery,updateOperationDocument);
            }
        }
        actor = ArrayListMultimap.create();
        p6.setFetchSize(1000);
        ResultSet rs6 = p6.executeQuery(); //For composer
        while(rs6.next()){
            actor.put(rs6.getInt(1),rs6.getInt(2));
        }rs6.close();
        for(Integer key:actor.keySet()) {
            BasicDBObject searchQuery = new BasicDBObject().append("_id", key);
            if(actor.get(key)!=null){
                BasicDBObject updateDocument = new BasicDBObject("producer",actor.get(key));
                BasicDBObject updateOperationDocument = new BasicDBObject("$set", updateDocument);
                collection.update(searchQuery,updateOperationDocument);
            }
            else{
                BasicDBObject updateDocument = new BasicDBObject("producer",forNull);
                BasicDBObject updateOperationDocument = new BasicDBObject("$set", updateDocument);
                collection.update(searchQuery,updateOperationDocument);
            }
        }
        actor = ArrayListMultimap.create();
        p7.setFetchSize(1000);
        ResultSet rs7 = p7.executeQuery(); //For composer
        while(rs7.next()){
            actor.put(rs7.getInt(1),rs7.getInt(2));
        }rs7.close();
        for(Integer key:actor.keySet()) {
            BasicDBObject searchQuery = new BasicDBObject().append("_id", key);
            if(actor.get(key)!=null){
                BasicDBObject updateDocument = new BasicDBObject("writer",actor.get(key));
                BasicDBObject updateOperationDocument = new BasicDBObject("$set", updateDocument);
                collection.update(searchQuery,updateOperationDocument);
            }
            else{
                BasicDBObject updateDocument = new BasicDBObject("writer",forNull);
                BasicDBObject updateOperationDocument = new BasicDBObject("$set", updateDocument);
                collection.update(searchQuery,updateOperationDocument);
            }
        }
        con.close();
    }


    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        MS_URL = args[0];
        U = args[1];
        P = args[2];
        MO_URL = args[3];
        D = args[4];
        mongoDbMovie();
        mongoDBPerson();
    }
}

