import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;

public class InitializePoints {
    static String DB_URL;
    //  Database credentials
    static String USER;
    static String PASS;
    static String SQL_QUERY;
    static String mongoDBURL;
    static String D;
    static String COLLECTION_NAME;

    static Connection conn = null;
    static PreparedStatement pstmt = null;

    private static MongoClient getClient(String u) {
        MongoClient client = null;
        if (u.equals("None")) client = new MongoClient();
        else client = new MongoClient(new MongoClientURI(u));
        return client;
    }

    public static void initialize() throws SQLException, ClassNotFoundException {
        MongoClient client = getClient(mongoDBURL);
        DB db = client.getDB(D);

        Class.forName("com.mysql.jdbc.Driver");
        DBCollection collection = db.getCollection(COLLECTION_NAME);
        conn = DriverManager.getConnection(DB_URL, USER, PASS);
        PreparedStatement s1= conn.prepareStatement(SQL_QUERY);
        s1.setFetchSize(100);
        ResultSet rs = s1.executeQuery();
        while(rs.next()){
            BasicDBObject N = new BasicDBObject();
            N.put("_id",rs.getInt(1));
            double[] dims = new double[3];
            dims[0] = (rs.getDouble(2));
            dims[1] = (rs.getDouble(3));
            dims[2] = (rs.getDouble(4));

            System.out.println(Arrays.toString(dims));
            N.put("point",dims);
            System.out.println(N);
            collection.insert(N);
        }
        rs.close();
    }

    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        DB_URL = args[0];
        USER = args[1];
        PASS = args[2];
        SQL_QUERY = args[3];
        mongoDBURL = args[4];
        D = args[5];
        COLLECTION_NAME = args[6];
        initialize();

    }
}
