import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.mongodb.*;
import java.sql.*;
import java.sql.Connection;

public class InitializeTransactions {
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

    public static void initialize() throws ClassNotFoundException, SQLException {
        Multimap<Integer, Integer> items = ArrayListMultimap.create();
        MongoClient client = getClient(mongoDBURL);
        DB db = client.getDB(D);
        Class.forName("com.mysql.jdbc.Driver");
        DBCollection collection = db.getCollection(COLLECTION_NAME);
        conn = DriverManager.getConnection(DB_URL, USER, PASS);
        PreparedStatement s1= conn.prepareStatement(SQL_QUERY);
        s1.setFetchSize(100);
        ResultSet rs = s1.executeQuery();
        while (rs.next()){
            int tid = rs.getInt(1);
            int iid = rs.getInt(2);
            items.put(tid,iid);

        }
        rs.close();
        conn.close();
        s1.close();
        for (int key:items.keySet()){
            System.out.println(key + " : "+ items.get(key));
            BasicDBObject N = new BasicDBObject();
            N.put("_id",key);
            N.put("items",items.get(key));
            collection.insert(N);
        }
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
