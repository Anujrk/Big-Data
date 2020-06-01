import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import javax.net.ssl.SSLException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.SocketTimeoutException;
import java.sql.*;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;


public class FillFromWikidata {
    static String DB_URL;
    //  Database credentials
    static String USER;
    static String PASS;
    static String F;
    static String MO_URL;
    static String D;

    static Connection conn = null;
    static PreparedStatement pstmt = null;

    private static MongoClient getClient(String u) {
        MongoClient client = null;
        if (u.equals("None")) client = new MongoClient();
        else client = new MongoClient(new MongoClientURI(u));
        return client;
    }


    public static void update_YAdata() throws ClassNotFoundException, SQLException {

        MongoClient client = getClient(MO_URL);
        DB db = client.getDB(D);
        Class.forName("com.mysql.jdbc.Driver");
        JSONParser parser = new JSONParser();
        LinkedHashMap<String, Integer> id_link = new LinkedHashMap<>();
        LinkedHashSet<Integer> alive_ids = new LinkedHashSet<>();

        try {
            //Parsing contents of Json File
            String CurentLine;
            PreparedStatement s1 = conn.prepareStatement("Select id from YoungActor where alive = 0;");
            ResultSet rs = s1.executeQuery();
            while (rs.next()) {
                alive_ids.add(rs.getInt(1));
            }
            rs.close();
            s1.close();
            BufferedReader br = new BufferedReader(new FileReader(F));
            String sql = "UPDATE YoungActor SET deathcause=? WHERE id=?;";
            pstmt = conn.prepareStatement(sql);
            while ((CurentLine = br.readLine()) != null) {
                Object obj;
                try {
                    obj = parser.parse(CurentLine);
                    JSONObject jsonObject = (JSONObject) obj;
                    String actor_id = (String) jsonObject.get("id");
                    String actorLink = (String) jsonObject.get("actor");
                    String cause = (String) jsonObject.get("cause");

                    if (actor_id != null) {
                        if (actor_id.contains("/")) {
                            actor_id = actor_id.substring(0, actor_id.indexOf("/"));
                        }
                        if (actor_id.contains("https")) {
                            actor_id = "tt8465094";
                        }
                        int id = Integer.parseInt(actor_id.substring(2));

                        if (alive_ids.contains(id)) {
                            id_link.put(actorLink, id);
                        }
                    }
                    if (jsonObject.containsKey("cause")) {
                        if (id_link.containsKey(actorLink)) {
                            int id2 = id_link.get(actorLink);
                            Document causes = null;
                            try{
                            causes = Jsoup.connect(cause).get();}
                            catch (SocketTimeoutException | SSLException error){
                                try {
                                    causes = Jsoup.connect(cause).get();
                                }
                                catch (SocketTimeoutException | SSLException error2){
                                    causes = Jsoup.connect(cause).get();
                                }
                            }
                            String causeM = causes.title();
                            if (causeM.contains("-")) {
                                causeM = causeM.substring(0, causes.title().indexOf("-"));
                            }
                            pstmt.setString(1, causeM);
                            pstmt.setInt(2, id2);
//                            System.out.println(id2 + " : " + causeM);
                            pstmt.executeUpdate();
                        }
                    }

                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
            br.close();

        } catch (IOException | SQLException e) {
            e.printStackTrace();
        }
        pstmt.close();

    }

    public static void update_ADMdata() throws ClassNotFoundException, SQLException {
        Class.forName("com.mysql.jdbc.Driver");
        JSONParser parser = new JSONParser();
        int index = 0;
//        conn.setAutoCommit(false);
        LinkedHashMap<String, Integer> id_link = new LinkedHashMap<>();
        LinkedHashSet<Integer> ADM_ids =new LinkedHashSet<>();
        LinkedHashMap<String,String> colorMatch = new LinkedHashMap<>();
        try {
            //Parsing contents of Json File
            String CurentLine;
            PreparedStatement s1 = conn.prepareStatement("Select id from ActionDramaMovie;");
            ResultSet rs = s1.executeQuery();
            while (rs.next()) {
                    ADM_ids.add(rs.getInt(1));
            }
            s1.close();
            rs.close();
            BufferedReader br = new BufferedReader(new FileReader(F));
            String sql = "UPDATE ActionDramaMovie SET color=? WHERE id=?;";
            pstmt = conn.prepareStatement(sql);
            while ((CurentLine = br.readLine()) != null) {
                Object obj;
                try {
                    obj = parser.parse(CurentLine);
                    JSONObject jsonObject = (JSONObject) obj;
                    String movie_id = (String) jsonObject.get("id");
                    String movieLink = (String) jsonObject.get("movie");
                    String color = (String) jsonObject.get("color");

                    if (jsonObject.containsKey("movie") && jsonObject.containsKey("id")) {
                        if (movie_id != null)  {
                            if (movie_id.contains("/")) {
                                movie_id = movie_id.substring(0, movie_id.indexOf("/"));
                            }
                            if (movie_id.contains("https")) {
                                movie_id = "tt8465094";
                            }
                            int id = Integer.parseInt(movie_id.substring(2));
                            if(ADM_ids.contains(id)) {
                                id_link.put(movieLink, id);
                            }
                        }
                    }
                    if (jsonObject.containsKey("color")) {
                        if (id_link.containsKey(movieLink)) {
                            int id2 = id_link.get(movieLink);
                            if (!colorMatch.containsKey(color)){
                                Document colors = null;
                                try{
                                colors = Jsoup.connect(color).get();}
                                catch (SocketTimeoutException | SSLException error){
                                    try {
                                        colors = Jsoup.connect(color).get();
                                    }
                                    catch (SocketTimeoutException | SSLException error2){
                                        colors = Jsoup.connect(color).get();
                                    }
                                }
                                String colorM = colors.title();
                                if (colorM.contains("-")) {
                                    colorM = colorM.substring(0, colors.title().indexOf("- W"));
                                }
                                colorMatch.put(color,colorM);
                            }
                            if (id2!=3735246) {
                                pstmt.setString(1, colorMatch.get(color));
                                pstmt.setInt(2, id2);
                                pstmt.executeUpdate();
                                index++;
                            }
                        }
                    }


                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
            br.close();

        } catch (IOException | SQLException e) {
            e.printStackTrace();
        }
        pstmt.close();

    }
    public static void update_CMdata() throws ClassNotFoundException, SQLException {
        Class.forName("com.mysql.jdbc.Driver");
        JSONParser parser = new JSONParser();
        int index = 0;
//        conn.setAutoCommit(false);
        LinkedHashMap<String, Integer> id_link = new LinkedHashMap<>();
        LinkedHashSet<Integer> CM_ids =new LinkedHashSet<>();
        LinkedHashMap<String,String> unitMatch = new LinkedHashMap<>();

        try {
            //Parsing contents of Json File
            String CurentLine;
            PreparedStatement s1 = conn.prepareStatement("Select id from ComedyMovie;");
            ResultSet rs = s1.executeQuery();
            while (rs.next()) {
                CM_ids.add(rs.getInt(1));
            }
            rs.close();
            s1.close();
            BufferedReader br = new BufferedReader(new FileReader(F));
            String sql = "UPDATE ComedyMovie SET boxoffice=? WHERE id=?;";
            pstmt = conn.prepareStatement(sql);
            while ((CurentLine = br.readLine()) != null) {
                Object obj;
                try {
                    obj = parser.parse(CurentLine);
                    JSONObject jsonObject = (JSONObject) obj;
                    String movie_id = (String) jsonObject.get("id");
                    String movieLink = (String) jsonObject.get("movie");
                    String boxoffice = (String) jsonObject.get("boxoffice");
                    String boxofficeunit = (String) jsonObject.get("boxofficeUnit");

                    if (jsonObject.containsKey("movie") && jsonObject.containsKey("id")) {
                        if (movie_id != null) {
                            if (movie_id.contains("/")) {
                                movie_id = movie_id.substring(0, movie_id.indexOf("/"));
                            }
                            if (movie_id.contains("https")) {
                                movie_id = "tt8465094";
                            }
                            int id = Integer.parseInt(movie_id.substring(2));
                            if(CM_ids.contains(id)) {
                                id_link.put(movieLink, id);
                            }
                        }
                    }
                    if (jsonObject.containsKey("boxoffice")) {
                        if (id_link.containsKey(movieLink)) {
                            int id2 = id_link.get(movieLink);
                            id_link.remove(movieLink);
                            if (!unitMatch.containsKey(boxofficeunit)){
                                Document unit = null;
                                try {
                                    unit = Jsoup.connect(boxofficeunit).timeout(2000).get();
                                }
                                catch (SocketTimeoutException | SSLException error){
                                    try {
                                        System.out.println("Entered Try of catch");
                                        unit = Jsoup.connect(boxofficeunit).timeout(3000).get();
                                    }
                                    catch (SocketTimeoutException | SSLException error2){
                                        System.out.println("Entered catch of catch");
                                        unit = Jsoup.connect(boxofficeunit).get();
                                    }
                                }
                                String unitM = unit.title();
                                StringBuilder sb = new StringBuilder();
                                if (unitM.contains("-")) {
                                    unitM = unitM.substring(0, unit.title().indexOf("- W"));
                                }

                                if (unitM.contains("Australian")){
                                    sb = null;
                                }
                                else if (unitM.contains("dollar")){
                                    sb.append("$");         // ==   495
                                    sb.append(boxoffice);
                                }
                                else if (unitM.contains("ruble")){
                                    sb.append("₽");         //       == 15
                                    sb.append(boxoffice);
                                }
                                else if (unitM.contains("rupee")){
                                    sb.append("₹");              // == 4
                                    sb.append(boxoffice);
                                }
                                else if (unitM.contains("koruna")){
                                    sb.append("Kč");            // == 1
                                    sb.append(boxoffice);
                                }
                                else if (unitM.contains("pound")){
                                    sb.append("£");             // == 4
                                    sb.append(boxoffice);
                                }
                                else if (unitM.contains("euro")){
                                    sb.append("€");             // == 10
                                    sb.append(boxoffice);
                                }
                                else if (unitM.contains("Italian")){
//                                    sb.append("₺");             // == 1
//                                    sb.append(boxoffice);
//                                    System.out.println(unitM + id2);
                                    sb = null;
                                }
                                else if (unitM.contains("peso")){
                                    sb.append("₱");             // == 2
                                    sb.append(boxoffice);
                                }
                                else if (unitM.contains("baht")){
                                    sb.append("฿");             // == 1
                                    sb.append(boxoffice);
                                }
//                                else if(id2 == 50777 || id2 == 325537 || id2 == 466399){
//                                    sb = null;
//                                    System.out.println(id2 + unitM);
//                                }
                                else {
                                    sb = null;
                                }

                                if (sb!=null) {
                                    pstmt.setString(1, String.valueOf(sb));
                                    pstmt.setInt(2, id2);
                                    pstmt.executeUpdate();
                                    index++;
                                    System.out.println(index + " : " + id2 + " : " + sb + " // " + unitM);
                                }
                            }

                        }
                    }
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
            br.close();

        } catch (IOException | SQLException e) {
            e.printStackTrace();
        }
        pstmt.close();

    }


    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Class.forName("com.mysql.jdbc.Driver");
        F = args[0];
        DB_URL = args[1];
        USER = args[2];
        PASS = args[3];
        MO_URL = args[4];
        D = args[5];

        // Open a connection
        conn = DriverManager.getConnection(DB_URL, USER, PASS);
        update_CMdata();
        update_ADMdata();
        update_YAdata();


    }
}
