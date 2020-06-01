import java.sql.*;

public class GenerateMappings {
    static String DB_URL;

    //  Database credentials
    static String USER;
    static String PASS;

    public static void Movie_G () {
        Connection conn;
        Statement stmt;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            // Open a connection
            System.out.println("Establishing connection to Database");
            conn = DriverManager.getConnection(DB_URL, USER, PASS);
            // Execute a query
            System.out.println("Creating Tables ");
//            stmt = conn.createStatement();
//            //Create Movie_G Table
//            String sql = "CREATE TABLE Movie_G" +
//                    "(id INT ," +
//                    "title VARCHAR(100)," +
//                    "year INT ," +
//                    "genre VARCHAR(100),"+
//                    "boxoffice VARCHAR(100) ," +
//                    "color VARCHAR(100))";
//            //Update the table
//            stmt.executeUpdate(sql);
//            stmt.close();
//            System.out.println("Created Movie_G table");
            PreparedStatement s1 = conn.prepareStatement("create view Movie_G as\n" +
                    "Select distinct id,title,year,\"Comedy\" as genre,boxoffice,null as color from ComedyMovie \n" +
                    "union all\n" +
                    "select distinct id,title,year,\"ActionDrama\" as genre,null as boxoffice,color from ActionDramaMovie;");
            s1.executeUpdate();
            s1.close();
            conn.close();
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    public static void Actor_G () {
        Connection conn;
        Statement stmt;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            // Open a connection
            System.out.println("Establishing connection to Database");
            conn = DriverManager.getConnection(DB_URL, USER, PASS);
            // Execute a query
            System.out.println("Creating ACtor_G Tables ");
//            stmt = conn.createStatement();
//            //Create Actor_G Table
//            String sql = "CREATE TABLE Actor_G" +
//                    "(id INT ," +
//                    "name VARCHAR(100)," +
//                    "alive TINYINT ,"+
//                    "deathcause VARCHAR(100),"+
//                    "PRIMARY KEY(id))";
//            //Update the table
//            stmt.executeUpdate(sql);
//            stmt.close();
//            System.out.println("Created Actor_G table");
            PreparedStatement s1 = conn.prepareStatement("Create View Actor_G as\n" +
                    "Select * from YoungActor;");
            s1.executeUpdate();
            s1.close();
            conn.close();
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    public static void Movie_Actor_G () {
        Connection conn;
        Statement stmt;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            // Open a connection
            System.out.println("Establishing connection to Database");
            conn = DriverManager.getConnection(DB_URL, USER, PASS);
            // Execute a query
            System.out.println("Creating Tables ");
//            stmt = conn.createStatement();
//            //Create Movie_Actor_G Table
//            String sql = "CREATE TABLE Movie_Actor_G" +
//                    "(actor INT," +
//                    "movie INT," +
//                    "PRIMARY KEY(actor,movie))";
//            //Update the table
//            stmt.executeUpdate(sql);
//            stmt.close();
//            System.out.println("Created Movie_Actor_G table");
            PreparedStatement s1 = conn.prepareStatement("create View Movie_Actor_G as\n" +
                    "Select * from Acted;");
            s1.executeUpdate();
            s1.close();
            conn.close();
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

        public static void main(String[] args) throws ClassNotFoundException {
        Class.forName("com.mysql.jdbc.Driver");
        DB_URL = args[0];
        USER = args[1];
        PASS = args[2];

        // Open a connection
        Movie_G();
        Actor_G();
        Movie_Actor_G();

    }
}
