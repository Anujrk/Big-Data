import java.sql.*;
import java.util.LinkedHashSet;


public class CreateSources {
    static String DB_URL;
    //  Database credentials
    static String USER;
    static String PASS;
    static Connection conn = null;

    public static void populateS1() throws ClassNotFoundException, SQLException {
        Class.forName("com.mysql.jdbc.Driver");
        conn = DriverManager.getConnection(DB_URL, USER, PASS);
        PreparedStatement s1 = conn.prepareStatement("INSERT IGNORE INTO ComedyMovie(id,title,year) \n" +
                "SELECT movie.id,title,releaseYear from movie \n" +
                "join hasgenre on hasgenre.movieId = movie.id \n" +
                "join genre on hasgenre.genreId = genre.id \n" +
                "where runtime >= 75 and genre.name = \"Comedy\";");
        s1.executeUpdate();
        s1.close();
        System.out.println("Inserted S1");
    }
    public static void populateS2() throws ClassNotFoundException, SQLException {
        Class.forName("com.mysql.jdbc.Driver");
        PreparedStatement s2 = conn.prepareStatement("INSERT IGNORE INTO ActionDramaMovie(id,title,year) \n" +
                "SELECT movie.id,title,releaseYear from movie \n" +
                "join hasgenre on hasgenre.movieId = movie.id \n" +
                "join genre on hasgenre.genreId = genre.id \n" +
                "where genre.name = \"Action\" and movieId in (SELECT movie.id from movie \n" +
                "join hasgenre on hasgenre.movieId = movie.id \n" +
                "join genre on hasgenre.genreId = genre.id \n" +
                "where genre.name = \"Drama\");");
        s2.executeUpdate();
        s2.close();
        System.out.println("Inserted S2");
    }

    public static void populateS3() throws ClassNotFoundException, SQLException {
        Class.forName("com.mysql.jdbc.Driver");
        PreparedStatement s3 = conn.prepareStatement("INSERT IGNORE INTO YoungActor(id,name,alive) " +
                "select id,name,alive from\n" +
                "(SELECT distinct id,name,\n" +
                "Case\n" +
                "when deathYear is  null then 1\n" +
                "else 0\n" +
                "end as alive\n" +
                "from person\n" +
                "where birthYear >= 1990)p\n" +
                "inner join\n" +
                "(select distinct personid from actedin) ai\n" +
                "on ai.personId =p.id ;");
        s3.executeUpdate();
        s3.close();
        System.out.println("Inserted S3");

    }
    public static void populateS4() throws ClassNotFoundException, SQLException {
        Class.forName("com.mysql.jdbc.Driver");
        PreparedStatement s4 = conn.prepareStatement("INSERT IGNORE INTO Acted(actor,movie) \n" +
                "select personId,movieId from YoungActor \n" +
                "join actedin on actedin.personId = YoungActor.id\n" +
                "join movie on actedin.movieId = movie.id\n" +
                "where movieId in (Select id from ComedyMovie) \n" +
                "or movieId in (Select id from ActionDramaMovie);");
        s4.executeUpdate();
        s4.close();
        System.out.println("Inserted S4");
        conn.close();
    }


    public static void main(String[] args) throws ClassNotFoundException, SQLException {
        Class.forName("com.mysql.jdbc.Driver");
            DB_URL = args[0];
            USER = args[1];
            PASS = args[2];

        // Open a connection
        relations.relation();
        populateS1();
        populateS2();
        populateS3();
        populateS4();

    }
}
