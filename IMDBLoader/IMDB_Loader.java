package IMDBLoader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Scanner;
import java.util.zip.GZIPInputStream;

jdbc:mysql://localhost/imdb_ibd root Blitzcreek1 jennifer
public class IMDB_Loader {
    // JDBC driver name and database URL

    //  static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    static String DB_URL;
    //  "jdbc:mysql://localhost/IMDBLoader";
    //  static final String DB_URL_Batch = "jdbc:mysql://localhost/IMDBLoader?rewriteBatchStatements=true";

    //  Database credentials
    static String USER;
    //= "root";
    static String PASS;
    //= "Blitzcreek1";
    static String F;
    //  "/Users/anuj/Downloads/IBD/";
    static Connection conn = null;
    static PreparedStatement pstmt = null;

    //arraylist for movies
    static ArrayList<String> movie_list = new ArrayList<>();


    private static void genre_populate() throws ClassNotFoundException, SQLException, IOException {
        // Register JDBC driver
        Class.forName("com.mysql.jdbc.Driver");
//
//        // Open a connection
//        conn = DriverManager.getConnection(DB_URL, USER, PASS);

        //Creating HashMaps
        LinkedHashMap<String, Integer> genres = new LinkedHashMap<>();

        conn.setAutoCommit(false);
        PreparedStatement HGenre_pstmt = null;
        PreparedStatement Movie_pstmt = null;

        //Initializing variable
        int index = 0; // for skipping first line
        int counterId = 1; //counterid for hashmap of genres
        //Reading data from file
        InputStream gzipStream = new GZIPInputStream(new FileInputStream(F + "title.basics.tsv.gz"));
        Scanner sc = new Scanner(gzipStream, "UTF-8");
        String sql = "INSERT IGNORE INTO movie values(?,?,?,?,?,?)";
        Movie_pstmt = conn.prepareStatement(sql);
        String sql5 = "INSERT IGNORE INTO hasgenre(movieId,genreId) SELECT movie.id,genre.id FROM movie,genre WHERE" +
                " movie.id=? AND genre.id=?";
        HGenre_pstmt = conn.prepareStatement(sql5);
        String sql3 = "INSERT IGNORE INTO genre values(?,?)";
        pstmt = conn.prepareStatement(sql3);
        int counter = 1;
        int g_counter = 0;
        int m_counter = 1;


        while (sc.hasNextLine()) {
            String line = sc.nextLine();
            if (index == 0) {
                index++;
                continue;
            }
            String[] genre = line.split("\t");
            String titleType = genre[1];
            if (titleType.matches("(.*)short(.*)") || titleType.matches("(.*)movie(.*)")
                    || titleType.matches("(.*)tvShort(.*)") || titleType.matches("(.*)tvMovie(.*)")) {
                movie_list.add(genre[0]);
                long movieId = Long.parseLong(genre[0].substring(2));
                String[] temp_genre = genre[8].split(",");

                for (String temp : temp_genre) {
                    if (!genres.keySet().contains(temp)) {
                        g_counter++;
                        genres.put(temp, counterId);
                        counterId++;
                        pstmt.setInt(1, counter);
                        if (!temp.contains("\\N")) {
                            pstmt.setString(2, temp);
                        } else {
                            pstmt.setNull(2, Types.INTEGER);
                        }
                        pstmt.executeUpdate();
                        System.out.println(counter + " inserting Genre Data " + temp);
                        counter++;
                    }
                }

                //Converting runtime from String to int
                Integer runTime = null;
                try {
                    runTime = Integer.parseInt(genre[7]);
                } catch (Exception e) {
                    runTime = null;
                }

                //Converting releaseyear from String to int
                Integer endyr = null;
                Integer startyr;
                try {
                    endyr = Integer.parseInt(genre[6]);
                } catch (Exception e) {
                    endyr = null;
                }
                try {
                    startyr = Integer.parseInt(genre[5]);
                } catch (Exception e) {
                    startyr = null;
                }

                Integer releaseYr;
                if (endyr == null) {
                    releaseYr = startyr;
                } else {
                    releaseYr = endyr;
                }

                //   Insert into Movies Table
                Movie_pstmt.setInt(1, (int) movieId);
                Movie_pstmt.setString(2, genre[3]);
                if (releaseYr != null) {
                    Movie_pstmt.setInt(3, releaseYr);
                } else {
                    Movie_pstmt.setNull(3, Types.INTEGER);
                }
                if (runTime != null) {
                    Movie_pstmt.setInt(4, runTime);
                } else {
                    Movie_pstmt.setNull(4, Types.INTEGER);
                }
                Movie_pstmt.setNull(5, Types.INTEGER);
                Movie_pstmt.setNull(6, Types.INTEGER);
                Movie_pstmt.addBatch();

                //  Insert into HasGenre Table
                for (String value : temp_genre) {
                    if (genres.get(value) != null) {
                        HGenre_pstmt.setInt(1, (int) movieId);
                        HGenre_pstmt.setInt(2, genres.get(value));
                        HGenre_pstmt.addBatch();
                    }
                }

                //BatchSize
//                if (g_counter % 4 == 0) {
//                    pstmt.executeBatch();
//                    System.out.println(g_counter + " Inserting into Genre table");
//                    g_counter++;
//                }
                if (index % 10000 == 0) {
                    Movie_pstmt.executeBatch();
                    m_counter++;
                    System.out.println(index + " Inserting into Movie table");
                }
                if (m_counter % 10 == 0) {
                    HGenre_pstmt.executeBatch();
                    System.out.println(m_counter + " Inserting into HasGenre Table");
                    m_counter++;
                }
                conn.commit();
                index++;
            }
        }

        //Adding remaining data
        Movie_pstmt.executeBatch();
        HGenre_pstmt.executeBatch();
        pstmt.executeBatch();
        conn.commit();
        System.out.println("all Genre,Movie and HasGenre Data Inserted");
        sc.close();
        Movie_pstmt.close();
        HGenre_pstmt.close();
        pstmt.close();

    }


    private static void person_populate() throws ClassNotFoundException, SQLException, IOException {

        // Register JDBC driver
       Class.forName("com.mysql.jdbc.Driver");
//
//        // Open a connection
//      conn = DriverManager.getConnection(DB_URL, USER, PASS);
        conn.setAutoCommit(false);


        //Initializing variable
        int index = 0; // for skipping first line

        InputStream gzipStream = new GZIPInputStream(new FileInputStream(F + "name.basics.tsv.gz"));
        Scanner sc = new Scanner(gzipStream, "UTF-8");

        //Creating statement
        String sql = "INSERT IGNORE INTO person values(?,?,?,?)";
        pstmt = conn.prepareStatement(sql);

        while (sc.hasNextLine()) {
            String line = sc.nextLine();
            if (index == 0) {
                index++;
                continue;
            }
            String[] person = line.split("\t");
            String nameId = person[0].substring(2);
            int personId = Integer.parseInt(nameId);


            Integer birthyr = null;
            Integer deathyr = null;

            // Parsing birthyr
            try {
                if (line != null) {
                    birthyr = Integer.parseInt(person[2]);
                }
            } catch (Exception e) {
                birthyr = null;
            }

            // Parsing deathyr
            try {
                if (line != null) {
                    deathyr = Integer.parseInt(person[3]);
                }
            } catch (Exception e) {
                deathyr = null;
            }

            //Poupulate Person Table
            pstmt.setInt(1, personId);
            pstmt.setString(2, person[1]);
            if (birthyr != null) {
                pstmt.setInt(3, birthyr);
            } else {
                pstmt.setNull(3, Types.INTEGER);
            }
            if (deathyr != null) {
                pstmt.setInt(4, deathyr);
            } else {
                pstmt.setNull(4, Types.INTEGER);
            }
            pstmt.addBatch();
            if ((index % 10000) == 0) {
                System.out.println(index + " inserting Person Data");
                pstmt.executeBatch();
                conn.commit();
            }
            index++;
        }
        pstmt.executeBatch();
        conn.commit();
        System.out.println("all Person Data Inserted");
        sc.close();
        pstmt.close();
    }

    private static void movie_populate() throws ClassNotFoundException, SQLException, IOException {
       //  Register JDBC driver
        Class.forName("com.mysql.jdbc.Driver");

//        // Open a connection
//        conn = DriverManager.getConnection(DB_URL, USER, PASS);
        conn.setAutoCommit(false);

        //Initializing variable
        int index = 0; // for skipping first line

        //Prepare Statement
        String sql = "UPDATE movie SET rating=?, numberOfVotes=? WHERE id=?";
        pstmt = conn.prepareStatement(sql);

        // Reading Data from File
        InputStream gzipStream = new GZIPInputStream(new FileInputStream(F + "title.ratings.tsv.gz"));
        Scanner sc = new Scanner(gzipStream, "UTF-8");

        while (sc.hasNextLine()) {
            String line = sc.nextLine();
            if (index == 0) {
                index++;
                continue;
            }
            String[] movie = line.split("\t");
            int movieId = Integer.parseInt(movie[0].substring(2));


            //Parsing ratings
            Float avgRating = null;
            Integer noVotes = null;
            try {
                if (line != null) {
                    avgRating = Float.parseFloat(movie[1]);
                }
            } catch (Exception e) {
                avgRating = null;
            }

            // Parsing votes
            try {
                if (line != null) {
                    noVotes = Integer.parseInt(movie[2]);
                }
            } catch (Exception e) {
                noVotes = null;
            }

            //To populate movie Table using update method
            if (avgRating != null) {
                pstmt.setFloat(1, avgRating);
            } else {
                pstmt.setNull(1, Types.FLOAT);
            }
            if (noVotes != null) {
                pstmt.setInt(2, noVotes);
            } else {
                pstmt.setNull(2, Types.INTEGER);
            }
            pstmt.setInt(3, movieId);
            pstmt.addBatch();
            if ((index % 10000) == 0) {
                System.out.println(movieId + " inserting MOVIE Data");
                pstmt.executeBatch();
                conn.commit();
            }
            index++;
        }
        pstmt.executeBatch();
        conn.commit();
        System.out.println("all Movie Data Inserted");
        sc.close();
        pstmt.close();
    }

    private static void entity_relations() throws ClassNotFoundException, SQLException, IOException {
      //   Register JDBC driver
        Class.forName("com.mysql.jdbc.Driver");
//
//  //       Open a connection
//        conn = DriverManager.getConnection(DB_URL, USER, PASS);
        conn.setAutoCommit(false);
        PreparedStatement written_pstmt = null;
        PreparedStatement acted_pstmt = null;
        PreparedStatement composed_pstmt = null;
        PreparedStatement directed_pstmt = null;
        PreparedStatement edited_pstmt = null;
        PreparedStatement producted_pstmt = null;
        //Initializing variable
        int index = 0; // for skipping first line

        //Prepare Statement
        String sql = "INSERT IGNORE INTO actedin(personId,movieId) SELECT Person.id,Movie.id FROM Person,Movie WHERE" +
                " Person.id=? AND Movie.id=?";
        String sql1 = "INSERT IGNORE INTO composedby(personId,movieId) SELECT Person.id,Movie.id FROM Person,Movie WHERE" +
                " Person.id=? AND Movie.id=?";
        String sql2 = "INSERT IGNORE INTO directedby(personId,movieId) SELECT Person.id,Movie.id FROM Person,Movie WHERE" +
                " Person.id=? AND Movie.id=?";
        String sql3 = "INSERT IGNORE INTO editedby(personId,movieId) SELECT Person.id,Movie.id FROM Person,Movie WHERE" +
                " Person.id=? AND Movie.id=?";
        String sql4 = "INSERT IGNORE INTO producedby(personId,movieId) SELECT Person.id,Movie.id FROM Person,Movie WHERE" +
                " Person.id=? AND Movie.id=?";
        String sql5 = "INSERT IGNORE INTO writtenby(personId,movieId) SELECT Person.id,Movie.id FROM Person,Movie WHERE" +
                " Person.id=? AND Movie.id=?";
        acted_pstmt = conn.prepareStatement(sql);
        composed_pstmt = conn.prepareStatement(sql1);
        directed_pstmt = conn.prepareStatement(sql2);
        edited_pstmt = conn.prepareStatement(sql3);
        producted_pstmt = conn.prepareStatement(sql4);
        written_pstmt = conn.prepareStatement(sql5);


        // Reading Data from File
        InputStream gzipStream = new GZIPInputStream(new FileInputStream(F + "title.principals.tsv.gz"));
        Scanner sc = new Scanner(gzipStream, "UTF-8");
        while (sc.hasNextLine()) {
            String line = sc.nextLine();
            //Skipping first line
            if (index == 0) {
                index++;
                continue;
            }
            String[] relation = line.split("\t");
            if (relation[3].contains("actor") || relation[3].contains("actress") || relation[3].contains("self")) {
                acted_pstmt.setInt(1, (int) Long.parseLong(relation[2].substring(2)));
                acted_pstmt.setInt(2, (int) Long.parseLong(relation[0].substring(2)));
                acted_pstmt.addBatch();
            } else if (relation[3].contains("composer")) {
                composed_pstmt.setInt(1, (int) Long.parseLong(relation[2].substring(2)));
                composed_pstmt.setInt(2, (int) Long.parseLong(relation[0].substring(2)));
                composed_pstmt.addBatch();
            } else if (relation[3].contains("editor")) {
                edited_pstmt.setInt(1, (int) Long.parseLong(relation[2].substring(2)));
                edited_pstmt.setInt(2, (int) Long.parseLong(relation[0].substring(2)));
                edited_pstmt.addBatch();
            } else if (relation[3].contains("producer")) {
                producted_pstmt.setInt(1, (int) Long.parseLong(relation[2].substring(2)));
                producted_pstmt.setInt(2, (int) Long.parseLong(relation[0].substring(2)));
                producted_pstmt.addBatch();
            } else if (relation[3].contains("writer")) {
                written_pstmt.setInt(1, (int) Long.parseLong(relation[2].substring(2)));
                written_pstmt.setInt(2, (int) Long.parseLong(relation[0].substring(2)));
                written_pstmt.addBatch();
            } else if (relation[3].contains("director")) {
                directed_pstmt.setInt(1, (int) Long.parseLong(relation[2].substring(2)));
                directed_pstmt.setInt(2, (int) Long.parseLong(relation[0].substring(2)));
                directed_pstmt.addBatch();
            }

            //creating batchsize
            if ((index % 10000) == 0) {
                System.out.println(index + " Adding in actor relation table");
                acted_pstmt.executeBatch();
                conn.commit();

            }
            if ((index % 10000) == 0) {
                System.out.println(index + " Adding in writer relation table");
                written_pstmt.executeBatch();
                conn.commit();

            }
            if ((index % 10000) == 0) {
                System.out.println(index + " Adding in director relation table");
                directed_pstmt.executeLargeBatch();
                conn.commit();

            }
            if ((index % 10000) == 0) {
                System.out.println(index + " Adding in composer relation table");
                composed_pstmt.executeBatch();
                conn.commit();

            }
            if ((index % 10000) == 0) {
                System.out.println(index + " Adding in producer relation table");
                producted_pstmt.executeBatch();
                conn.commit();

            }
            if ((index % 10000) == 0) {
                System.out.println(index + " Adding in editor relation table");
                edited_pstmt.executeBatch();
                conn.commit();
            }


            index++;
        }
        //for remaining batch size
        acted_pstmt.executeBatch();
        written_pstmt.executeBatch();
        directed_pstmt.executeBatch();
        composed_pstmt.executeBatch();
        producted_pstmt.executeBatch();
        edited_pstmt.executeBatch();
        conn.commit();
        sc.close();
        acted_pstmt.close();
        written_pstmt.close();
        directed_pstmt.close();
        composed_pstmt.close();
        producted_pstmt.close();
        edited_pstmt.close();
        conn.close();
    }

    public static void main(String[] args) throws SQLException, IOException, ClassNotFoundException {
        // Register JDBC driver
        Class.forName("com.mysql.jdbc.Driver");

        for (int i = 0; i < args.length; i++) {
            DB_URL = args[0];
            USER = args[1];
            PASS = args[2];
            F = args[3];
        }
        // Open a connection
        conn = DriverManager.getConnection(DB_URL, USER, PASS);

//       CreateDatabase.dataBase();
        CreateGenreTable.genreTable();
        CreatePersonTable.personTable();
        CreateMovieTable.movieTable();
        relations.relation();

        person_populate();
        genre_populate();
        movie_populate();
        entity_relations();
        conn.close();
    }

}
