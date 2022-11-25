import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.neo4j.driver.*;
import org.neo4j.driver.Record;

import java.util.ArrayList;
import java.util.Scanner;
import java.util.StringTokenizer;

public class Main {
    public static void main(String[] args) {
        MongoDatabase mongoDatabase = null;     // Database selected
        MongoCollection mongoCollection = null; // Collection selected

        /* Connection to MongoDB */
        MongoClient mongo = MongoClients.create("mongodb://localhost:27017");

        // Connexion directe au serveur Neo4J
        Driver driver = GraphDatabase.driver( "bolt://localhost:7687", AuthTokens.basic("neo4j", "neo4j") );

        String optionChoisie;
        String input;
        Scanner sc = new Scanner (System.in);

        do {
            afficheMenu(mongoDatabase, mongoCollection);
            optionChoisie = sc.nextLine();

            if (optionChoisie.equals("1")) {
                input = sc.nextLine();
                mongoDatabase = selectDatabase(mongo, input);
            } else if (optionChoisie.equals("2")) {
                input = sc.nextLine();
                mongoCollection = selectCollection(mongoDatabase, input);
            } else if (optionChoisie.equals("3")) {
                // TODO TRANSFERT NEO4J TO MONGO
            }
        } while (!optionChoisie.equals("0"));
    }

    private static void afficheMenu(MongoDatabase db, MongoCollection<Document> col){
        System.out.println();
        System.out.println("Menu : ");
        System.out.println("1 - Sélectionner une base MongoDB (actuellement : " +
                (db == null ? "Aucune":db.getName()) + ")");
        System.out.println("2 - Sélectionner une collection MongoDB (actuellement : " +
                (col == null ? "Aucune":col.getNamespace().getCollectionName())+")");
        System.out.println("3 - Lister tous les documents");
        System.out.println("4 - Rechercher un document par nom");
        System.out.println();
        System.out.println("0 - Quitter");

    }

    private static MongoDatabase selectDatabase(MongoClient mongo, String databaseName) {
        return mongo.getDatabase(databaseName);
    }

    private static MongoCollection selectCollection(MongoDatabase db, String collectionName) {
        return db.getCollection(collectionName);
    }

    private static void insertOne(MongoCollection cl, Document dc) {
        cl.insertOne(dc);
    }

    private static void transfertToMongoDB(Driver driver, MongoCollection cl) {
        Session session = driver.session();
        Result result = session.run(
                "MATCH (n:Article) " +
                        "RETURN n.id, n.titre "+
                        "ORDER BY titre ASC"
        );

        while (result.hasNext())
        {
            Record record = result.next();
            int id = record.get("id").asInt();
            String titre  = record.get("titre").asString().trim().toLowerCase();
            StringTokenizer st = new StringTokenizer(titre, ".( )+[]{}?! ");
            ArrayList<String> motcles = new ArrayList<String>();

            while (st.hasMoreTokens()){
                motcles.add(st.nextToken());
            }
            Document dc = new Document().append("idDocument", id).append("motsCles", motcles);
            insertOne(cl, dc);
        }
        session.close();
    }
}
