import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Indexes;
import org.bson.Document;
import org.neo4j.driver.*;
import org.neo4j.driver.Record;
import static com.mongodb.client.model.Filters.eq;


import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.StringTokenizer;

public class Main {
    public static void main(String[] args) {
        /* Connexion à MongoDB */
        MongoClient mongo = MongoClients.create("mongodb://localhost:27017");

        /* Connexion à Neo4j server */
        Driver driver = GraphDatabase.driver( "bolt://localhost:7687", AuthTokens.none());

        /* Selection pour mongo db */
        MongoDatabase mongoDatabase = mongo.getDatabase("TPdb");     // Database sélectionné
        MongoCollection mongoCollection = mongoDatabase.getCollection("index");; // Collection sélectionné

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
                transfertToMongoDB(driver, mongoCollection);
            } else if (optionChoisie.equals("4")) {
                reverseCollection(mongoDatabase, mongoCollection);
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
        System.out.println("3 - Transfert data");
        System.out.println("4 - Reverse Index");
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

    // Q1
    private static void transfertToMongoDB(Driver driver, MongoCollection cl) {
        // Lancement session neo4j
        Session session = driver.session();
        // Execution requête neo4j
        Result result = session.run(
                "MATCH (n:Article) " +
                        "RETURN Id(n) as id, n.titre as titre "+
                        "ORDER BY n.titre ASC"
        );
        while (result.hasNext()) {
            Record record = result.next();
            Integer id = record.get("id").asInt();

            // Création du champs motcles
            String titre  = record.get("titre").asString().trim().toLowerCase();
            StringTokenizer st = new StringTokenizer(titre, ".( )+[]{}?! ");
            ArrayList<String> motcles = new ArrayList<String>();

            while (st.hasMoreTokens()) {
                motcles.add(st.nextToken());
            }

            // Création du document et insertion
            Document dc = new Document().append("idDocument", id).append("motsCles", motcles);
            insertOne(cl, dc);
        }
        session.close();
    }

    // Q2
    private static void reverseCollection(MongoDatabase db, MongoCollection index) {
        // Creation de la collection reverseIndex
        MongoCollection reverseIndexCollection = db.getCollection("reverseIndex");

        // Récupération des documents dans index
        List<Document> documents = (List<Document>) index.find().into(new ArrayList<>());

        for (Document d : documents) {
            for (Document current : (List<Document>) d) {
                // Rechercher le mot dans indexReverse
                FindIterable<Document> documentsOnReverseIndex = reverseIndexCollection.find(eq("mot", current.get("motCles")));
                if (documentsOnReverseIndex.first() != null) {
                    Document newDocument = documentsOnReverseIndex.first().append("documents", current.get("idDocument"));
                    reverseIndexCollection.replaceOne(
                            Filters.eq("_id", documentsOnReverseIndex.first().get("_id")),
                            newDocument
                    );
                } else {
                    Document newDocument = new Document();
                    newDocument.put("mot", current.get("motCles"));
                    reverseIndexCollection.insertOne(newDocument);
                }
            }
        }
        // index croissant
        reverseIndexCollection.createIndex(Indexes.ascending("mot"));
    }
}
