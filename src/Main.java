import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.neo4j.driver.*;
import org.neo4j.driver.Record;
import static com.mongodb.client.model.Filters.eq;


import java.util.ArrayList;
import java.util.List;
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

        //Q1
        transfertToMongoDB(driver, mongo);

        //Q3
        reverseCollection(mongo);

        //Q4
        findArticle(driver, mongo, "with");

        //Q5
        bestAuthor(driver);

        //Q6 TODO


    }


    // Q1
    private static void transfertToMongoDB(Driver driver, MongoClient mongo) {
        // Lancement session neo4j
        Session session = driver.session();
        // Database sélectionné
        MongoDatabase mongoDatabase = mongo.getDatabase("TPdb");
        // Collection sélectionné
        MongoCollection cl = mongoDatabase.getCollection("index");

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
            cl.insertOne(dc);
        }
        session.close();
    }

    // Q3
    private static void reverseCollection(MongoClient mongo) {
        // Database sélectionné
        MongoDatabase mongoDatabase = mongo.getDatabase("TPdb");
        // Collection sélectionné
        MongoCollection index = mongoDatabase.getCollection("index");
        // Creation de la collection reverseIndex
        MongoCollection reverseIndexCollection = mongoDatabase.getCollection("reverseIndex");

        // Récupération des documents dans index
        List<Document> documents = (List<Document>) index.find().into(new ArrayList<>());

        for (Document d : documents) {
            for (String m :  (List<String>) d.get("motsCles")) {
                // Rechercher le mot dans indexReverse
                FindIterable<Document> documentsOnReverseIndex = reverseIndexCollection.find(eq("mot", m));
                // Modification si existant
                if (documentsOnReverseIndex.first() != null) {
                        Document newDocument = documentsOnReverseIndex.first().append("documents", d.get("idDocument"));
                        UpdateResult result = reverseIndexCollection.updateOne(
                                Filters.eq("mot", m),
                                Updates.addToSet("documents", d.get("idDocument"))
                        );

                } else {
                    // Insertion si non existant
                    Document newDocument = new Document();
                    newDocument.append("mot", m);
                    newDocument.append("documents", List.of(d.get("idDocument")));
                    reverseIndexCollection.insertOne(newDocument);
                }
            }
        }
        // index croissant
        reverseIndexCollection.createIndex(Indexes.ascending("mot"));
    }

    // Q4
    private static void findArticle(Driver driver, MongoClient mongo, String aRechercher) {
        // Lancement session neo4j
        Session session = driver.session();
        // Database sélectionné
        MongoDatabase mongoDatabase = mongo.getDatabase("TPdb");
        // Collection sélectionné
        MongoCollection reverseIndexCollection = mongoDatabase.getCollection("reverseIndex");

        // Recherche du mot clé dans la collection
        Document document = (Document) reverseIndexCollection.find(Filters.eq("mot", aRechercher)).first();

        // faire traitement si le mot clé existe
        if (document != null) {
            // Nombre d'article trouvé
            int nbResults = 0;
            // On récupère les différents id des articles contenant le mot clé
            List<Integer> idDocs = document.getList("documents", Integer.class);
            // On récupère les articles dans neo4j
            Result result = session.run(
                    "MATCH (n:Article) " +
                            "WHERE Id(n) IN " +  idDocs.toString() +
                            "RETURN n.titre as titre, Id(n) as id "+
                            "ORDER BY n.titre ASC "
            );

            // Affichage des articles
            while (result.hasNext()) {
                Record record = result.next();
                Integer id = record.get("id").asInt();
                String titre = record.get("titre").asString();

                nbResults++;
                System.out.println(id + " - " + titre);
            }
            System.out.println("On retrouve " +  nbResults + " article avec le mot " + aRechercher );
        }
        session.close();
    }

    // Q5
    private static void bestAuthor(Driver driver) {
        // Lancement session neo4j
        Session session = driver.session();

        // Requête neo4j
        Result result = session.run(
                "MATCH (a:Auteur) - [r:Ecrire] - (n:Article) " +
                        "WITH a, count(n) as nbArticles " +
                        "RETURN a.nom as nom, nbArticles " +
                        "ORDER BY nbArticles DESC, nom ASC " +
                        "LIMIT 10"
        );

        // Affichage du top
        while(result.hasNext()) {
            Record record = result.next();
            Integer nbArtciles = record.get("nbArticles").asInt();
            String nom = record.get("nom").asString();

            System.out.println(nbArtciles + " - " + nom);
        }
        session.close();
    }
}
