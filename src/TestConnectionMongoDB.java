import java.sql.ClientInfoStatus;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.mongodb.client.model.Filters.eq;

import com.mongodb.BasicDBObject;
import com.mongodb.Block;
import com.mongodb.DBObject;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Filters.*;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.Updates;
import com.mongodb.client.model.Updates.*;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;

import org.bson.Document;

import org.bson.conversions.Bson;


public class TestConnectionMongoDB{


    public static void truc(String [] a){

        MongoDatabase database = null;

        MongoCollection collection = null;


        // D�finition d'une "connectString"
        String uri = "mongodb://localhost:27017";

        // Connexion au serveur MongDB via la connectString
        final MongoClient mongoClient = MongoClients.create(uri);

        // S�lection de la base de donn�es
        database = null;

        String optionChoisie;
        Scanner sc = new Scanner (System.in);

        do{

            afficheMenu(database, collection);
            optionChoisie = sc.nextLine();

            // Traitement de l'option
            if (optionChoisie.equals("1")){
                System.out.println("Donner le nom de la base de donn�e :");
                String bdd = sc.nextLine();

                MongoIterable<String> listeDB = mongoClient.listDatabaseNames();

                boolean trouve = false;
                for (String s : listeDB) {
                    if (s.equals(bdd)) {
                        trouve = true;
                    }
                }
                if (trouve) {
                    database = mongoClient.getDatabase(bdd);
                }
                else{
                    System.out.println("BDD "+bdd+" inconnue...");
                }
            }
            else{
                if (optionChoisie.equals("2")){
                    if (database == null){
                        System.out.println("Veuillez d'abord s�lectionner une base de donn�es !");
                    }
                    else{
                        String coll = sc.nextLine();

                        boolean trouve = false;
                        for (String s : database.listCollectionNames()){
                            if (s.equals(coll)){
                                trouve = true;
                            }
                        }
                        if (trouve)
                            collection = database.getCollection(coll);
                        else
                            System.out.println("Collection "+coll+" inconnue !");
                    }

                }
                else{
                    if (optionChoisie.equals("3")){
                        if (collection == null){
                            System.out.println("Veuillez d'abord s�lectionner une collection !");
                        }
                        else{
                            // Application du traitement par document
                            for (Object d : collection.find()) {
                                afficheDocumentIndente((Document)d,0);
                            }
                        }
                    }
                    else{
                        if (optionChoisie.equals("4")){
                            if (collection == null){
                                System.out.println("Veuillez d'abord s�lectionner une collection !");
                            }
                            else{
                                System.out.println("Donner le nom � rechercher dans les documents");
                                String nom = sc.nextLine();

                                // Application du traitement par document
                                FindIterable<Document> docs = collection.find(Filters.eq("name",nom));

                                if (docs.first() == null){
                                    System.out.println("Aucun document s�lectionn�");
                                }
                                else{
                                    for (Document doc : docs){
                                        afficheDocumentIndente(doc, 0);
                                    }
                                }
                            }
                        }
                        else{
                            if (!optionChoisie.equals("0")){
                                System.out.println("Option non connue....");
                            }
                        }
                    }
                }
            }



        }while (!optionChoisie.equals("0"));

        // Fermeture de la connexion
        mongoClient.close();
    }



    private static void afficheMenu(MongoDatabase bdd, MongoCollection<Document> coll){
        System.out.println();
        System.out.println("Menu : ");
        System.out.println("1 - S�lectionner une base (actuellement : "+(bdd == null ? "Aucune":bdd.getName())+")");
        System.out.println("2 - S�lectionner une collection (actuellement : "+(coll == null ? "Aucune":coll.getNamespace().getCollectionName())+")");
        System.out.println("3 - Lister tous les documents");
        System.out.println("4 - Rechercher un document par nom");
        System.out.println();
        System.out.println("0 - Quitter");

    }

    private static void afficheTAB(int niveau){
        for (int i = 0 ; i < niveau ; i++){
            System.out.print("\t");
        }
    }

    private static void affiche(Object obj, int niveau){
        if (obj instanceof List) {

            List l = (List)obj;
            System.out.println();
            afficheTAB(niveau);
            System.out.println("[");

            int i = 0;
            for(Object oo : l){

                if (i>0){
                    System.out.println(",");
                }

                affiche(oo, niveau);
                i++;
            }

            System.out.println();
            afficheTAB(niveau);
            System.out.print("]");
        }
        else{
            if (obj instanceof Document){
                afficheDocumentIndente ((Document)obj, niveau+1);
            }
            else{
                System.out.print (obj);
            }
        }
    }


    private static void afficheDocumentIndente(Document d, int niveau){
        System.out.println();
        afficheTAB(niveau);
        System.out.println("{");

        // Affiche les diff�rents attributs
        int i = 0;
        Set<Entry<String, Object>> attrs = d.entrySet();
        for (Entry<String, Object> attr : attrs) {

            if (i>0){
                System.out.println(",");
            }

            afficheTAB(niveau + 1);

            System.out.print(attr.getKey()+" : ");

            Object o = attr.getValue();

            affiche(o, niveau+1);

            i++;
        }

        System.out.println();
        afficheTAB(niveau);
        System.out.print("}");

    }
}