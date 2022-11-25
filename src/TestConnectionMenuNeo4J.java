
import java.util.Scanner;

import org.neo4j.driver.*;
import org.neo4j.driver.Record;
import org.neo4j.driver.types.Node;


public class TestConnectionMenuNeo4J {

    public static Scanner sc = new Scanner(System.in);

    public static int menu(){

        System.out.println("Menu : ");
        System.out.println("\t(1) Lister tous les films;");
        System.out.println("\t(2) Lister toutes les personnes;");
        System.out.println("\t(3) Afficher les 3 films les plus not�s;");
        System.out.println("\t(4) Afficher 5 films 'proches';");
        System.out.println("");
        System.out.println("\t(0) Quitter;");

        System.out.println("");
        System.out.println("Votre choix ?");

        int choix = sc.nextInt();

        sc.nextLine();

        return choix;
    }


    public static void afficheFilms(Driver driver){
        // D�marrer une session
        Session session = driver.session();

        // Traitement
        Result result = session.run(
                "match (f:Movie)" +
                        "return f.title as titre, f.released as date, f.tagline as tags "+
                        "order by date DESC"
        );


        // Affichage
        System.out.println("Films disponibles :");
        while ( result.hasNext() )
        {
            Record record = result.next();
            int date = record.get( "date" ).asInt();
            String titre  = record.get( "titre" ).asString();
            String tags = record.get( "tags" ).asString();

            System.out.println("\t"+titre+" - "+date+" ("+ tags +")");
        }

        // Fermeture de la session
        session.close();
    }


    public static void affichePersonnes(Driver driver){
        // D�marrer une session
        Session session = driver.session();

        // Traitement
        Result result = session.run(
                "match (p:Person)-[r]-(m:Movie) " +
                        "with p, r, m " +
                        "order by m.title " +

                        "with p, type(r) as relation, collect(m.title) as films " +

                        "return p as personne, relation , films  "+
                        "order by p.name ASC, relation ASC"
        );


        // Affichage
        System.out.println("Personnes disponibles :");
        String nom = null;
        while ( result.hasNext() )
        {
            Record record = result.next();

            Node personne = record.get( "personne" ).asNode();

            String nomPersonne = personne.get("name").asString();

            if (nom == null || !nom.equals(nomPersonne) ){
                nom = nomPersonne;
                System.out.println(nomPersonne +" ("+(personne.containsKey("born") ? personne.get("born").asInt() : "?")+")");
            }

            String nomRelation = record.get( "relation" ).asString();

            String films = record.get( "films" ).toString();


            System.out.println("\t"+nomRelation+" "+films);
        }

        // Fermeture de la session
        session.close();
    }


    public static void affiche3MeilleursFilms(Driver driver){
        // D�marrer une session
        Session session = driver.session();

        // Traitement
        Result result = session.run(
                "match (m:Movie)<-[r:REVIEWED]-() " +
                        "return m.title as titre, r.rating as rating " +
                        "order by rating DESC " +
                        "limit 3"
        );


        // Affichage
        System.out.println("3 Films les mieux not�s :");
        while ( result.hasNext() )
        {
            Record record = result.next();
            int note = record.get( "rating" ).asInt();
            String titre  = record.get( "titre" ).asString();

            System.out.println("\t"+titre+" - "+note);
        }

        // Fermeture de la session
        session.close();
    }


    public static void affiche5FilmsProches(Driver driver){
        // D�marrer une session
        Session session = driver.session();


        String titreSaisi = "The Matrix";



        System.out.println("Donner le titre du film : ");
        titreSaisi = sc.nextLine();

        // Traitement
        Result result = session.run(
                "match (m:Movie) " +
                        "where m.title = '"+titreSaisi+"' " +
                        "return count(distinct m.title) as nb"
        );

        Record record = result.next();
        int nb = record.get( "nb" ).asInt();

        if (nb == 0){
            System.out.println("Le film '"+titreSaisi+"' n'existe pas dans la base....");
        }
        else{
            // Traitement
            result = session.run(
                    "match (m:Movie)<-[r:ACTED_IN]-(p:Person)-[r2:ACTED_IN]->(m1:Movie) " +
                            "where m.title='"+titreSaisi+"' " +
                            "return m1.title as titre, count(distinct p.name) as nb " +
                            "order by nb DESC, m1.title ASC " +
                            "limit 5"
            );


            // Affichage
            System.out.println("Au plus 5 Films proches de '"+titreSaisi+"' :");
            while ( result.hasNext() )
            {
                record = result.next();
                nb = record.get( "nb" ).asInt();
                String titre  = record.get( "titre" ).asString();

                System.out.println("\t"+titre+" - "+nb);
            }

        }



        // Fermeture de la session
        session.close();
    }

    public static void main(String[] args) {
        // Connexion directe au serveur Neo4J
        Driver driver = GraphDatabase.driver( "bolt://localhost", AuthTokens.basic("neo4j", "$iutinfo") );

        int choix;
        // Boucle principale;
        do{

            choix = menu();

            switch (choix){
                case 1 : afficheFilms(driver) ;
                    break;

                case 2 : affichePersonnes(driver) ;
                    break;

                case 3 : affiche3MeilleursFilms(driver) ;
                    break;

                case 4 : affiche5FilmsProches(driver) ;
                    break;

                case 0 : break;
                default : System.out.println("Choix inconnu... Recommencez...");

            }
        }while (choix != 0);
        // Fermeture de la connexion
        driver.close();
    }
}