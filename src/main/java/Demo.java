import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.*;
import static com.mongodb.client.model.Updates.*;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

public class Demo {
    public static void main(String[] args) {
        MongoClient cliente = new MongoClient();
        MongoDatabase db = cliente.getDatabase("nba");
        MongoCollection<Document> coll = db.getCollection("jugadores");
        for (Document d: coll.find(eq("country","Spain"))) {
            System.out.printf("%s %s\n", d.getString("firstName"),
                    d.getString("lastName"));
        }

        coll.find(new Document()).
                forEach((Consumer<Document>) it -> System.out.println(it.toJson()));

        Document d = new Document("lastName", "Aldama").append("firstName","Santi");
        coll.insertOne(d);

        FindIterable<Document> iterobj
                = coll.find(eq("country","Spain")).
                projection(exclude("country"));

        // Print the documents using iterators
        Iterator itr = iterobj.iterator();
        while (itr.hasNext()) {
            System.out.println(itr.next());
        }

        List<Document> lista = coll.find().into(new ArrayList<Document>());
        System.out.println(lista.get(0));

        coll.updateOne(eq("lastName","Abrines"),
                set("country","Baleares"));

        coll.deleteMany(eq("lastName", "Gasol"));

        List<Document> pipeline = Arrays.asList(new Document("$group",
                        new Document("_id",
                                new Document("teamId", "$teamId"))
                                .append("alturaMedia",
                                        new Document("$avg",
                                                new Document("$toDouble", "$heightMeters")))
                                .append("pesoMedio",
                                        new Document("$avg",
                                                new Document("$toDouble", "$weightKilograms")))),
                new Document("$lookup",
                        new Document("from", "equipos")
                                .append("localField", "_id.teamId")
                                .append("foreignField", "teamId")
                                .append("as", "team")),
                new Document("$project",
                        new Document("_id", 1L)
                                .append("alturaMedia", 1L)
                                .append("pesoMedio", 1L)
                                .append("equipo",
                                        new Document("$arrayElemAt", Arrays.asList("$team", 0L)))),
                new Document("$project",
                        new Document("_id", 0L)
                                .append("alturaMedia", 1L)
                                .append("pesoMedio", 1L)
                                .append("equipo", "$equipo.nickname")));

        List<Document> results = coll.aggregate(pipeline).into(new ArrayList<Document>());
        for (Document doc: results) {
            System.out.println(doc.toJson());
        }

        cliente.close();

    }
}