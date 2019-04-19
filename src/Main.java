import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ServerAddress;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;

import org.bson.Document;
import java.util.Arrays;
import com.mongodb.Block;

import com.mongodb.client.MongoCursor;
import static com.mongodb.client.model.Filters.*;
import com.mongodb.client.result.DeleteResult;
import static com.mongodb.client.model.Updates.*;
import com.mongodb.client.result.UpdateResult;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import java.util.Scanner;

public class Main {

	private void count() {
		MongoClientURI connectionString = new MongoClientURI("mongodb://ec2-13-59-38-216.us-east-2.compute.amazonaws.com:27018");
		MongoClient mongo = new MongoClient(connectionString);
		MongoDatabase database = mongo.getDatabase("testdb");
		MongoCollection<Document> collection = database.getCollection("testCol");
		System.out.println("Collection " +
				collection.getNamespace().getDatabaseName() +
				"." +
				collection.getNamespace().getCollectionName() +
				" has " +
				collection.countDocuments() +
				" records.");
		mongo.close();
	}

	public static void main(String[] args) {
		Logger mongoLogger = Logger.getLogger("org.mongodb.driver");
		mongoLogger.setLevel(Level.SEVERE);

		Main main = new Main();

		System.out.println("Welcome to the Employee Compensation console\n\n"
				+ "Version: 0.1\n\n"
				+ "Enter help for list of commands\n");

		Scanner s = new Scanner(System.in);

		while(true) {
			System.out.print("command> ");
			String input = s.next();
			if(input.toLowerCase().trim().equals("help")) {
				System.out.println("exit - exit console");
				System.out.println("count - return the number of records on file");
				System.out.println("? - ");
				System.out.println("? - ");
				System.out.println("? - ");
				System.out.println("? - ");
				System.out.println("? - ");
				System.out.println("? - ");
				System.out.println("? - ");
				System.out.println("? - ");
				System.out.println("? - ");
				System.out.println("? - ");
				System.out.println("? - ");
				System.out.println("? - ");
				System.out.println("? - ");
				System.out.println("? - ");
			}
			else if (input.toLowerCase().trim().equals("exit"))
				break;
			else if(input.toLowerCase().trim().equals("count")) {
				main.count();
			}
		}
    s.close();
	}
}
