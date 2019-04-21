import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ServerAddress;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;

import org.bson.Document;
import java.util.Arrays;
import com.mongodb.Block;

import com.mongodb.client.MongoCursor;
import com.mongodb.client.FindIterable;
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
		MongoDatabase database = mongo.getDatabase("projectdb");
		MongoCollection<Document> collection = database.getCollection("sfdata");
		System.out.println("Collection " +
				collection.getNamespace().getDatabaseName() +
				"." +
				collection.getNamespace().getCollectionName() +
				" has " +
				collection.countDocuments() +
				" records.");
		mongo.close();
	}

	private void publicProtection() {
		MongoClientURI connectionString = new MongoClientURI("mongodb://ec2-13-59-38-216.us-east-2.compute.amazonaws.com:27018");
		MongoClient mongo = new MongoClient(connectionString);
		MongoDatabase database = mongo.getDatabase("projectdb");
		MongoCollection<Document> collection = database.getCollection("sfdata");

		MongoCursor<Document> cursor = collection.find(and(eq("Organization_Group","Public Protection"),or(eq("Year",2018),eq("Year",2017)))).limit(1000).iterator();

		try {
			while(cursor.hasNext()) {
				Document doc = cursor.next();
				System.out.println("Employee Identifier: " + doc.get("Employee_Identifier") + "\n" +
						"Organization Group: " + doc.get("Organization_Group") + "\n" +
						"Year: " + doc.get("Year") + "\n");
			}
		}
		finally {
			cursor.close();
			mongo.close();
		}
	}

	public static void main(String[] args) {
		Logger mongoLogger = Logger.getLogger("org.mongodb.driver");
		mongoLogger.setLevel(Level.SEVERE);

		Main main = new Main();

		System.out.println("Welcome to the Employee Compensation Console\n\n"
				+ "Version: 0.1\n\n"
				+ "Enter help for list of commands\n");

		Scanner s = new Scanner(System.in);

		while(true) {
			System.out.print("command> ");
			String input = s.nextLine();
			if(input.toLowerCase().trim().equals("help")) {
				System.out.println("exit - exit console");
				System.out.println("count - return the number of records on file");
				System.out.println("publicprotection - return employees from public protection from 2017-2018");
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
			else if(input.toLowerCase().trim().equals("publicprotection")) {
				main.publicProtection();
			}
			else if(input.isEmpty()) {

			}
			else {
				System.out.println("Invalid command. Use help to view commands.");
			}
		}
		s.close();
	}
}
