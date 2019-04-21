import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ServerAddress;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Updates;
import com.mongodb.client.MongoCollection;

import org.bson.Document;
import org.bson.types.ObjectId;
import java.util.Arrays;
import com.mongodb.Block;
import com.mongodb.BasicDBObject;

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

	private void deleteSingle(String id) {

		MongoClientURI connectionString = new MongoClientURI("mongodb://ec2-13-59-38-216.us-east-2.compute.amazonaws.com:27018");
		MongoClient mongo = new MongoClient(connectionString);
		MongoDatabase database = mongo.getDatabase("projectdb");
		MongoCollection<Document> collection = database.getCollection("sfdata");
		try {
			DeleteResult dr = collection.deleteOne(eq("Employee_Identifier", Integer.parseInt(id)));
			if(dr.getDeletedCount() == 1)
				System.out.println("Delete Successful");
			else
				System.out.println("ID not found");
		}
		finally{
			mongo.close();
		}
	}

	private void giveRaise(String id) {
		MongoClientURI connectionString = new MongoClientURI("mongodb://ec2-13-59-38-216.us-east-2.compute.amazonaws.com:27018");
		MongoClient mongo = new MongoClient(connectionString);
		MongoDatabase database = mongo.getDatabase("projectdb");
		MongoCollection<Document> collection = database.getCollection("sfdata");

		MongoCursor<Document> cursor = collection.find(eq("Employee_Identifier",Integer.parseInt(id))).sort(new BasicDBObject("Year",-1)).limit(1).iterator();

		ObjectId objID = null;
		double newSalary = 0;
		double newTotalComp = 0;

		try {
			while(cursor.hasNext()) {
				Document doc = cursor.next();
				objID = (ObjectId)doc.get("_id");
				if(objID == null)
					break;
				System.out.println("Giving a raise by 3%");
				System.out.println("Old salary: " + doc.get("Salaries"));
				System.out.println("Old total compensation: " + doc.get("Total_Compensation"));
				newSalary = (Double)doc.get("Salaries") * 1.3;
				newTotalComp = (Double)doc.get("Total_Compensation") + (Double)doc.get("Salaries") * 0.3;
				System.out.println("New salary: " + newSalary);
				System.out.println("New total compensation: " + newTotalComp);
				collection.updateOne(eq("_id",objID), new Document("$set", new Document("Salaries",newSalary)));
				collection.updateOne(eq("_id",objID), new Document("$set", new Document("Total_Compensation",newTotalComp)));
				System.out.println("Update Complete");
			}
		}
		finally {
			mongo.close();
		}
	}

	private void insertSingle(String yearType, String empID, String job) {
		MongoClientURI connectionString = new MongoClientURI("mongodb://ec2-13-59-38-216.us-east-2.compute.amazonaws.com:27018");
		MongoClient mongo = new MongoClient(connectionString);
		MongoDatabase database = mongo.getDatabase("projectdb");
		MongoCollection<Document> collection = database.getCollection("sfdata");

		int intEmpID = Integer.parseInt(empID);

		try{
			Document doc = new Document("Year_Type", yearType).append("Year", 2019).append("Organization_Group_Code", 0)
					.append("Organizatoin_Group", "").append("Department_Code", "").append("Department", "")
					.append("Union_Code", 0).append("Union", "").append("Job_Family_Code", 0).append("Job_Family", "")
					.append("Job_Code", 0).append("Job", job).append("Employee_Identifier", intEmpID).append("Salaries", 0)
					.append("Overtime", 0).append("Other_Salaries", 0).append("Total_Salary", 0).append("Retirement", 0)
					.append("Health_and_Dental", 0).append("Other_Benefits", 0).append("Total_Benefits", 0)
					.append("Total_Compensation", 0);
			collection.insertOne(doc);

			//This is for printing out inserted value
			Document document = collection.find(eq("Employee_Identifier", intEmpID)).first();
			System.out.println("Employee Identifier: " + document.get("Employee_Identifier") + "\n" +
					"Job: " + document.get("Job") + "\n" +
					"Year_Type: " + document.get("Year_Type") + "\n");
		}
		finally{
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
			String input = "";
			if(s.hasNextLine()) {
				input = s.nextLine();
			}
			if(input.toLowerCase().trim().equals("help")) {
				System.out.println("exit - exit console");
				System.out.println("count - return the number of records on file");
				System.out.println("publicprotection - return employees from public protection from 2017-2018");
				System.out.println("deletesingle - delete a single employee by passing employee id");
				System.out.println("giveraise - give an employee a raise");
				System.out.println("insertsingle - Insert a new employee into the database");
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
			else if(input.toLowerCase().trim().equals("deletesingle")) {
				String id = "";
				System.out.println("Enter the id of the employee to delete: ");
				if(s.hasNextLine())
					id = s.nextLine();
				if(!id.isEmpty())
					main.deleteSingle(id);
			}
			else if(input.toLowerCase().trim().equals("giveraise")) {
				String id = "";
				System.out.println("Enter the id of the employee to give a raise to:");
				if(s.hasNextLine())
					id = s.nextLine();
				if(!id.isEmpty())
					main.giveRaise(id);
			}
			else if(input.toLowerCase().trim().equals("insertsingle")){
				String year = "";
				String id = "";
				String job = "";
				System.out.println("Enter the year type(Fiscal or Calendar): ");
				if(s.hasNextLine())
					year = s.nextLine();
				System.out.println("Enter your employee ID(Only numbers): ");
				if(s.hasNextLine())
					id = s.nextLine();
				System.out.println("Enter your current job: ");
				if(s.hasNextLine())
					job = s.nextLine();
				main.insertSingle(year, id, job);

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
