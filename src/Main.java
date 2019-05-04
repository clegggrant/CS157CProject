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

		MongoCursor<Document> cursor = collection.find(and(eq("Organization_Group","Public Protection"),or(eq("Year",2018),eq("Year",2017)))).limit(500).iterator();

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

		try {
			ObjectId objID = null;
			double newSalary = 0;
			double newTotalComp = 0;

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
			cursor.close();
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
			Document document = collection.find(and(eq("Employee_Identifier", intEmpID),eq("Year_Type", yearType),eq("Job",job))).first();
			System.out.println("Employee Identifier: " + document.get("Employee_Identifier") + "\n" +
					"Job: " + document.get("Job") + "\n" +
					"Year_Type: " + document.get("Year_Type") + "\n");
		}
		finally{
			mongo.close();
		}
	}

	private void salaryAsc() {
		MongoClientURI connectionString = new MongoClientURI("mongodb://ec2-13-59-38-216.us-east-2.compute.amazonaws.com:27018");
		MongoClient mongo = new MongoClient(connectionString);
		MongoDatabase database = mongo.getDatabase("projectdb");
		MongoCollection<Document> collection = database.getCollection("sfdata");

		MongoCursor<Document> cursor = collection.find(and(ne("Department","Untitled"),ne("Job_Family",0))).sort(new BasicDBObject("Total_Salary",1)).limit(100).iterator();

		try {
			while(cursor.hasNext()) {
				Document doc = cursor.next();
				System.out.println("Employee ID: " + doc.get("Employee_Identifier") + "\n" +
						"Department: " + doc.get("Department") + "\n" +
						"Job Family: " + doc.get("Job_Family") + "\n" +
						"Total Salary: " + doc.get("Total_Salary") + "\n");
			}
		}
		finally {
			cursor.close();
			mongo.close();
		}
	}

	private void retirementAndHealth() {
		MongoClientURI connectionString = new MongoClientURI("mongodb://ec2-13-59-38-216.us-east-2.compute.amazonaws.com:27018");
		MongoClient mongo = new MongoClient(connectionString);
		MongoDatabase database = mongo.getDatabase("projectdb");
		MongoCollection<Document> collection = database.getCollection("sfdata");

		MongoCursor<Document> cursor = collection.find(and(or(eq("Year",2010),eq("Year",2015),eq("Year",2020)),gt("Retirement",1000),gt("Health_and_Dental",500))).sort(new BasicDBObject("Employee_Identifier",1).append("Year", -1)).limit(1000).iterator();

		try {
			while(cursor.hasNext()) {
				Document doc = cursor.next();
				System.out.println("Employee ID: " + doc.get("Employee_Identifier") + "\n" +
						"Year: " + doc.get("Year") + "\n" +
						"Retirement: " + doc.get("Retirement") + "\n" +
						"Health and Dental: " + doc.get("Health_and_Dental") + "\n" +
						"Total Compensation: " + doc.get("Total_Compensation") + "\n");
			}
		}
		finally {
			cursor.close();
			mongo.close();
		}
	}

	private void maxForYear(String year) {
		int yr = Integer.parseInt(year);

		MongoClientURI connectionString = new MongoClientURI("mongodb://ec2-13-59-38-216.us-east-2.compute.amazonaws.com:27018");
		MongoClient mongo = new MongoClient(connectionString);
		MongoDatabase database = mongo.getDatabase("projectdb");
		MongoCollection<Document> collection = database.getCollection("sfdata");

		MongoCursor<Document> cursor = collection.find(eq("Year",yr)).sort(new BasicDBObject("Total_Compensation",-1)).limit(1).iterator();

		try {
			if(cursor.hasNext()) {
				Document doc = cursor.next();
				System.out.println("Employee ID: " + doc.get("Employee_Identifier") + "\n" +
						"Year: " + doc.get("Year") + "\n" +
						"Total Compensation: " + doc.get("Total_Compensation") + "\n");
			}
			else {
				System.out.println("There is no compensation data for the year " + year);
			}
		}
		finally {
			cursor.close();
			mongo.close();
		}
	}

	private void otAndOther() {
		MongoClientURI connectionString = new MongoClientURI("mongodb://ec2-13-59-38-216.us-east-2.compute.amazonaws.com:27018");
		MongoClient mongo = new MongoClient(connectionString);
		MongoDatabase database = mongo.getDatabase("projectdb");
		MongoCollection<Document> collection = database.getCollection("sfdata");

		Document match = new Document("$match", new Document("Job","Deputy Court Clerk II"));
		Document group = new Document("$group", new Document("_id",null).append("Overtime_Sum", new Document("$sum","$Overtime")).append("Other_Salary_Sum", new Document("$sum","$Other_Salaries")));

		MongoCursor<Document> cursor = collection.aggregate(Arrays.asList(match, group)).iterator();

		try {
			while(cursor.hasNext()) {
				Document doc = cursor.next();
				System.out.println("Total Overtime: " + doc.get("Overtime_Sum") + "\n" +
						"Total Other Salaries: " + doc.get("Other_Salary_Sum"));
			}
		}
		finally {
			cursor.close();
			mongo.close();
		}

	}

	private void healthForAll(String year) {
		int yr = Integer.parseInt(year);

		MongoClientURI connectionString = new MongoClientURI("mongodb://ec2-13-59-38-216.us-east-2.compute.amazonaws.com:27018");
		MongoClient mongo = new MongoClient(connectionString);
		MongoDatabase database = mongo.getDatabase("projectdb");
		MongoCollection<Document> collection = database.getCollection("sfdata");

		try {
			collection.updateMany(and(eq("Year", yr),eq("Health_and_Dental",0)), inc("Total_Compensation",1000));
			UpdateResult result = collection.updateMany(and(eq("Year", yr),eq("Health_and_Dental",0)), inc("Health_and_Dental",1000));
			System.out.println("Matched: " + result.getMatchedCount() + "\n" +
					"Updated: " + result.getModifiedCount());
		}
		finally {
			mongo.close();
		}
	}

	private void totalCompForYear(String year) {
		int yr = Integer.parseInt(year);

		MongoClientURI connectionString = new MongoClientURI("mongodb://ec2-13-59-38-216.us-east-2.compute.amazonaws.com:27018");
		MongoClient mongo = new MongoClient(connectionString);
		MongoDatabase database = mongo.getDatabase("projectdb");
		MongoCollection<Document> collection = database.getCollection("sfdata");

		Document match = new Document("$match", new Document("Year",yr));
		Document group = new Document("$group", new Document("_id",null).append("Total_Comp_Sum", new Document("$sum","$Total_Compensation")));

		MongoCursor<Document> cursor = collection.aggregate(Arrays.asList(match,group)).iterator();

		try {
			while(cursor.hasNext()) {
				Document doc = cursor.next();
				System.out.println("Year: " + year + "\n" +
						"Total Compensation: " + doc.get("Total_Comp_Sum"));
			}
		}
		finally {
			cursor.close();
			mongo.close();
		}
	}

	private void overtimeSum(String year) {
		int yr = Integer.parseInt(year);

		MongoClientURI connectionString = new MongoClientURI("mongodb://ec2-13-59-38-216.us-east-2.compute.amazonaws.com:27018");
		MongoClient mongo = new MongoClient(connectionString);
		MongoDatabase database = mongo.getDatabase("projectdb");
		MongoCollection<Document> collection = database.getCollection("sfdata");

		Document match = new Document("$match", new Document("Year",yr));
		Document group = new Document("$group", new Document("_id",null).append("Overtime_Sum", new Document("$sum","$Overtime")));

		MongoCursor<Document> cursor = collection.aggregate(Arrays.asList(match,group)).iterator();

		try {
			while(cursor.hasNext()) {
				Document doc = cursor.next();
				System.out.println("Year: " + year + "\n" +
						"Total Overtime: " + doc.get("Overtime_Sum"));
			}
		}
		finally {
			cursor.close();
			mongo.close();
		}

	}

	private void updateSalary(String empid, String amount, String which) {
		int emp = Integer.parseInt(empid);
		int amt = Integer.parseInt(amount);

		MongoClientURI connectionString = new MongoClientURI("mongodb://ec2-13-59-38-216.us-east-2.compute.amazonaws.com:27018");
		MongoClient mongo = new MongoClient(connectionString);
		MongoDatabase database = mongo.getDatabase("projectdb");
		MongoCollection<Document> collection = database.getCollection("sfdata");

		MongoCursor<Document> cursor = collection.find(eq("Employee_Identifier", emp)).sort(new Document("Year",-1)).limit(1).iterator();

		try {
			ObjectId id = null;
			while(cursor.hasNext()) {
				Document doc = cursor.next();
				id = (ObjectId)doc.get("_id");
				System.out.println("Old " + which + ": " + doc.get(which));
				int difference = (int)doc.get(which) - amt;
				collection.updateOne(eq("_id",id), new Document("$set", new Document(which,amt)));
				if(difference >= 0)
					collection.updateOne(eq("_id",id), new Document("$inc", new Document("Total_Compensation",-difference)));
				else
					collection.updateOne(eq("_id",id), new Document("$inc", new Document("Total_Compensation",-difference)));
				System.out.println("New " + which + ": " + amt);
			}
		}
		finally {
			cursor.close();
			mongo.close();
		}

	}

	private void maxForJob(String job) {
		MongoClientURI connectionString = new MongoClientURI("mongodb://ec2-13-59-38-216.us-east-2.compute.amazonaws.com:27018");
		MongoClient mongo = new MongoClient(connectionString);
		MongoDatabase database = mongo.getDatabase("projectdb");
		MongoCollection<Document> collection = database.getCollection("sfdata");

		MongoCursor<Document> cursor = collection.find(eq("Job",job)).sort(new BasicDBObject("Total_Compensation",-1)).limit(1).iterator();

		try {
			while(cursor.hasNext()) {
				Document doc = cursor.next();
				System.out.println("Job: " + doc.get("Job") + "\n" +
						"Employee ID: " + doc.get("Employee_Identifier") + "\n" +
						"Total Compensation: " + doc.get("Total_Compensation"));
			}
		}
		finally {
			cursor.close();
			mongo.close();
		}
	}

	private void maxForDeptCode(String deptCode) {
		int deptC = Integer.parseInt(deptCode);

		MongoClientURI connectionString = new MongoClientURI("mongodb://ec2-13-59-38-216.us-east-2.compute.amazonaws.com:27018");
		MongoClient mongo = new MongoClient(connectionString);
		MongoDatabase database = mongo.getDatabase("projectdb");
		MongoCollection<Document> collection = database.getCollection("sfdata");

		MongoCursor<Document> cursor = collection.find(eq("Department_Code",deptC)).sort(new BasicDBObject("Total_Compensation",-1)).limit(1).iterator();

		try {
			while(cursor.hasNext()) {
				Document doc = cursor.next();
				System.out.println("Department Code: " + doc.get("Department_Code") + "\n" +
						"Employee ID: " + doc.get("Employee_Identifier") + "\n" +
						"Total Compensation: " + doc.get("Total_Compensation"));
			}
		}
		finally {
			cursor.close();
			mongo.close();
		}
	}

	private void highOvertime() {
		MongoClientURI connectionString = new MongoClientURI("mongodb://ec2-13-59-38-216.us-east-2.compute.amazonaws.com:27018");
		MongoClient mongo = new MongoClient(connectionString);
		MongoDatabase database = mongo.getDatabase("projectdb");
		MongoCollection<Document> collection = database.getCollection("sfdata");

		MongoCursor<Document> cursor = collection.find(and(gt("Overtime",1000),lt("Overtime",5000))).limit(1000).iterator();

		try {
			while(cursor.hasNext()) {
				Document doc = cursor.next();
				System.out.println("Employee ID: " + doc.get("Employee_Identifier") + "\n" +
						"Overtime: " + doc.get("Overtime") + "\n");
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
				+ "Version: 0.2\n\n"
				+ "Enter help for list of commands\n");

		Scanner s = new Scanner(System.in);

		while(true) {
			System.out.print("command> ");
			String input = "";
			if(s.hasNextLine()) {
				input = s.nextLine();
			}
			if(input.toLowerCase().trim().equals("help")) {
				System.out.println("exit - exit console\n");
				System.out.println("count - return the number of records on file\n");
				System.out.println("publicprotection - return employees from public protection from 2017-2018\n");
				System.out.println("deletesingle - delete a single employee by passing employee id\n");
				System.out.println("giveraise - give an employee a raise\n");
				System.out.println("insertsingle - Insert a new employee into the database\n");
				System.out.println("salaryasc - Get the department,job family, and salary if employees in ascending order by salary\n");
				System.out.println("retnhealth - Get retirment and health benefits of employees for 2010, 2015, and 2020\n");
				System.out.println("maxforyear - Get the employee with the highest compensation for a given year\n");
				System.out.println("otandother - Get sum of paid overtime and other salaries\n");
				System.out.println("healthforall - For a given year, give all employees with no health benefits 1000 in health benefits\n");
				System.out.println("totalcompforyear - For a given year, get the total of total compensations\n");
				System.out.println("overtimesum - For a given year, get the sum of overtime paid\n");
				System.out.println("updatesalary - Update an employees salary or other salary field to a given amount\n");
				System.out.println("maxforjob - For a given job, find the max total compensation\n");
				System.out.println("maxfordeptcode - For a given dept code, find the max total compensation\n");
				System.out.println("highovertime - Find employees with overtime between 1000-5000 exclusive\n");
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
			else if(input.toLowerCase().trim().equals("salaryasc")) {
				main.salaryAsc();
			}
			else if(input.toLowerCase().trim().equals("retnhealth")) {
				main.retirementAndHealth();
			}
			else if(input.toLowerCase().trim().equals("maxforyear")) {
				String year = "";
				System.out.println("Enter the year you wish to search (ex: 2019)");
				if(s.hasNextLine())
					year=s.nextLine();
				main.maxForYear(year);
			}
			else if(input.toLowerCase().trim().equals("otandother")) {
				main.otAndOther();
			}
			else if(input.toLowerCase().trim().equals("healthforall")) {
				System.out.println("Enter the year you wish to give benefits for");
				String year = "";
				if(s.hasNext())
					year = s.nextLine();
				main.healthForAll(year);
			}
			else if(input.toLowerCase().trim().equals("totalcompforyear")) {
				System.out.println("Enter the year you wish to see total compensation for");
				String year = "";
				if(s.hasNext())
					year = s.nextLine();
				main.totalCompForYear(year);
			}
			else if(input.toLowerCase().trim().equals("overtimesum")) {
				System.out.println("Enter the year you would like to see total overtime for");
				String year = "";
				if(s.hasNext())
					year = s.nextLine();
				main.overtimeSum(year);
			}
			else if(input.toLowerCase().trim().equals("updatesalary")) {
				String empid = "";
				String amount = "";
				String which = "";
				System.out.println("Enter the employee id you wish to update");
				if(s.hasNext())
					empid = s.nextLine();
				System.out.println("Enter the new salary");
				if(s.hasNext())
					amount = s.nextLine();
				System.out.println("Which field? Enter 1 for salaries or 2 for other salaries");
				if(s.hasNext()) {
					int choice = Integer.parseInt(s.nextLine());
					if(choice == 1)
						which = "Salaries";
					if(choice == 2)
						which = "Other_Salaries";
				}
				main.updateSalary(empid, amount, which);
			}
			else if(input.toLowerCase().trim().equals("maxforjob")) {
				String job = "";
				System.out.println("Enter the job to search");
				if(s.hasNext())
					job = s.nextLine();
				main.maxForJob(job);
			}
			else if(input.toLowerCase().trim().equals("maxfordeptcode")) {
				String deptCode = "";
				System.out.println("Enter the dept code to search");
				if(s.hasNext())
					deptCode = s.nextLine();
				main.maxForDeptCode(deptCode);
			}
			else if(input.toLowerCase().trim().equals("highovertime")) {
				main.highOvertime();
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
