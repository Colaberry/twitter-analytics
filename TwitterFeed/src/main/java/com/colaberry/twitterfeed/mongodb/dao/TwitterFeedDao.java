package com.colaberry.twitterfeed.mongodb.dao;

import java.net.UnknownHostException;
import java.util.Date;

import twitter4j.Status;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;

public class TwitterFeedDao {
	/**Method to Get the Connection from the database*/
	public static DBCollection getConnection(String dbName, String collectionName)throws UnknownHostException {

		/** Connecting to MongoDB */
		MongoClient mongo = new MongoClient("localhost", 27017);

		/**Gets database, incase if the database is not existing
	             MongoDB Creates it for you*/
		DB db = mongo.getDB(dbName);

		/**Gets collection / table from database specified if
	             collection doesn't exists, MongoDB will create it for
	             you*/
		DBCollection table = db.getCollection(collectionName);
		return table;
	}
	/**Method to insert data*/
	public static void insertData(String dbName, String collectionName,Status status) throws UnknownHostException{
		/**Connecting to MongoDB*/
		DBCollection table =TwitterFeedDao.getConnection(dbName, collectionName);
		BasicDBObject document = new BasicDBObject();
		document.put("userName",status.getUser().getScreenName());
		document.put("text",status.getText());
		table.insert(document);
	}
	/**Method to find data*/
	public static void findData(String dbName, String collectionName)throws UnknownHostException{
		DBCollection table =TwitterFeedDao.getConnection(dbName, collectionName);
		BasicDBObject searchQuery = new BasicDBObject();
		//searchQuery.put("userName", "java");
		//DBCursor cursor = table.find(searchQuery);
		DBCursor cursor = table.find();
		while (cursor.hasNext()) {
			System.out.println(cursor.next());
		}
	}
	/**Method to remove data*/
	public static void removeData(String dbName, String collectionName)throws UnknownHostException{
		DBCollection table =TwitterFeedDao.getConnection(dbName, collectionName);
		BasicDBObject document = new BasicDBObject();
		// Delete All documents from collection Using blank BasicDBObject
		table.remove(document);

	}

}
