package uk.co.vis360.mongo;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.bson.Document;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

import io.searchbox.core.Index;
import uk.co.vis360.watson.JavaESClient;
/**
 * This class is intended to process the harvested data and store it in ES for searchkit.
 * @author Kundan Ray
 * 
 */
public class NewReaperData {
	private static final Logger log = Logger.getLogger(NewReaperData.class.getName());

	//private static final JavaESClient CLIENT_TEST  = new JavaESClient("http://localhost:9200/");
	//private static JavaESClient CLIENT_PROD = new JavaESClient("http://18.202.16.202:9200/");
	private static List<Index> indexList = new ArrayList<Index>();

	private static MongoDatabase database;

	public static MongoDatabase getConnection (String URL,String dbName){
		MongoClientURI uri = new MongoClientURI(URL);
		MongoClient mongoClient = new MongoClient(uri);
		database = mongoClient.getDatabase(dbName);
		return database;
	}
	/**
	 * Fetch data from mongodb and store it in ES
	 * @param skip
	 * @param limit
	 */
	public static void fetchAndUpdateData(int skip, int limit) {
		//CLIENT_TEST.createIndex("publication-ml-prod-v1", null);
		if(database==null)
			getConnection("mongodb+srv://app-user:evPDczilHwXQIBkg@konfer-prod-wrzju.mongodb.net","ncub-dev");
		MongoCollection<Document> collectionNew = database.getCollection("webPageContent");
		BasicDBObject whereQuery = new BasicDBObject();
		whereQuery.put("source.parentReference.collectionType", "orcid_bio");
		FindIterable<Document> fi = collectionNew.find(whereQuery).skip(skip).limit(limit);
		MongoCursor<Document> cursor = fi.iterator();
		try {
			int ctr = 0;
			while(cursor.hasNext()) {
				JSONParser parser = new JSONParser(); 
				try {
					JSONObject json = (JSONObject) parser.parse(cursor.next().toJson());
					buildAndAddPublication(json);
				} catch (ParseException e) {
					e.printStackTrace();
				}
				log.info("counter "+(++ctr));
			}
			try {
				JavaESClient CLIENT_PROD = new JavaESClient("http://18.202.16.202:9200/");
				CLIENT_PROD.updateBulkData("publication-ml-prod-v1", "konfer-pub", indexList,100);
				indexList.clear();
			}catch(Exception e) {
				log.info("Error occured while updating bulk data to ES "+e);
			}
		} finally {
			cursor.close();
		}


	}

	private static void buildAndAddPublication(JSONObject publicationRecord) {
		List<JSONObject> conceptsResults = (List<JSONObject>) publicationRecord.get("filteredConcepts");
		if(conceptsResults==null) 
			return;
		List<Map<String,String>> conceptList = buildConceptList(conceptsResults);
		publicationRecord.put("searchkit-concepts", conceptList);
		//Collect categories
		//TODO:TO confirm to ignore
		List<JSONObject> watsonCategories = (List<JSONObject>)publicationRecord.get("watsonCategories");
		if(watsonCategories==null)
			return;
		String category = buildCategories(watsonCategories);
		Map<String,List<String>> laveledCategories = getLeveledCategories(category);
		JSONObject categoryObject = new JSONObject();
		categoryObject.put("category.levl1",laveledCategories.get("level1"));categoryObject.put("category.levl2",laveledCategories.get("level2"));
		categoryObject.put("category.levl3",laveledCategories.get("level3"));categoryObject.put("category.levl4",laveledCategories.get("level4"));
		publicationRecord.put("searchkit-categories", categoryObject);
		//Collect Konfer Categories
		JSONObject konferCategories = (JSONObject)publicationRecord.get("konferCategory");
		String categoryName = "";
		if(konferCategories!=null) {
			if(konferCategories.get("category")!=null) 
				categoryName = String.valueOf(konferCategories.get("category")).replaceAll(" ", "_");
			konferCategories.put("category",categoryName.replaceAll(",", "_and_").replaceAll("/", "_and_"));
			publicationRecord.put("searchkit-konfer-categories", konferCategories);
		}else {
			konferCategories = new JSONObject();
			konferCategories.put("category","unclassified");
			if(publicationRecord!=null)
				publicationRecord.put("searchkit-konfer-categories", konferCategories);
		}
		//Topic
		JSONObject filteredCategories = (JSONObject)publicationRecord.get("filteredCategories");
		if(filteredCategories!=null) {
			JSONObject topic = buildTopic(filteredCategories);
			publicationRecord.put("topic", topic);
		}else {
			publicationRecord.put("topic", new JSONObject());
		}
		//Get the publication year
		JSONObject publicationDate = (JSONObject)publicationRecord.get("publicationDate");
		if(publicationDate!=null) {
			if(publicationDate.get("$date")!=null) {
				LocalDate dateTime =
						Instant.ofEpochMilli(Long.parseLong(publicationDate.get("$date").toString())).atZone(ZoneId.systemDefault()).toLocalDate();
				publicationRecord.put("publicationYear", dateTime.getYear());
			}
		}
		List<String> institutions = getInstitutions(publicationRecord.get("institutions").toString());
		publicationRecord.put("searchkit-institutions",institutions);
		indexList.add(new Index.Builder(publicationRecord).build());
	}

	private static JSONObject buildTopic(JSONObject filteredCategories) {
		JSONObject topic = new JSONObject();
		if(filteredCategories.get("label")!=null) {
			String categoryName = filteredCategories.get("label").toString();
			String[] categories = categoryName.split("/");
			categoryName = categories[categories.length-1].replaceAll(" ", "_");
			topic.put("label", categoryName);
		}
		return topic;
	}

	private static String buildCategories(List<JSONObject> watsonCategories){
		String category = "";

		for(int i=0;i<watsonCategories.size();i++) {
			JSONObject categoryObject = watsonCategories.get(i);
			if(Float.parseFloat(categoryObject.get("score").toString())>0.2) {
				category = categoryObject.get("label").toString();
				break;
			}
		}
		return category;
	}


	private static Map<String,List<String>> getLeveledCategories(String categories){
		Map<String,List<String>> leveledCategories = new HashMap<String,List<String>>();
		List<String> level1 = new ArrayList<String>();
		List<String> level2 = new ArrayList<String>();
		List<String> level3 = new ArrayList<String>();
		List<String> level4 = new ArrayList<String>();
		categories = categories.replaceAll(",", " and");
		String[] allCategories;

		allCategories = categories.trim().split("/");

		if(allCategories.length>=2) {
			if(allCategories[1].startsWith(" ")) 
				allCategories[1] = allCategories[1].substring(1, allCategories[1].length());
			level1.add(allCategories[1].replaceAll(" ", "_"));
		}
		if(allCategories.length>=3) 
			level2.add(allCategories[2].replaceAll(" ", "_"));
		if(allCategories.length>=4) 
			level3.add(allCategories[3].replaceAll(" ", "_"));
		if(allCategories.length>=5)
			level4.add(allCategories[4].replaceAll(" ", "_"));
		leveledCategories.put("level1", level1);
		leveledCategories.put("level2", level2);
		leveledCategories.put("level3", level3);
		leveledCategories.put("level4", level4);
		return leveledCategories;
	}

	private static List<Map<String,String>> buildConceptList(List<JSONObject> conceptsResults) {
		List<Map<String,String>> conceptList = new ArrayList<Map<String,String>>();
		for(JSONObject conceptResult:conceptsResults) {
			JSONObject conceptObect = new JSONObject();
			String text = conceptResult.get("text").toString().trim();
			if(text.endsWith(" ")) 
				text = text.substring(1, text.length());
			if(text.startsWith(" ")) 
				text = text.substring(0,text.length()-1);
			String conceptText = conceptResult.get("text").toString().toString().replaceAll(" ", "_").replaceAll(",", "").replaceAll("&", "and");
			conceptObect.put("text",conceptText);
			Double doubleValue = (Double)conceptResult.get("relevance")*10;
			conceptObect.put("relevance",Math.round(doubleValue));
			conceptList.add(conceptObect);
		}
		return conceptList;
	}

	private static List<String> getInstitutions(String institutions) {
		String replaceBracket = institutions.replaceAll("^\\[|]$", "");
		replaceBracket = replaceBracket.replaceAll("^\"|\"$", "");
		String[] institutionArray = replaceBracket.split(",");
		List<String> al = new ArrayList<String>();
		al = Arrays.asList(institutionArray[0]);
		List<String> newInstituteList = new ArrayList<String>(); 
		for(String institute:al) {
			newInstituteList.add(institute.replaceAll(" ", "_"));
		}
		return newInstituteList;
	}

	public void processData(int skip, int limit) {
		fetchAndUpdateData(skip,limit);
	}
	
	public long getPublicationCount() {
		if(database==null)
			getConnection("mongodb+srv://app-user:evPDczilHwXQIBkg@konfer-prod-wrzju.mongodb.net","ncub-dev");
		
		MongoCollection<Document> collectionNew = database.getCollection("webPageContent");
		//BasicDBObject whereQuery = new BasicDBObject();
		//whereQuery.put("source.parentReference.collectionType", "orcid_bio");
		
		log.info("Total publication count=" +collectionNew.count());
		
		return collectionNew.count();
	}
}
