package uk.co.vis360.watson;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.ibm.watson.developer_cloud.natural_language_understanding.v1.model.AnalysisResults;
import com.ibm.watson.developer_cloud.natural_language_understanding.v1.model.ConceptsResult;

import io.searchbox.client.JestResult;
import io.searchbox.client.JestResultHandler;
import io.searchbox.core.Index;
import io.searchbox.core.MultiSearch;
import io.searchbox.core.MultiSearchResult;
import io.searchbox.core.Search;
import uk.co.vis360.constant.WatsonCategory;
import uk.co.vis360.util.OpenCSVWriter;
import uk.co.vis360.util.ReadJSON;

public class PublicationData {

	static Logger log = Logger.getLogger(PublicationData.class.getName());

	//private static final JavaESClient CLIENT_LOCAL  = new JavaESClient("http://18.202.16.202:9200/");

	//private static final JavaESClient CLIENT_TEST  = new JavaESClient("http://18.202.16.202:9200/");

	private static final JavaESClient CLIENT_TEST  = new JavaESClient("http://localhost:9200/");

	public static Search buildQuery(String index,String indexType) {

		JSONObject queryObject = new JSONObject();
		List<String>  sourceList = new ArrayList<String>();
		sourceList.add("url");
		sourceList.add("title");
		sourceList.add("institutions");
		sourceList.add("fetchDate");
		sourceList.add("extractedContent");
		queryObject.put("_source", sourceList);
		queryObject.put("size", 50000);

		List<String> universities = ReadJSON.collectUnivercityNames("konfer-university.json");
		JSONObject boolObject = new JSONObject();
		JSONObject shouldObject = new JSONObject();
		JSONArray shouldArray = new JSONArray();
		for(String university:universities) {
			JSONObject matchUniversityJSON = new JSONObject();
			JSONObject universityObject = new JSONObject();
			universityObject.put("institutions", university);
			matchUniversityJSON.put("match", universityObject);
			shouldArray.add(matchUniversityJSON);
		}
		shouldObject.put("should", shouldArray);
		boolObject.put("bool",shouldObject);
		queryObject.put("query",boolObject);

		String filterOrcid = "{\n" + 
				"				\"term\": {\n" + 
				"					\"source.parentReference.collectionType\": \"orcidBio\"\n" + 
				"				}\n" + 
				"			}";
		try {
			queryObject.put("filter", new JSONParser().parse(filterOrcid));
		} catch (ParseException e) {
			e.printStackTrace();
		}
		Search search = new Search.Builder(queryObject.toJSONString())
				// multiple index or types can be added.
				.addIndex(index)
				.addType(indexType)
				.setParameter("from", 100000)
				.build();
		return search;

	}	
	
	/**
	 * This method is used to build multisearch query. Currently we take the data from an excel fine manually
	 * and provide as hardcoded one by one.This will be automated if further works introduce
	 * @param index
	 * @param indexType
	 * @return MultiSearch object
	 */
	public static MultiSearch buildMultiSearchQuery(String index,String indexType) {

		String query = "{\n" + 
				"	\"post_filter\": {\n" + 
				"		\"term\": {\n" + 
				"			\"concepts.text\": \"wireless_sensor_network\"\n" + 
				"		}\n" + 
				"	},\n" + 
				"	\"size\": 10000,\n" + 
				"	\"_source\": [\"url\", \"title\", \"institutions\", \"extractedContent\"]\n" + 
				"}"; 

		String query2 = "{\n" + 
				"	\"query\": {\n" + 
				"		\"bool\": {\n" + 
				"			\"should\": [{\n" + 
				"				\"wildcard\": {\n" + 
				"					\"_all\": {\n" + 
				"						\"value\": \"internet of things\"\n" + 
				"					}\n" + 
				"				}\n" + 
				"			}, {\n" + 
				"				\"wildcard\": {\n" + 
				"					\"title\": {\n" + 
				"						\"value\": \"internet of things\",\n" + 
				"						\"boost\": 2\n" + 
				"					}\n" + 
				"				}\n" + 
				"			}, {\n" + 
				"				\"multi_match\": {\n" + 
				"					\"query\": \"internet of things\",\n" + 
				"					\"type\": \"phrase_prefix\",\n" + 
				"					\"fields\": [\"concepts^2\", \"categories^5\", \"title^10\"]\n" + 
				"				}\n" + 
				"			}]\n" + 
				"		}\n" + 
				"	},\n" + 
				"	\"size\": 10000,\n" + 
				"	\"_source\": [\"url\", \"title\", \"institutions\", \"extractedContent\", \"categories\", \"concepts\"]\n" + 
				"}"; 

		Search complexSearch = new Search.Builder(query).addIndex(index)
				.addType(indexType).build();
		Search complexSearch2 = new Search.Builder(query2).addIndex(index)
				.addType(indexType).build();
		
		MultiSearch multiSearch = new MultiSearch.Builder(Arrays.asList(complexSearch,complexSearch2))
				.build();

		return multiSearch;
	}

	public void executeMultiSearchRequest() {
		try {
			MultiSearchResult result = JavaESClient.getJestClient().execute(buildMultiSearchQuery("publication-ml-test","konfer-pub"));
			JsonObject resultObject = result.getJsonObject();
			JsonArray responses = (JsonArray) resultObject.get("responses");
			//log.info("Hits array "+responses.get(0));
			List<Map<String,String>> recordList = new ArrayList<Map<String,String>>();
			JSONArray universitiesLatLongs = ReadJSON.readJSONFile("university-lat-long.json");
			for(int i=0;i<responses.size();i++) {
				JsonObject responseObject = (JsonObject) responses.get(i);
				JsonObject hitsObject = (JsonObject) responseObject.get("hits");
				JsonArray hitsArray = (JsonArray) hitsObject.get("hits");
				log.info("hits array "+i+" size "+hitsArray.size());
				
				for(int j=0;j<hitsArray.size();j++) {
					Map<String,String> record = new HashMap<String,String>();
					JsonObject hitObject = (JsonObject) hitsArray.get(j);
					//log.info("score : "+hitObject.get("_score"));
					record.put("score", String.valueOf(hitObject.get("_score")));
					JsonObject publicationSource = (JsonObject)hitObject.get("_source");
					//log.info("Title : "+publicationSource.get("title"));
					record.put("title", String.valueOf(publicationSource.get("title")));
					record.put("extractedContent", String.valueOf(publicationSource.get("extractedContent")));
					record.put("url", String.valueOf(publicationSource.get("url")));
					JsonArray institutions = (JsonArray)publicationSource.get("institutions");
					record.put("university",String.valueOf(institutions.get(0)));
					record = getLatLongOfUniversity(String.valueOf(institutions.get(0)),record,universitiesLatLongs);
					recordList.add(record);
				}
			}
			log.info("Record size before de-duplicating "+recordList.size());
			recordList = new ArrayList<>(new HashSet<>(recordList));
			log.info("Record size after de-duplicating "+recordList.size());
			OpenCSVWriter.writeToCSV(recordList);

		} catch (IOException e) {
			log.info("Exception occured "+e);
			e.printStackTrace();
		}
	}
	
	public Map<String,String> getLatLongOfUniversity(String universityName,Map<String,String> record,JSONArray universitiesLatLongs){
		universityName = universityName.replaceAll("^\"|\"$", "");
		for(int i=0;i<universitiesLatLongs.size();i++) {
			JSONObject universityObject = (JSONObject)universitiesLatLongs.get(i);
			JSONObject universityLocation = (JSONObject)universityObject.get("location");
			String institutionName = String.valueOf(universityObject.get("institutionName"));
			if(universityName.replaceAll("_", " ").equalsIgnoreCase(institutionName)) {
				log.info("Co-ordinates "+String.valueOf(universityLocation.get("coordinates"))+" of university "+universityName);
				record.put("coordinates",String.valueOf(universityLocation.get("coordinates")));
				break;
			}
		}
		return record;
	}

	public void updateKonferPublication() {

		//Fetch data from TEST Konfer
		JavaESClient.getJestClient().executeAsync(PublicationData.buildQuery("ncub-dev","webPageContent"), new JestResultHandler<JestResult>() {
			@Override
			public void completed(JestResult result) {
				JsonObject resultObject = result.getJsonObject();
				JsonObject hits = (JsonObject) resultObject.get("hits");
				JSONArray hitsArray = new JSONArray();
				try {
					hitsArray = (JSONArray) new JSONParser().parse(hits.get("hits").toString());
					log.info("Total publications" +hitsArray.size());
				} catch (ParseException e) {
					e.printStackTrace();
				}
			}
			@Override
			public void failed(Exception ex) {
				System.out.println("The request has failed due to "+ex);
			}
		});
	}

	public void getDataFromES() {
		JavaESClient.getJestClient().executeAsync(PublicationData.buildQuery("ncub-dev","webPageContent"), new JestResultHandler<JestResult>() {
			@Override
			public void completed(JestResult result) {
				JsonObject resultObject = result.getJsonObject();
				JsonObject hits = (JsonObject) resultObject.get("hits");
				JSONArray hitsArray = new JSONArray();
				try {
					hitsArray = (JSONArray) new JSONParser().parse(hits.get("hits").toString());
					log.info("Total publications" +hitsArray.size());
				} catch (ParseException e) {
					e.printStackTrace();
				}
				updateWatsonPublication(hitsArray);
			}
			@Override
			public void failed(Exception ex) {
				System.out.println("The request has failed due to "+ex);
			}
		});
	}


	private void updateWatsonPublication(JSONArray hitsArray) {
		int publicationCtr = 0;
		log.info("Size "+hitsArray.size());
		List<Index> indexList = new ArrayList<Index>();
		for(Object obj:hitsArray) {
			JSONObject jsonObject = (JSONObject) obj;
			JSONObject source = (JSONObject) jsonObject.get("_source");
			AnalysisResults results = null;
			try {
				if(!isPublicationValid(String.valueOf(source.get("title")))) {
					log.info("Publication is not valid");
					continue;
				}

				publicationCtr++;
				if(!source.get("title").equals(""))
					results = analyseContent(String.valueOf(source.get("title")),String.valueOf(source.get("extractedContent")));
				List<ConceptsResult> conceptsResults = results.getConcepts();
				List<Map<String,String>> conceptList = buildConceptList(conceptsResults);
				source.put("concepts", conceptList);
				List<String> categories = buildCategories(results);
				Map<String,List<String>> laveledCategories = getLeveledCategories(categories);
				JSONObject categoryObject = new JSONObject();
				categoryObject.put("category.levl1",laveledCategories.get("level1"));categoryObject.put("category.levl2",laveledCategories.get("level2"));
				categoryObject.put("category.levl3",laveledCategories.get("level3"));categoryObject.put("category.levl4",laveledCategories.get("level4"));
				source.put("categories", categoryObject);
			}catch(Exception e) {
				log.info("Exception occured while processing on Watson "+e);
			}
			List<String> institutions = getInstitutions(String.valueOf(source.get("institutions")));
			source.put("institutions",institutions);
			indexList.add(new Index.Builder(source).build());
			if(indexList.size()==1000) {
				log.info("Updating 1000 sets of data with publication counter "+publicationCtr);
				CLIENT_TEST.updateBulkData("publication-ml-test", "konfer-pub", indexList,100);
				indexList.clear();
			}
		}
		log.info("Going to persist remaining data "+indexList.size());
		CLIENT_TEST.updateBulkData("publication-ml-test", "konfer-pub", indexList,100);
	}

	private List<String> buildCategories(AnalysisResults results){
		List<String> categories = results.getCategories().stream()
				.filter(v->v.getScore()>0.2 && !(v.getLabel().contains(WatsonCategory.ART_AND_ENTERTAINMENT.toString()) || 
						v.getLabel().contains(WatsonCategory.HOME_AND_GARDEN.toString())||
						v.getLabel().contains(WatsonCategory.SHOPPING.toString())|| 
						v.getLabel().contains(WatsonCategory.TECHNOLOGY_AND_COMPUTING.toString()) || 
						v.getLabel().contains(WatsonCategory.STYLE_AND_FASHION.toString()) || 
						v.getLabel().contains(WatsonCategory.SPORTS.toString())))
				.map(v->v.getLabel())
				.limit(1)
				.collect(Collectors.toList());
		return categories;
	}

	private List<Map<String,String>> buildConceptList(List<ConceptsResult> conceptsResults) {
		List<Map<String,String>> conceptList = new ArrayList<Map<String,String>>();
		for(ConceptsResult conceptResult:conceptsResults) {
			JSONObject conceptObect = new JSONObject();
			String text = conceptResult.getText().trim();
			if(text.endsWith(" ")) 
				text = text.substring(1, text.length());
			if(text.startsWith(" ")) 
				text = text.substring(0,text.length()-1);
			String conceptText = conceptResult.getText().replaceAll(" ", "_").replaceAll(",", "").replaceAll("&", "and");
			conceptObect.put("text",conceptText);
			Double doubleValue = conceptResult.getRelevance()*10;
			conceptObect.put("relevance",Math.round(doubleValue));
			conceptList.add(conceptObect);
		}
		return conceptList;
	}

	private List<String> getInstitutions(String institutions) {
		String replaceBracket = institutions.replaceAll("^\\[|]$", "");
		replaceBracket = replaceBracket.replaceAll("^\"|\"$", "");
		String[] institutionArray = replaceBracket.split(",");
		List<String> al = new ArrayList<String>();
		al = Arrays.asList(institutionArray);
		List<String> newInstituteList = new ArrayList<String>(); 
		for(String institute:al) {
			newInstituteList.add(institute.replaceAll(" ", "_"));
		}
		return newInstituteList;
	}

	public static AnalysisResults analyseContent(String title, String publicationAbstract) {
		String analyseRawData = "";
		//remove title suffix ": Sussex Research Online"
		if(title.contains(": Sussex Research Online"))
			title = title.substring(0, title.indexOf(": Sussex Research Online"));

		if(title.contains("Research Explorer : Aston University")) {
			title = title.substring(0, title.indexOf("Research Explorer : Aston University"));
			log.info("Removing last part from Aston::::::::::");
		}
		analyseRawData = title;
		//If the extractedContent field includes string "Full text not available from this repository" AND extractedContent does not include string "Abstract/n" then only pass title to Watson categories APIc. Where "Abstract/n" is in extractedContent then only pass text to Watson following "extractedContent" field
		if(!publicationAbstract.contains("Full text not available from this repository")) {
			if(publicationAbstract.contains("Abstract")) {		
				publicationAbstract = publicationAbstract.substring(publicationAbstract.indexOf("Abstract")+8, publicationAbstract.length());
				analyseRawData = getAbstract(analyseRawData,title,publicationAbstract);

			}
		}else if(publicationAbstract.contains("Abstract")) {
			publicationAbstract = publicationAbstract.substring(publicationAbstract.indexOf("Abstract")+8, publicationAbstract.length());
			analyseRawData = getAbstract(analyseRawData,title,publicationAbstract);
		}
		if(analyseRawData.contains("Copyright")) 
			analyseRawData = analyseRawData.substring(0, analyseRawData.indexOf("Copyright"));
		if(analyseRawData.contains("©"))
			analyseRawData = analyseRawData.substring(0, analyseRawData.indexOf("©"));
		log.info("Conetnt to analyse "+ analyseRawData);
		return WatsonApiUtil.analyseContent(analyseRawData);
	}

	private static String getAbstract(String analyseRawData ,String title,String publicationAbstract) {
		String[] publcationAbstractArray = publicationAbstract.split("\\.");
		if(publcationAbstractArray.length>=3) {
			analyseRawData = title+"."+System.lineSeparator()+publcationAbstractArray[0]+"."+publcationAbstractArray[1]+"."+publcationAbstractArray[2];
		}else {
			analyseRawData = title+"."+System.lineSeparator()+publicationAbstract;
		}
		return analyseRawData.replaceAll("Item Type:", "");
	}

	private Map<String,List<String>> getLeveledCategories(List<String> categories){
		Map<String,List<String>> leveledCategories = new HashMap<String,List<String>>();
		List<String> level1 = new ArrayList<String>();
		List<String> level2 = new ArrayList<String>();
		List<String> level3 = new ArrayList<String>();
		List<String> level4 = new ArrayList<String>();

		for(String categoryLable:categories) {
			categoryLable = categoryLable.replaceAll(",", " and");
			String[] allCategories = categoryLable.trim().split("/");
			if(allCategories[1].startsWith(" ")) 
				allCategories[1] = allCategories[1].substring(1, allCategories[1].length());
			level1.add(allCategories[1].replaceAll(" ", "_"));
			if(allCategories.length>=3) 
				level2.add(allCategories[2].replaceAll(" ", "_"));
			if(allCategories.length>=4) 
				level3.add(allCategories[3].replaceAll(" ", "_"));
			if(allCategories.length>=5)
				level4.add(allCategories[4].replaceAll(" ", "_"));
		}
		leveledCategories.put("level1", level1);
		leveledCategories.put("level2", level2);
		leveledCategories.put("level3", level3);
		leveledCategories.put("level4", level4);
		return leveledCategories;
	}

	private boolean isPublicationValid(String title) {
		return !title.contains("People Search");
	}
}


