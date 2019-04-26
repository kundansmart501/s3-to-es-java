package uk.co.vis360.util;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class ReadJSON {

	public static JSONArray readJSONFile(String fileName) {
		JSONParser jsonParser = new JSONParser();
		JSONArray universities = new JSONArray();
		try (FileReader reader = new FileReader(fileName))
		{
			//Read JSON file
			Object obj = jsonParser.parse(reader);

			universities = (JSONArray) obj;
			System.out.println("Total universitties "+universities.size());
			System.out.println("University name "+universities.get(0));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return universities;
	}
	
	public static List<String> collectUnivercityNames(String fileName){
		JSONArray universities = readJSONFile(fileName);
		List<String> universityList = new ArrayList<String>();
		for(int i=0;i<universities.size();i++) {
			JSONObject jsonObj = (JSONObject) universities.get(i);
			universityList.add(String.valueOf(jsonObj.get("key")));
		}
		return universityList;
	}
	
	public static List<String> collectLatLongs(String fileName){
		JSONArray universities = readJSONFile(fileName);
		List<String> universityList = new ArrayList<String>();
		for(int i=0;i<universities.size();i++) {
			JSONObject jsonObj = (JSONObject) universities.get(i);
			universityList.add(String.valueOf(jsonObj.get("key")));
		}
		return universityList;
	}
}
