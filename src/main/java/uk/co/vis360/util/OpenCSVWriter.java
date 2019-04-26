package uk.co.vis360.util;

import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.opencsv.CSVWriter;
/**
 * This class is used to write the records in CSV
 * @author kundan
 *
 */
public class OpenCSVWriter{

	public static void writeToCSV(List<Map<String,String>> pubs ) {
		//StringWriter writer = new StringWriter();
		Writer writer ;
		try {
			writer = Files.newBufferedWriter(Paths.get("./thema-results/networking.csv"));
			//using custom delimiter and quote character
			CSVWriter csvWriter = new CSVWriter(writer);

			List<String[]> data = toStringArray(pubs);

			csvWriter.writeAll(data);

			try {
				csvWriter.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		

	}

	private static List<String[]> toStringArray(List<Map<String,String>> pubs) {
		List<String[]> records = new ArrayList<String[]>();

		// adding header row
		records.add(new String[] { "University", "Title", "Abstract", "Relevance Score" ,"Publication URL","Coordinates"});

		Iterator<Map<String,String>> it = pubs.iterator();
		while (it.hasNext()) {
			Map<String,String> publication = it.next();
			records.add(new String[] { publication.get("university"),publication.get("title"),publication.get("extractedContent"),
					publication.get("score"),publication.get("url"), publication.get("coordinates")});
		}
		return records;
	}
}
