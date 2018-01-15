package com.amazonaws.lambda.vis360;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

import io.searchbox.core.Index;

public class LambdaFunctionHandler implements RequestHandler<S3Event, String> {

	private AmazonS3 s3 = AmazonS3ClientBuilder.standard().build();
	
	private static final String INDEX_NAME = "titlesMetadata";
	
	private static final String INDEX_TYPE = "title";

	public LambdaFunctionHandler() {}

	// Test purpose only.
	LambdaFunctionHandler(AmazonS3 s3) {
		this.s3 = s3;
	}

	@Override
	public String handleRequest(S3Event event, Context context) {
		context.getLogger().log("Received event: " + event);
		context.getLogger().log("ES_ENDPOINT : " + System.getenv("ES_ENDPOINT"));
		// Get the object from the event and show its content type
		String bucket = event.getRecords().get(0).getS3().getBucket().getName();
		String key = event.getRecords().get(0).getS3().getObject().getKey();
		try {
			S3Object response = s3.getObject(new GetObjectRequest(bucket, key));
			displayTextInputStream(response.getObjectContent(),context);
			//context.getLogger().log("s3 data : " + response.getObjectContent());
			String contentType = response.getObjectMetadata().getContentType();
			context.getLogger().log("CONTENT TYPE: " + contentType);
			return contentType;
		} catch (Exception e) {
			e.printStackTrace();
			context.getLogger().log(String.format(
					"Error getting object %s from bucket %s. Make sure they exist and"
							+ " your bucket is in the same region as this function.", key, bucket));
			throw e;
		}
	}

	private void displayTextInputStream(InputStream input,Context context) {
		// Read one text line at a time and display.
		BufferedReader reader = new BufferedReader(new 
				InputStreamReader(input));
		List<Index> indexList = new ArrayList<Index>();
		StringBuilder sb = new StringBuilder();
		try {
			while (true) {
				String line = reader.readLine();
				if (line == null) break;

				context.getLogger().log("    " + line);
				sb.append(line);
			}
			reader.close();
		}catch(IOException ex) {
			context.getLogger().log(String.format(
					"Error getting object %s from bucket %s. Make sure they exist and"
							+ " your bucket is in the same region as this function."));
		}
		JSONParser parser = new JSONParser();
		JSONArray jsonArray = new JSONArray();
		try {
			jsonArray = (JSONArray) parser.parse(sb.toString());
			for(Object obj:jsonArray) {
				JSONObject jsonObject = (JSONObject)obj;
				indexList.add(new Index.Builder(jsonObject.toJSONString()).build());
				//context.getLogger().log(jsonObject.toJSONString());
			}
		} catch (ParseException e) {
			context.getLogger().log("Exception occured "+e);
		}
		JavaESClient jesClient = new JavaESClient(System.getenv("ES_ENDPOINT"));
		jesClient.createIndex(INDEX_NAME,context);
		jesClient.updateBulkData(INDEX_NAME,INDEX_TYPE,indexList,context);
		context.getLogger().log("Index updated ");
	}
}