package com.amazonaws.lambda.vis360;

import static org.mockito.Mockito.when;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;

import io.searchbox.core.Index;

/**
 * A simple test harness for locally invoking your Lambda function handler.
 */
@RunWith(MockitoJUnitRunner.class)
public class LambdaFunctionHandlerTest {

    private final String CONTENT_TYPE = "image/jpeg";
    private S3Event event;
    private JavaESClient javaESClient = new JavaESClient("https://search-vm-search-service-pqzlp4xq23yqvr5yudnfis6f2u.eu-west-1.es.amazonaws.com");
    
    @Mock
    private AmazonS3 s3Client;
    @Mock
    private S3Object s3Object;

    @Captor
    private ArgumentCaptor<GetObjectRequest> getObjectRequest;

    @Before
    public void setUp() throws IOException {
        event = TestUtils.parse("/s3-event.put.json", S3Event.class);

        // TODO: customize your mock logic for s3 client
        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setContentType(CONTENT_TYPE);
        when(s3Object.getObjectMetadata()).thenReturn(objectMetadata);
        when(s3Client.getObject(getObjectRequest.capture())).thenReturn(s3Object);
    }

    private Context createContext() {
        TestContext ctx = new TestContext();

        // TODO: customize your context here if needed.
        ctx.setFunctionName("Your Function Name");

        return ctx;
    }

    @Test
    @Ignore
    public void testLambdaFunctionHandler() {
        LambdaFunctionHandler handler = new LambdaFunctionHandler(s3Client);
        Context ctx = createContext();

        String output = handler.handleRequest(event, ctx);

        // TODO: validate output here if needed.
        Assert.assertEquals(CONTENT_TYPE, output);
    }
    
    @Test
    @Ignore
    public void doBulkUpdate() {
    	JSONParser parser = new JSONParser();
    	JSONArray array = new JSONArray();
    	List<Index> indexList = new ArrayList<Index>();
    	try {
			array = (JSONArray) parser.parse(new FileReader("/home/kundan/test-search-data.json"));
		} catch (IOException | ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	/*for(Object obj:array) {
    		 JSONObject jsonObject = (JSONObject) obj;
    		 indexList.add(new Index.Builder(jsonObject.toJSONString()).build());
    	}
    	javaESClient.createIndex("movies");
    	javaESClient.updateBulkData("movies",indexList);*/
    }
}
