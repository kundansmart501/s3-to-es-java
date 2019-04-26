package uk.co.vis360.watson;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.ibm.watson.developer_cloud.natural_language_understanding.v1.NaturalLanguageUnderstanding;

public class LambdaFunctionHandler implements RequestHandler<Object, String> {

	private static final String INDEX_NAME = "publication";
	
	private static final String INDEX_TYPE = "konfer-pub";
	
	public static NaturalLanguageUnderstanding service;
	
	public static final String DEFAULT_API_ENDPOINT = "https://gateway.watsonplatform.net/natural-language-understanding/api"; 

	static {
		service = new NaturalLanguageUnderstanding(
				"2018-08-13",
				"0c0523a8-858e-4ee3-a9c0-45d72d22a99a",
				"ZfKYcA33XyS6"
				);
		service.setEndPoint(DEFAULT_API_ENDPOINT);
	}

	@Override
	public String handleRequest(Object input, Context context) {
		context.getLogger().log("Input: " + input);
		String text = "IBM is an American multinational technology " +
				"company headquartered in Armonk, New York, " +
				"United States, with operations in over 170 countries.";
		//AnalysisResults results = WatsonApiUtil.analyseContent(text,context);
		
		
		// TODO: implement your handler
		return "Hello from Lambda!";
	}

}