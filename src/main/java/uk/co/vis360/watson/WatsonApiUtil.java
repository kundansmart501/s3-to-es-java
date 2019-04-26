package uk.co.vis360.watson;

import com.ibm.watson.developer_cloud.natural_language_understanding.v1.model.AnalysisResults;
import com.ibm.watson.developer_cloud.natural_language_understanding.v1.model.AnalyzeOptions;
import com.ibm.watson.developer_cloud.natural_language_understanding.v1.model.CategoriesOptions;
import com.ibm.watson.developer_cloud.natural_language_understanding.v1.model.ConceptsOptions;
import com.ibm.watson.developer_cloud.natural_language_understanding.v1.model.Features;

public class WatsonApiUtil {

	//TODO:Dynamic building based on the parameter provided
	public static AnalysisResults analyseContent(String text) {

		/*EntitiesOptions entitiesOptions = new EntitiesOptions.Builder()
				.emotion(true)
				.sentiment(true)
				.limit(2)
				.build();

		KeywordsOptions keywordsOptions = new KeywordsOptions.Builder()
				.emotion(true)
				.sentiment(true)
				.limit(2)
				.build();*/
		ConceptsOptions conceptsOptions = new ConceptsOptions.Builder()
				.limit(5)
				.build();

		CategoriesOptions categoriesOptions = new CategoriesOptions();

		Features features = new Features.Builder()
				.concepts(conceptsOptions)
				.categories(categoriesOptions)
				.build();

		AnalyzeOptions parameters = new AnalyzeOptions.Builder()
				.text(text)
				.features(features)
				.build();

		AnalysisResults response = LambdaFunctionHandler.service
				.analyze(parameters)
				.execute();
		return response;
	}

}
