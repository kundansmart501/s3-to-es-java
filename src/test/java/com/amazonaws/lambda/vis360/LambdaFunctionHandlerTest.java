package com.amazonaws.lambda.vis360;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import com.amazonaws.services.lambda.runtime.Context;

import io.searchbox.core.SearchResult;
import uk.co.vis360.mongo.NewReaperData;
import uk.co.vis360.util.ReadJSON;
import uk.co.vis360.watson.JavaESClient;
import uk.co.vis360.watson.LambdaFunctionHandler;
import uk.co.vis360.watson.PublicationData;

/**
 * A simple test harness for locally invoking your Lambda function handler.
 */
@RunWith(MockitoJUnitRunner.class)
public class LambdaFunctionHandlerTest {
	
	private static final JavaESClient CLIENT_LOCAL  = new JavaESClient("http://localhost:9200/");
	private static Object input;

	@BeforeClass
	public static void createInput() throws IOException {
		// TODO: set up your sample input object here.
		input = null;
	}

	private Context createContext() {
		TestContext ctx = new TestContext();

		// TODO: customize your context here if needed.
		ctx.setFunctionName("LambdaFunctionHandler");

		return ctx;
	}

	@Test
	@Ignore
	public void testLambdaFunctionHandler() {
		LambdaFunctionHandler handler = new LambdaFunctionHandler();
		Context ctx = createContext();

		String output = handler.handleRequest(input, ctx);

		// TODO: validate output here if needed.
		Assert.assertEquals("Hello from Lambda!", output);
	}

	@Test
	@Ignore
	public void updatePublication() {
		TestContext ctx = new TestContext();
		PublicationData sk = new PublicationData();
		sk.updateKonferPublication();
	}

	@Test
	@Ignore
	public void updateWatsonPublication() {
		TestContext ctx = new TestContext();
		PublicationData sk = new PublicationData();
		sk.getDataFromES();
	}
	
	@Test
	@Ignore
	public void analyseData() {
		Context ctx = createContext();
		String pubAbstract = "Black is a Country\\nNext Section\\nAbstract\\nRacism transcends borders and so too must the fight against it, argues Kehinde Andrews. Too often, analyses of race are hemmed in by “methodological nationalism,” or the tendency to frame our thinking around the nation-state. Instead, Andrews says, the African diaspora should unite across oceans and boundaries to form a country based on freedom and equality for Black populations.\\nMarcus Garvey\\nMARION S. TRIKOSKO\\nBirmingham, England—Racism transcends the boundaries of the nation-state, and so the fight for freedom and equality must also be global.\\nToo often when we try to understand anti-Black racism, our analyses are limited to our own country’s borders. The late sociologist Herminio Martins dubbed this tendency to frame our thinking within the nation-state “methodological nationalism.” In thinking about racism, the nation-state is frequently considered a real, tangible unit of study for racial formation and inequalities.                   In reality, the nation-state is no more solid a concept than race; they’re social constructions rooted in myth and produced                   by powerful ideologies.\\nUndergirding Western capitalism is a global system of racism: The genocide of natives in the Americas, transatlantic enslavement                   of Africans, and colonial and neo-colonial domination were all transnational oppressions. Despite these international origins                   of systemic racism, civil rights analysis and politics remain inscribed within a country’s borders.";
		PublicationData.analyseContent("Black is a Country", pubAbstract);
		//ctx.getLogger().log(""+WatsonApiUtil.analyseContent(rawData,ctx));
	}
	
	@Test
	@Ignore
	public void getData() {
		try {
			SearchResult result = JavaESClient.getJestClient().execute(
					PublicationData.buildQuery("publication","konfer-pub"));
			System.out.println(result.getJsonString());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	@Test
	@Ignore
	public void readJsonFile() {
		ReadJSON.readJSONFile("konfer-university.json");
	}
	
	@Test
	@Ignore
	public void processNewReaperData() {
		new NewReaperData().processData(0,0);
		assertEquals(1,1);
	}
}
