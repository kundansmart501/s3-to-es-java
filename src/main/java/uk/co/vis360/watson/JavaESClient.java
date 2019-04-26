package uk.co.vis360.watson;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.amazonaws.services.lambda.runtime.Context;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.BulkResult;
import io.searchbox.core.Index;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.DeleteIndex;
import io.searchbox.indices.mapping.PutMapping;

public class JavaESClient {
	Logger log = Logger.getLogger(JavaESClient.class.getName());
	
	private static JestClientFactory factory = new JestClientFactory();
	private static JestClient jestClient;
	
	public JavaESClient(String ESEndpoint) {
		factory.setHttpClientConfig(new HttpClientConfig
				.Builder(ESEndpoint)
				.multiThreaded(true)
                .maxConnectionIdleTime(20000L, TimeUnit.MILLISECONDS)
                .readTimeout(40000)
                .maxTotalConnection(10)
                .defaultMaxTotalConnectionPerRoute(75)
                .build());
	}

	private static JestClientFactory getJestFactory() {
		return factory;
	}
	
	public static JestClient getJestClient() {
		if(jestClient!=null)
			return jestClient;
		jestClient = getJestFactory().getObject();
		return jestClient;
		
	}
	
	public void createIndex(String indexName,Context context) {
		try {
			getJestClient().execute(new DeleteIndex.Builder(indexName).build());
		} catch (IOException e1) {
			context.getLogger().log("Exception occured during createIndex "+e1);
		}
		try {
			getJestClient().execute(new CreateIndex.Builder(indexName).build());
			PutMapping putMapping = new PutMapping.Builder(
			        indexName,
			        "konfer-pub",
			        "\"konfer-pub\" : {\n" + 
			        "        \"dynamic\" : \"strict\",\n" + 
			        "        \"properties\" : {\n" + 
			        "          \"author\" : {\n" + 
			        "            \"type\" : \"text\",\n" + 
			        "            \"index\" : false\n" + 
			        "          },\n" + 
			        "          \"extractedContent\" : {\n" + 
			        "            \"type\" : \"text\",\n" + 
			        "            \"fields\" : {\n" + 
			        "              \"original_text\" : {\n" + 
			        "                \"type\" : \"text\",\n" + 
			        "                \"analyzer\" : \"standard\"\n" + 
			        "              }\n" + 
			        "            }\n" + 
			        "          },\n" + 
			        "          \"extractedContentHtml\" : {\n" + 
			        "            \"type\" : \"text\",\n" + 
			        "            \"fields\" : {\n" + 
			        "              \"original_text_html\" : {\n" + 
			        "                \"type\" : \"text\",\n" + 
			        "                \"index\" : false\n" + 
			        "              }\n" + 
			        "            }\n" + 
			        "          },\n" + 
			        "          \"fetchDate\" : {\n" + 
			        "            \"type\" : \"date\",\n" + 
			        "            \"format\" : \"dateOptionalTime\"\n" + 
			        "          },\n" + 
			        "          \"filteredCategories\" : {\n" + 
			        "            \"dynamic\" : \"false\",\n" + 
			        "            \"properties\" : {\n" + 
			        "              \"label\" : {\n" + 
			        "                \"type\" : \"text\"\n" + 
			        "              },\n" + 
			        "              \"score\" : {\n" + 
			        "                \"type\" : \"double\"\n" + 
			        "              }\n" + 
			        "            }\n" + 
			        "          },\n" + 
			        "          \"filteredConcepts\" : {\n" + 
			        "            \"type\" : \"nested\",\n" + 
			        "            \"dynamic\" : \"false\",\n" + 
			        "            \"properties\" : {\n" + 
			        "              \"relevance\" : {\n" + 
			        "                \"type\" : \"double\"\n" + 
			        "              },\n" + 
			        "              \"text\" : {\n" + 
			        "                \"type\" : \"text\"\n" + 
			        "              }\n" + 
			        "            }\n" + 
			        "          },\n" + 
			        "          \"institutions\" : {\n" + 
			        "            \"type\" : \"keyword\"\n" + 
			        "          },\n" + 
			        "          \"konferCategory\" : {\n" + 
			        "            \"properties\" : {\n" + 
			        "              \"category\" : {\n" + 
			        "                \"type\" : \"keyword\"\n" + 
			        "              },\n" + 
			        "              \"confidence\" : {\n" + 
			        "                \"type\" : \"double\"\n" + 
			        "              }\n" + 
			        "            }\n" + 
			        "          },\n" + 
			        "          \"locations\" : {\n" + 
			        "            \"type\" : \"nested\",\n" + 
			        "            \"properties\" : {\n" + 
			        "              \"coordinates\" : {\n" + 
			        "                \"type\" : \"geo_point\"\n" + 
			        "              },\n" + 
			        "              \"type\" : {\n" + 
			        "                \"type\" : \"text\"\n" + 
			        "              }\n" + 
			        "            }\n" + 
			        "          },\n" + 
			        "          \"mongoId\" : {\n" + 
			        "            \"type\" : \"keyword\"\n" + 
			        "          },\n" + 
			        "          \"originalUrl\" : {\n" + 
			        "            \"type\" : \"text\",\n" + 
			        "            \"index\" : false\n" + 
			        "          },\n" + 
			        "          \"publicationAbstract\" : {\n" + 
			        "            \"type\" : \"text\",\n" + 
			        "            \"index\" : false\n" + 
			        "          },\n" + 
			        "          \"publicationDate\" : {\n" + 
			        "            \"type\" : \"date\",\n" + 
			        "            \"format\" : \"dateOptionalTime\"\n" + 
			        "          },\n" + 
			        "          \"publicationYear\" : {\n" + 
			        "            \"type\" : \"integer\"\n" + 
			        "          },\n" + 
			        "          \"source\" : {\n" + 
			        "            \"properties\" : {\n" + 
			        "              \"fetchTime\" : {\n" + 
			        "                \"type\" : \"date\",\n" + 
			        "                \"index\" : false,\n" + 
			        "                \"format\" : \"dateOptionalTime\"\n" + 
			        "              },\n" + 
			        "              \"parentReference\" : {\n" + 
			        "                \"properties\" : {\n" + 
			        "                  \"collectionType\" : {\n" + 
			        "                    \"type\" : \"keyword\"\n" + 
			        "                  },\n" + 
			        "                  \"reference\" : {\n" + 
			        "                    \"type\" : \"keyword\"\n" + 
			        "                  },\n" + 
			        "                  \"source\" : {\n" + 
			        "                    \"type\" : \"text\",\n" + 
			        "                    \"index\" : false\n" + 
			        "                  }\n" + 
			        "                }\n" + 
			        "              },\n" + 
			        "              \"runId\" : {\n" + 
			        "                \"type\" : \"text\",\n" + 
			        "                \"index\" : false\n" + 
			        "              }\n" + 
			        "            }\n" + 
			        "          },\n" + 
			        "          \"themeId\" : {\n" + 
			        "            \"type\" : \"text\"\n" + 
			        "          },\n" + 
			        "          \"title\" : {\n" + 
			        "            \"type\" : \"text\",\n" + 
			        "            \"fields\" : {\n" + 
			        "              \"original_text\" : {\n" + 
			        "                \"type\" : \"text\",\n" + 
			        "                \"analyzer\" : \"standard\"\n" + 
			        "              }\n" + 
			        "            }\n" + 
			        "          },\n" + 
			        "          \"url\" : {\n" + 
			        "            \"type\" : \"text\",\n" + 
			        "            \"index\" : false\n" + 
			        "          }\n" + 
			        "        }\n" + 
			        "      }"
			).build();
			getJestClient().execute(putMapping);
		} catch (IOException e) {
			context.getLogger().log("Exception occured during createIndex execute "+e);
		}
	}

	public void updateBulkData(String indexName,String type,List<Index> indexes,int batch) {

		List<Index> subIndex = new ArrayList<Index>();
		int ctr = 0;
		//For the multiply of 10000
		for(int i=0;i<indexes.size();i+=batch) {
			if(i+batch>indexes.size()) 
				break;
			subIndex = indexes.subList(i, i+batch);
			updateBatchData(subIndex,indexName,type,getJestClient());
			ctr = i+batch;
		}

		//process remaining items
		subIndex = indexes.subList(ctr, indexes.size());
		updateBatchData(subIndex,indexName,type,getJestClient());
	}

	private void updateBatchData(List<Index> subIndex,String indexName,String type,JestClient client) {
		Bulk bulk  = new Bulk.Builder()
				.defaultIndex(indexName)
				.defaultType(type)
				.addAction(subIndex)
				.build();
		try {
			BulkResult result = client.execute(bulk);
			log.info("Total records updated :"+result.isSucceeded());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
