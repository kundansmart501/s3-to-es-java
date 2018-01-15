package com.amazonaws.lambda.vis360;

import java.io.IOException;
import java.util.List;

import com.amazonaws.services.lambda.runtime.Context;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Index;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.DeleteIndex;

public class JavaESClient {
	private static JestClientFactory factory = new JestClientFactory();

	public JavaESClient(String ESEndpoint) {
		factory.setHttpClientConfig(new HttpClientConfig
				.Builder(ESEndpoint)
				.multiThreaded(true)
				//Per default this implementation will create no more than 2 concurrent connections per given route
				.defaultMaxTotalConnectionPerRoute(2)
				// and no more 20 connections in total
				.maxTotalConnection(5)
				.build());
	}

	public void createIndex(String indexName,Context context) {
		JestClient client = factory.getObject();
		try {
			client.execute(new DeleteIndex.Builder(indexName).build());
		} catch (IOException e1) {
			context.getLogger().log("Exception occured during createIndex "+e1);
		}
		try {
			client.execute(new CreateIndex.Builder(indexName).build());
		} catch (IOException e) {
			context.getLogger().log("Exception occured during createIndex execute "+e);
		}
	}

	public void updateBulkData(String indexName,String type,List<Index> indexes,Context context) {
		JestClient client = factory.getObject();
		Bulk bulk = new Bulk.Builder()
				.defaultIndex(indexName)
				.defaultType(type)
				.addAction(indexes)
				.build();
		try {
			client.execute(bulk);
		} catch (IOException e) {
			context.getLogger().log("Exception occured during bulk update "+e);
		}
	}
}
