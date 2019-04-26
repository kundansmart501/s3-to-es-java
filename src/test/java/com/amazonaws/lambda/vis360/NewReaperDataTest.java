package com.amazonaws.lambda.vis360;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import uk.co.vis360.mongo.NewReaperData;


public class NewReaperDataTest {

	@Test
	public void testPublicationCount() {
		//System.out.println("publication count = "+new NewReaperData().getPublicationCount());
		assertEquals(new NewReaperData().getPublicationCount(),949239);
	}

}
