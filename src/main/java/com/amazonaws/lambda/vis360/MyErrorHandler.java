package com.amazonaws.lambda.vis360;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.http.HttpResponseHandler;

public class MyErrorHandler implements HttpResponseHandler<AmazonServiceException> {
	@Override
	public AmazonServiceException handle(com.amazonaws.http.HttpResponse response) throws
	Exception {
		AmazonServiceException ase = new AmazonServiceException("");
		ase.setStatusCode(response.getStatusCode());
		ase.setErrorCode(response.getStatusText());
		return ase;
	}
	@Override
	public boolean needsConnectionLeftOpen() {
		return false;
	}
}