package com.amazonaws.lambda.vis360;

import java.io.IOException;

import com.amazonaws.AmazonWebServiceResponse;
import com.amazonaws.http.HttpResponseHandler;
import com.amazonaws.util.IOUtils;

public class MyHttpResponseHandler implements HttpResponseHandler<AmazonWebServiceResponse<String>> {

    @Override
    public AmazonWebServiceResponse<String> handle(com.amazonaws.http.HttpResponse response) throws IOException {

        AmazonWebServiceResponse<String> awsResponse = new AmazonWebServiceResponse<>();

        //putting response string in the result, available outside the handler
        awsResponse.setResult((String) IOUtils.toString(response.getContent()));

        return awsResponse;
    }

    @Override
    public boolean needsConnectionLeftOpen() {
        return false;
    }

}