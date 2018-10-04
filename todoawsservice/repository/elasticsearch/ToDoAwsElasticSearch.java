package com.bridgelabz.todoawsservice.repository.elasticsearch;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.stereotype.Repository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
@Repository
public class ToDoAwsElasticSearch {
/************************************************************************************/

    private RestHighLevelClient restHighLevelClient;

    private ObjectMapper objectMapper;
    
    ObjectMapper mapperObj = new ObjectMapper();
    
    public ToDoAwsElasticSearch( ObjectMapper objectMapper, RestHighLevelClient restHighLevelClient) {
        this.objectMapper = objectMapper;
        this.restHighLevelClient = restHighLevelClient;
    }
    
    public  <T> void insertObject(T obj,String index,String type) throws JsonProcessingException{
       // book.setId(UUID.randomUUID().toString());
    	System.out.println("In insertObject Method");
    	
    	 String jsonResp="";
    	try {
    		jsonResp = mapperObj.writeValueAsString(obj);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
       // Map dataMap = objectMapper.convertValue(jsonResp, Map.class);
        IndexRequest indexRequest = new IndexRequest(index, type)
                .source(jsonResp,XContentType.JSON);
        try {
            IndexResponse response = restHighLevelClient.index(indexRequest);
        } catch(ElasticsearchException e) {
            e.getDetailedMessage();
        } catch (java.io.IOException ex){
            ex.getLocalizedMessage();
        }
       // return book;
    }

    public Map<String, Object> getObjectById(String id,String index,String type){
        GetRequest getRequest = new GetRequest(index, type, id);
        GetResponse getResponse = null;
        try {
            getResponse = restHighLevelClient.get(getRequest);
        } catch (java.io.IOException e){
            e.getLocalizedMessage();
        }
        Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
        return sourceAsMap;
    }

    public <T> Map<String, Object> updateObjectById(String id,T obj,String index,String type){
        UpdateRequest updateRequest = new UpdateRequest(index, type, id)
                .fetchSource(true);    // Fetch Object after its update
        Map<String, Object> error = new HashMap<>();
        error.put("Error", "Unable to update book");
        try {
            String bookJson = objectMapper.writeValueAsString(obj);
            updateRequest.doc(bookJson, XContentType.JSON);
            UpdateResponse updateResponse = restHighLevelClient.update(updateRequest);
            Map<String, Object> sourceAsMap = updateResponse.getGetResult().sourceAsMap();
            return sourceAsMap;
        }catch (JsonProcessingException e){
            e.getMessage();
        } catch (java.io.IOException e){
            e.getLocalizedMessage();
        }
        return error;
    }

    public void deleteObjectById(String id,String index,String type) {
        DeleteRequest deleteRequest = new DeleteRequest(index, type, id);
        try {
            DeleteResponse deleteResponse = restHighLevelClient.delete(deleteRequest);
        } catch (java.io.IOException e){
            e.getLocalizedMessage();
        }
    }

}
