package com.bridgelabz.todoawsservice.controller;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.bridgelabz.todoawsservice.repository.elasticsearch.ToDoAwsElasticSearch;
import com.bridgelabz.todoawsservice.s3service.AwsS3Service;

@RestController
@RequestMapping(value="/aws-controller")
@RefreshScope
public class AwsController {
	
	@Autowired
	private AwsS3Service awsService;

	@Autowired
	private ToDoAwsElasticSearch toAwsElasticSearch;
	
	@PostMapping("/create-folder-into-bucket")
	public void createFolderIntoBucket(@RequestParam String bucketName,@RequestParam String folderName) {
		awsService.createFolderIntoBucket(bucketName, folderName);
	}

	@PostMapping("/put-object-into-bucket")
	public String putObjectIntoBucket(@RequestParam String bucketName, @RequestParam String folderName, 
			@RequestParam String filePath,@RequestParam String name) throws AmazonServiceException, SdkClientException, IOException{
		File file=new File(filePath);
		return awsService.putObjectIntoBucket(bucketName, folderName,file,name);
	}
	
		@PostMapping("/insert-object")
	    public   <T> void insertObject(@RequestBody T obj,@RequestParam String index,@RequestParam String type) throws Exception{
			 toAwsElasticSearch.insertObject(obj,index,type);
	    }
		
	    @GetMapping("/get-object/{id}")
	    public Map<String, Object> getObjectById(@PathVariable String id,@RequestParam String index,@RequestParam String type){
	        return toAwsElasticSearch.getObjectById(id,index,type);
	    }
	    
	    @PutMapping("/update-object/{id}")
	    public <T> Map<String, Object> updateBookById(@RequestBody Object obj, @PathVariable String id,@RequestParam String index,@RequestParam String type){
	        return toAwsElasticSearch.updateObjectById(id, obj,index,type);
	    }
	    
	    @DeleteMapping("/delete-object/{id}")
	    public void deleteObjectById(@PathVariable String id,@RequestParam String index,@RequestParam String type){
	    	toAwsElasticSearch.deleteObjectById(id,index,type);
	    }
	   
	    
	    
}
