package com.bridgelabz.todoawsservice.s3service;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

@Service
public class AwsS3Service {
	private static final String SUFFIX = "/";

	String id = System.getenv("id");
	String key = System.getenv("key");

	AWSCredentials credentials = new BasicAWSCredentials(id, key);

	// create a client connection based on credentials

	AmazonS3 s3client = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(credentials))
			.withRegion("ap-south-1").build();

	public String putObjectIntoBucket(String bucketName, String folderName, File file, String name)
			throws AmazonServiceException, SdkClientException, IOException {
		String fileName = folderName + SUFFIX + name;
		PutObjectRequest req = new PutObjectRequest(bucketName, fileName, file);

		req.setCannedAcl(CannedAccessControlList.PublicRead);
		s3client.putObject(req);

		return ((AmazonS3Client) s3client).getResourceUrl(bucketName, fileName);
	}

	public void createFolderIntoBucket(String bucketName, String folderName) {
		// create folder into bucket
		createFolder(bucketName, folderName, s3client);
	}

	public void diplayListOfBuckets() {
		// list buckets

		for (Bucket bucket : s3client.listBuckets()) {
			System.out.println(" - " + bucket.getName());
		}
	}

	public void createBucket(String bucketName) {
		s3client.createBucket(bucketName);
	}

	public void deleteBucket(String bucketName) {
		// deletes bucket
		s3client.deleteBucket(bucketName);
	}

	public void createFolder(String bucketName, String folderName, AmazonS3 client) {

		// create meta-data for your folder and set content-length to 0
		ObjectMetadata metadata = new ObjectMetadata();
		metadata.setContentLength(0);

		// create empty content
		InputStream emptyContent = new ByteArrayInputStream(new byte[0]);

		// create a PutObjectRequest passing the folder name suffixed by /
		PutObjectRequest putObjectRequest = new PutObjectRequest(bucketName, folderName + SUFFIX, emptyContent,
				metadata);

		// send request to S3 to create folder
		client.putObject(putObjectRequest);

	}

	/*
	 * public S3Object getObject(String bucketName,String folderName,AmazonS
	 * //public S3Object s3Interactor() {3 client) { List fileList =
	 * client.listObjects(bucketName, folderName).getObjectSummaries() return
	 * client.getObject(bucketName, "paris.jpg");
	 * 
	 * }
	 */

	/**
	 * This method first deletes all the files in given folder and than the folder
	 * itself
	 * 
	 * @throws IOException
	 */
	
	 /* public static void deleteFolder(String bucketName, String folderName,AmazonS3 client) 
	  { 
		  List fileList = client.listObjects(bucketName, folderName).getObjectSummaries(); 
		  for (S3ObjectSummary file : fileList) {
			  client.deleteObject(bucketName, file.getKey()); }
		  client.deleteObject(bucketName, folderName); 
	  }*/

	static public File convert(MultipartFile file) throws IOException {
		File convFile = new File(file.getOriginalFilename());
		convFile.createNewFile();
		FileOutputStream fos = new FileOutputStream(convFile);
		fos.write(file.getBytes());
		fos.close();
		return convFile;
	}
}

//}
