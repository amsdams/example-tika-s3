package com.amsdams.hmmaws;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.tika.detect.DefaultDetector;
import org.apache.tika.detect.Detector;
import org.apache.tika.metadata.Metadata;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.ResourcePatternResolver;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder.EndpointConfiguration;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;

import io.findify.s3mock.S3Mock;
import lombok.extern.slf4j.Slf4j;

@SpringBootTest
@Slf4j
public class BTest {

	private static final String KEY = "key";
	private static final String BUCKET_NAME = "bucket-name";

	@Autowired
	ResourcePatternResolver resourcePatternResolver;

	static S3Mock api;
	static AmazonS3Client client;

	@BeforeAll
	static void  start() {
		api = new S3Mock.Builder().withPort(8001).withInMemoryBackend().build();
		api.start();
		EndpointConfiguration endpoint = new EndpointConfiguration("http://localhost:8001", "us-west-2");
		client = (AmazonS3Client) AmazonS3ClientBuilder.standard().withPathStyleAccessEnabled(true)
				.withEndpointConfiguration(endpoint)
				.withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials())).build();
	}

	@AfterAll
	static void stop() {
		api.shutdown();
	}

	@Test
	void test() throws AmazonServiceException, SdkClientException, IOException {

		Resource resource = resourcePatternResolver.getResource("classpath:data/file_example_MP4_480_1_5MG.mp4");

		client.createBucket(BUCKET_NAME);
		client.putObject(BUCKET_NAME, KEY, resource.getFile());
		S3Object s3Object = client.getObject(BUCKET_NAME, KEY);
		log.info(this.getMediaType(s3Object.getObjectContent()));

	}

	String getMediaType(InputStream inputStream) throws IOException {
		Detector detector = new DefaultDetector();
		// Parser parser = new DefaultParser();
		Metadata metadata = new Metadata();

		detector.detect(new BufferedInputStream(inputStream), metadata);
		return metadata.toString();

	}
}
