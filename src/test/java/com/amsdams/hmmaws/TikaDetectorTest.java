package com.amsdams.hmmaws;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.tika.detect.DefaultDetector;
import org.apache.tika.detect.Detector;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
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
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.S3Object;

import io.findify.s3mock.S3Mock;
import lombok.extern.slf4j.Slf4j;

@SpringBootTest
@Slf4j
public class TikaDetectorTest {

	private static final String KEY = "key";
	private static final String BUCKET_NAME = "bucket-name";

	@Autowired
	ResourcePatternResolver resourcePatternResolver;

	static S3Mock api;
	static AmazonS3Client client;

	@BeforeAll
	static void start() {
		api = new S3Mock.Builder().withPort(8001).withInMemoryBackend().build();
		api.start();
		EndpointConfiguration endpoint = new EndpointConfiguration("http://localhost:8001", "us-west-2");
		client = (AmazonS3Client) AmazonS3ClientBuilder.standard().withPathStyleAccessEnabled(true)
				.withEndpointConfiguration(endpoint)
				.withCredentials(new AWSStaticCredentialsProvider(new AnonymousAWSCredentials())).build();
	}

	@BeforeEach
	void startEach() {
		client.createBucket(BUCKET_NAME);
	}

	@AfterEach
	void stopEach() {
		client.deleteBucket(BUCKET_NAME);
	}

	@AfterAll
	static void stop() {
		api.shutdown();
	}

	@ParameterizedTest
	@CsvSource({ "classpath:data/file_example_MP4_480_1_5MG.mp4,video/mp4",
			"classpath:data/file_example_MP3_700KB.mp3,audio/mpeg",
			"classpath:data/file-sample_150kB.pdf,application/pdf" })
	void test_OK(String input, String expected) throws AmazonServiceException, SdkClientException, IOException {

		Resource resource = resourcePatternResolver.getResource(input);

		client.putObject(BUCKET_NAME, KEY, resource.getFile());
		S3Object s3Object = client.getObject(BUCKET_NAME, KEY);
		MediaType mediaType = this.getMediaType(s3Object.getObjectContent());

		log.info("mediaType {}", mediaType);
		Assertions.assertEquals(expected, mediaType.toString());

	}

	@ParameterizedTest
	@CsvSource({ "classpath:data/file_example_MP4_480_1_5MG.mp4,video/mp4",
			"classpath:data/file_example_MP3_700KB.mp3,audio/mpeg",
			"classpath:data/file-sample_150kB.pdf,application/pdf" })
	void test_FAIL(String input, String expected) throws AmazonServiceException, SdkClientException, IOException {

		Resource resource = resourcePatternResolver.getResource(input);

		client.putObject(BUCKET_NAME, KEY, resource.getFile());
		S3Object s3Object = client.getObject(BUCKET_NAME, KEY);
		MediaType mediaType = this.getMediaType(s3Object.getObjectContent());

		log.info("mediaType {}", mediaType);
		Assertions.assertEquals(expected, mediaType.toString());
		if (mediaType.toString().equals(expected)) {
			client.deleteObject(BUCKET_NAME, KEY);
		}
		AmazonS3Exception exception = Assertions.assertThrows(AmazonS3Exception.class, () -> {
			client.getObject(BUCKET_NAME, KEY);

		});
		Assertions.assertEquals(
				"The resource you requested does not exist (Service: Amazon S3; Status Code: 404; Error Code: NoSuchKey; Request ID: null; S3 Extended Request ID: null)",
				exception.getMessage());

	}

	MediaType getMediaType(InputStream inputStream) throws IOException {
		Detector detector = new DefaultDetector();
		Metadata metadata = new Metadata();
		return detector.detect(new BufferedInputStream(inputStream), metadata);

	}
}
