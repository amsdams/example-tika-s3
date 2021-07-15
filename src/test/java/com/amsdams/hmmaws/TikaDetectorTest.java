package com.amsdams.hmmaws;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.List;

import org.apache.tika.detect.DefaultDetector;
import org.apache.tika.detect.Detector;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.mp4.MP4Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mp4parser.Box;
import org.mp4parser.IsoFile;
import org.mp4parser.boxes.iso14496.part12.HandlerBox;
import org.mp4parser.boxes.iso14496.part12.MediaBox;
import org.mp4parser.boxes.iso14496.part12.TrackBox;
import org.red5.io.mp3.impl.MP3Reader;
import org.red5.io.mp4.impl.MP4Reader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

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
	void test_OK(String input, String expected)
			throws AmazonServiceException, SdkClientException, IOException, SAXException, TikaException {

		Resource resource = resourcePatternResolver.getResource(input);

		client.putObject(BUCKET_NAME, KEY, resource.getFile());
		S3Object s3Object = client.getObject(BUCKET_NAME, KEY);
		InputStream inputStream = s3Object.getObjectContent();

		MediaType mediaType = this.getMediaType(inputStream);

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
		InputStream inputStream = s3Object.getObjectContent();

		MediaType mediaType = this.getMediaType(inputStream);

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

	void parser(InputStream inputStream) throws IOException, SAXException, TikaException {
		MP4Parser mp4 = new MP4Parser();
		ContentHandler handler = new BodyContentHandler();
		Metadata metadata = new Metadata();
		ParseContext context = new ParseContext();

		mp4.parse(new BufferedInputStream(inputStream), handler, metadata, context);

		String[] metadataNames = metadata.names();

		for (String name : metadataNames) {
			System.out.println(name + ": " + metadata.get(name));
		}

		// metadata.getValues(null)
	}

	public static final String PREFIX = "stream2file";
	public static final String SUFFIX = ".tmp";

	public static File stream2file(InputStream in) throws IOException {
		final File tempFile = File.createTempFile(PREFIX, SUFFIX);
		tempFile.deleteOnExit();
		Files.copy(in, tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
		return tempFile;

	}

	@Test
	void test_parser_OK_MP4()
			throws AmazonServiceException, SdkClientException, IOException, SAXException, TikaException {

		Resource resource = resourcePatternResolver.getResource("classpath:data/file_example_MP4_480_1_5MG.mp4");

		client.putObject(BUCKET_NAME, KEY, resource.getFile());
		S3Object s3Object = client.getObject(BUCKET_NAME, KEY);
		InputStream inputStream = s3Object.getObjectContent();
		ReadableByteChannel in = Channels.newChannel(inputStream);

		IsoFile isoFile = new IsoFile(in);
		List<TrackBox> trackBoxes = isoFile.getMovieBox().getBoxes(TrackBox.class);

		for (TrackBox trackBox : trackBoxes) {
			MediaBox mdia = trackBox.getMediaBox();
			log.info("mdia {}", mdia.toString());
			HandlerBox hdlr = mdia.getHandlerBox(); // hdlr
			log.info("hdlr {}", hdlr.toString());
		}

	}

	@Test
	void test_parser_OK_MP4_RED5()
			throws AmazonServiceException, SdkClientException, IOException, SAXException, TikaException {

		Resource resource = resourcePatternResolver.getResource("classpath:data/file_example_MP4_480_1_5MG.mp4");

		client.putObject(BUCKET_NAME, KEY, resource.getFile());
		S3Object s3Object = client.getObject(BUCKET_NAME, KEY);
		InputStream inputStream = s3Object.getObjectContent();
		MP4Reader mp4Reader = new MP4Reader(stream2file(inputStream));
		Assertions.assertEquals(1570024, mp4Reader.getTotalBytes());
		Assertions.assertEquals("mp4a", mp4Reader.getAudioCodecId());
		Assertions.assertEquals("avc1", mp4Reader.getVideoCodecId());

	}

	@Test
	void test_parser_OK_MP3_RED5()
			throws AmazonServiceException, SdkClientException, IOException, SAXException, TikaException {

		Resource resource = resourcePatternResolver.getResource("classpath:data/file_example_MP3_700KB.mp3");

		client.putObject(BUCKET_NAME, KEY, resource.getFile());
		S3Object s3Object = client.getObject(BUCKET_NAME, KEY);
		InputStream inputStream = s3Object.getObjectContent();
		MP3Reader mp3Reader = new MP3Reader(stream2file(inputStream));
		Assertions.assertEquals(123, mp3Reader.getTotalBytes());

	}
}
