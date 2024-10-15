package com.aws.ec2;

import java.io.IOException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.springframework.boot.SpringApplication;

import com.amazon.sqs.javamessaging.AmazonSQSMessagingClientWrapper;
import com.amazon.sqs.javamessaging.ProviderConfiguration;
import com.amazon.sqs.javamessaging.SQSConnection;
import com.amazon.sqs.javamessaging.SQSConnectionFactory;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.rekognition.AmazonRekognition;
import com.amazonaws.services.rekognition.AmazonRekognitionClientBuilder;
import com.amazonaws.services.rekognition.model.AmazonRekognitionException;
import com.amazonaws.services.rekognition.model.DetectLabelsRequest;
import com.amazonaws.services.rekognition.model.DetectLabelsResult;
import com.amazonaws.services.rekognition.model.Image;
import com.amazonaws.services.rekognition.model.Label;
import com.amazonaws.services.rekognition.model.S3Object;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;

public class AWSObjectRekognition {

    public static void main(String[] args) throws IOException, JMSException, InterruptedException {
        SpringApplication.run(AWSObjectRekognition.class, args);

        Regions clientRegion = Regions.US_EAST_1;
        String bucketName = "njit-cs-643";
        String queueName = "sqsforcarimage.fifo";

        try {
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                    .withRegion(clientRegion)
                    .withCredentials(new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials()))
                    .build();

            // Create a new connection factory with explicit credentials
            SQSConnectionFactory connectionFactory = new SQSConnectionFactory(new ProviderConfiguration(),
                    AmazonSQSClientBuilder.standard()
                            .withRegion(clientRegion)
                            .withCredentials(new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials()))
                            .build());

            // Create the connection
            SQSConnection connection = connectionFactory.createConnection();

            // Get the wrapped SQS client
            AmazonSQSMessagingClientWrapper client = connection.getWrappedAmazonSQSClient();

            // Check if the queue exists, and create it if it does not
            if (!client.queueExists(queueName)) {
                Map<String, String> attributes = new HashMap<>();
                attributes.put("FifoQueue", "true");
                attributes.put("ContentBasedDeduplication", "true");
                client.createQueue(new CreateQueueRequest().withQueueName(queueName).withAttributes(attributes));
            }

            // Create a nontransacted session with AUTO_ACKNOWLEDGE mode
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Create a queue identity with the specific queue name
            Queue queue = session.createQueue(queueName);

            // Create a message producer for the specified queue
            MessageProducer producer = session.createProducer(queue);
            System.out.println("Listing objects...");

            // List objects in the S3 bucket
            ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucketName);
            ListObjectsV2Result result;
            do {
                result = s3Client.listObjectsV2(req);
                for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
                    String photo = objectSummary.getKey();
                    AmazonRekognition rekognitionClient = AmazonRekognitionClientBuilder.standard()
                            .withRegion(clientRegion)
                            .withCredentials(new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials()))
                            .build();

                    DetectLabelsRequest detectLabelsRequest = new DetectLabelsRequest()
                            .withImage(new Image().withS3Object(new S3Object().withName(photo).withBucket(bucketName)))
                            .withMaxLabels(10).withMinConfidence(75F);
                    try {
                        DetectLabelsResult detectLabelsResult = rekognitionClient.detectLabels(detectLabelsRequest);
                        List<Label> labels = detectLabelsResult.getLabels();

                        for (Label label : labels) {
                            if ("Car".equals(label.getName()) && label.getConfidence() > 90) {
                                System.out.print("Detected labels for:  " + photo + " => ");
                                System.out.print("Label: " + label.getName() + ", ");
                                System.out.println("Confidence: " + label.getConfidence());

                                TextMessage message = session.createTextMessage(objectSummary.getKey());
                                message.setStringProperty("JMSXGroupID", "Default");
                                producer.send(message);

                                System.out.println("Pushed to SQS with JMS Message ID: " + message.getJMSMessageID());
                                System.out.println("JMS Message Sequence Number: " + message.getStringProperty("JMS_SQS_SequenceNumber"));
                            }
                        }
                    } catch (AmazonRekognitionException e) {
                        e.printStackTrace();
                    }
                }
                req.setContinuationToken(result.getNextContinuationToken());
            } while (result.isTruncated());

            connection.close();
        } catch (AmazonServiceException e) {
            e.printStackTrace();
        } catch (SdkClientException e) {
            e.printStackTrace();
        }
    }
}
