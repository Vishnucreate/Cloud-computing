package com.example.cloud1;

import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.rekognition.RekognitionClient;
import software.amazon.awssdk.services.rekognition.model.DetectLabelsRequest;
import software.amazon.awssdk.services.rekognition.model.DetectLabelsResponse;
import software.amazon.awssdk.services.rekognition.model.Image;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.util.List;

public class ObjectDetection {

    private static final String BUCKET_NAME = "njit-cs-643";
    private static final String QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/323052225972/sqsforcarimage";
    private static final Region REGION = Region.of("us-east-1b"); // Specify region

    public static void main(String[] args) {
        // Set up S3, Rekognition, and SQS clients
        S3Client s3Client = S3Client.builder()
                .region(REGION) // Set region explicitly
                .credentialsProvider(InstanceProfileCredentialsProvider.create())
                .build();

        RekognitionClient rekognitionClient = RekognitionClient.builder()
                .region(REGION) // Set region explicitly
                .credentialsProvider(InstanceProfileCredentialsProvider.create())
                .build();

        SqsClient sqsClient = SqsClient.builder()
                .region(REGION) // Set region explicitly
                .credentialsProvider(InstanceProfileCredentialsProvider.create())
                .build();

        // List images in the S3 bucket
        ListObjectsV2Request listObjectsRequest = ListObjectsV2Request.builder()
                .bucket(BUCKET_NAME)
                .build();
        ListObjectsV2Response listObjectsResponse = s3Client.listObjectsV2(listObjectsRequest);

        List<S3Object> images = listObjectsResponse.contents();

        for (S3Object image : images) {
            String imageKey = image.key();

            // Detect objects in the image using Rekognition
            DetectLabelsRequest detectLabelsRequest = DetectLabelsRequest.builder()
                    .image(Image.builder()
                            .s3Object(software.amazon.awssdk.services.rekognition.model.S3Object.builder()
                                    .bucket(BUCKET_NAME)
                                    .name(imageKey)
                                    .build())
                            .build())
                    .maxLabels(10)
                    .minConfidence(90F)
                    .build();

            DetectLabelsResponse detectLabelsResponse = rekognitionClient.detectLabels(detectLabelsRequest);

            // Check if a car is detected
            boolean carDetected = detectLabelsResponse.labels().stream()
                    .anyMatch(label -> label.name().equalsIgnoreCase("Car") && label.confidence() >= 90F);

            if (carDetected) {
                System.out.println("Car detected in image: " + imageKey);

                // Send message to SQS with image key
                SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                        .queueUrl(QUEUE_URL)
                        .messageBody(imageKey)
                        .build();

                sqsClient.sendMessage(sendMessageRequest);
                System.out.println("Message sent to SQS for image: " + imageKey);
            }
        }

        // Send termination signal to SQS
        SendMessageRequest terminateMessageRequest = SendMessageRequest.builder()
                .queueUrl(QUEUE_URL)
                .messageBody("-1") // Termination signal
                .build();

        sqsClient.sendMessage(terminateMessageRequest);
        System.out.println("Termination signal sent to SQS.");
    }
}
