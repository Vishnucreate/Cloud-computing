import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import java.io.ByteArrayInputStream;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.w3c.dom.Element;
import java.nio.charset.StandardCharsets;

public class ObjectDetection {

    public static void main(String[] args) throws IOException, JMSException {
        Regions clientRegion = Regions.US_EAST_1;
        String xmlBucketName = "carimagexml";
        String xmlFileName = "carimage.xml";
        String imageBucketName = "njit-cs-643"; // The bucket where images are located
        String queueName = "MyQueue.fifo";

        try {
            // Create S3 Client
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                    .withRegion(clientRegion)
                    .withCredentials(new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials()))
                    .build();

            // Download XML file from S3
            S3Object xmlFile = s3Client.getObject(xmlBucketName, xmlFileName);
            S3ObjectInputStream xmlStream = xmlFile.getObjectContent();
            
            // Parse XML content to retrieve image keys
            Document xmlDocument = DocumentBuilderFactory.newInstance().newDocumentBuilder()
                    .parse(new ByteArrayInputStream(xmlStream.readAllBytes()));

            NodeList contentsList = xmlDocument.getElementsByTagName("Contents");

            // Set up SQS and Rekognition as before
            SQSConnectionFactory connectionFactory = new SQSConnectionFactory(
                    new ProviderConfiguration(),
                    AmazonSQSClientBuilder.standard()
                            .withRegion(clientRegion)
                            .withCredentials(new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials()))
            );

            SQSConnection connection = connectionFactory.createConnection();
            AmazonSQSMessagingClientWrapper client = connection.getWrappedAmazonSQSClient();

            if (!client.queueExists(queueName)) {
                Map<String, String> attributes = new HashMap<>();
                attributes.put("FifoQueue", "true");
                attributes.put("ContentBasedDeduplication", "true");
                client.createQueue(new CreateQueueRequest().withQueueName(queueName).withAttributes(attributes));
            }

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue(queueName);
            MessageProducer producer = session.createProducer(queue);

            // Process each image key found in the XML
            for (int i = 0; i < contentsList.getLength(); i++) {
                Element contentElement = (Element) contentsList.item(i);
                String imageKey = contentElement.getElementsByTagName("Key").item(0).getTextContent();

                // Create Rekognition client
                AmazonRekognition rekognitionClient = AmazonRekognitionClientBuilder.standard()
                        .withRegion(clientRegion)
                        .withCredentials(new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials()))
                        .build();

                // Prepare request to detect labels in the image
                DetectLabelsRequest detectLabelsRequest = new DetectLabelsRequest()
                        .withImage(new Image().withS3Object(new S3Object().withName(imageKey).withBucket(imageBucketName)))
                        .withMaxLabels(10)
                        .withMinConfidence(75F);

                try {
                    // Detect labels in the image
                    DetectLabelsResult detectLabelsResult = rekognitionClient.detectLabels(detectLabelsRequest);
                    List<Label> labels = detectLabelsResult.getLabels();

                    for (Label label : labels) {
                        if ("Car".equals(label.getName()) && label.getConfidence() > 90) {
                            System.out.print("Detected labels for: " + imageKey + " => ");
                            System.out.print("Label: " + label.getName() + ", ");
                            System.out.println("Confidence: " + label.getConfidence());

                            // Send message to SQS
                            TextMessage message = session.createTextMessage(imageKey);
                            message.setStringProperty("JMSXGroupID", "Default");
                            producer.send(message);

                            System.out.println("Pushed to SQS with JMS Message ID: " + message.getJMSMessageID());
                        }
                    }
                } catch (AmazonRekognitionException e) {
                    e.printStackTrace();
                }
            }

            // Close the connection
            connection.close();
        } catch (AmazonServiceException | SdkClientException | Exception e) {
            e.printStackTrace();
        }
    }
}
