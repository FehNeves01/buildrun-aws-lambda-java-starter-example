package tech.buildrun.lambda;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.UUID;

import javax.imageio.ImageIO;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.rendering.PDFRenderer;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.fasterxml.jackson.databind.ObjectMapper;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class Handler implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

    private static final String BUCKET_NAME = "alfreddev";
    private static final String REGION = "sa-east-1";
    private static final String IMAGE_PATH_PREFIX = "images/";
    private static final String CONTENT_TYPE = "image/png";

    private final S3Client s3Client;

    public Handler() {
        this.s3Client = S3Client.builder()
                .region(Region.of(REGION))
                .httpClient(ApacheHttpClient.builder().build())
                .build();
    }

    @Override
    public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent request, Context context) {
        var logger = context.getLogger();
        logger.log("Request received.");

        try {
            PdfRequest pdfRequest = parseRequestBody(request.getBody());
            BufferedImage image = downloadAndRenderPdf(pdfRequest.url(), logger);
            String imageUrl = uploadImageToS3(image, logger);

            return createResponse(200, "{ \"imageUrl\": \"" + imageUrl + "\" }");
        } catch (IOException e) {
            logger.log("Error during PDF processing: " + e.getMessage());
            return createResponse(500, "{ \"error\": \"Erro ao processar PDF\" }");
        }
    }

    private PdfRequest parseRequestBody(String body) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue(body, PdfRequest.class);
    }

    private BufferedImage downloadAndRenderPdf(String pdfUrl, LambdaLogger logger) throws IOException {
        logger.log("Downloading and rendering PDF from: " + pdfUrl);
        URL url = new URL(pdfUrl);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");

        try (InputStream pdfStream = connection.getInputStream(); PDDocument document = PDDocument.load(pdfStream)) {
            PDFRenderer pdfRenderer = new PDFRenderer(document);
            return pdfRenderer.renderImageWithDPI(0, 100);
        } finally {
            connection.disconnect();
        }
    }

    private String uploadImageToS3(BufferedImage image, LambdaLogger logger) throws IOException {
        logger.log("Uploading image to S3.");

        String objectKey = IMAGE_PATH_PREFIX + UUID.randomUUID() + ".png";
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ImageIO.write(image, "png", outputStream);

        s3Client.putObject(
                PutObjectRequest.builder()
                        .bucket(BUCKET_NAME)
                        .key(objectKey)
                        .acl("public-read")
                        .contentType(CONTENT_TYPE)
                        .build(),
                RequestBody.fromBytes(outputStream.toByteArray())
        );

        logger.log("Image uploaded successfully to S3: " + objectKey);
        return "https://" + BUCKET_NAME + ".s3.amazonaws.com/" + objectKey;
    }

    private APIGatewayProxyResponseEvent createResponse(int statusCode, String body) {
        return new APIGatewayProxyResponseEvent()
                .withStatusCode(statusCode)
                .withBody(body)
                .withIsBase64Encoded(false);
    }

    private static class PdfRequest {

        private String url;

        public String url() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }
    }
}
