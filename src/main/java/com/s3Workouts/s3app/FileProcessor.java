package com.s3Workouts.s3app;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cloudfront.CloudFrontUtilities;
import software.amazon.awssdk.services.cloudfront.model.CannedSignerRequest;
import software.amazon.awssdk.services.cloudfront.model.CustomSignerRequest;
import software.amazon.awssdk.services.cloudfront.url.SignedUrl;
import software.amazon.awssdk.services.s3.*;
import software.amazon.awssdk.services.s3.model.*;
import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Time;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping
public class FileProcessor {
    private static final CloudFrontUtilities cloudFrontUtilities =
            CloudFrontUtilities.create();

    @Value("${aws.cloudfront-ddn}")
    private String CLOUD_FRONT_DISTRIBUTION_DOMAIN_NAME;

    @Value("${aws.cloudfront-public-key}")
    private String CLOUD_FRONT_PUBLIC_KEY;

    @Value("${aws.bucket-name}")
    private String bucketName;

    @PostMapping("/")
    public ResponseEntity<Object> init(@RequestParam("file") MultipartFile filed) throws IOException {
        String key = filed.getName()+ "-"+UUID.randomUUID();

        ProfileCredentialsProvider credentialsProvider = ProfileCredentialsProvider.builder()
                .profileName("default")
                .build();

        Region region = Region.US_EAST_1;
        S3Client s3Client = S3Client.builder()
                .region(region)
                .credentialsProvider(credentialsProvider)
                .build();

        CreateMultipartUploadResponse createMultipartUploadResponse =
                s3Client.createMultipartUpload(b -> b
                        .bucket(bucketName)
                        .key(key));
        String uploadId = createMultipartUploadResponse.uploadId();

        int partNumber = 1;
        List<CompletedPart> completedParts = new ArrayList<>();
        byte[] fileBytes = filed.getBytes();
        int partSize = 5 * 1024 * 1024; // 5 MB part size
        ByteBuffer bb;

        for (int i = 0; i < fileBytes.length; i += partSize){
            int currentPartSize = Math.min(partSize, fileBytes.length - i);

            bb = ByteBuffer.wrap(fileBytes, i, currentPartSize);

            UploadPartRequest uploadPartRequest = UploadPartRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .uploadId(uploadId)
                    .partNumber(partNumber)
                    .build();

            UploadPartResponse partResponse = s3Client.uploadPart(
                    uploadPartRequest,
                    RequestBody.fromByteBuffer(bb)
            );

            CompletedPart part = CompletedPart.builder()
                    .partNumber(partNumber)
                    .eTag(partResponse.eTag())
                    .build();

            completedParts.add(part);
            partNumber++;
        }

        CompleteMultipartUploadRequest completeRequest = CompleteMultipartUploadRequest.builder()
                .bucket(bucketName)
                .key(key)
                .uploadId(uploadId)
                .multipartUpload(CompletedMultipartUpload.builder()
                        .parts(completedParts)
                        .build())
                .build();

        s3Client.completeMultipartUpload(completeRequest);

        String objectUrl = s3Client.utilities().getUrl(GetUrlRequest.builder()
                        .bucket(bucketName)
                        .key(key)
                        .build())
                .toExternalForm();

        System.out.println("Uploaded object URL: " + objectUrl);

        return new ResponseEntity<>("Multipart file upload completed successfully! "+ objectUrl, HttpStatus.OK);
    }

    @PostMapping("/a")
    public ResponseEntity<Object> init2(@RequestParam("file") MultipartFile filed) throws Exception {

        ProfileCredentialsProvider credentialsProvider = ProfileCredentialsProvider.builder()
                .profileName("default")
                .build();

        Region region = Region.US_EAST_1;

        S3Client s3Client = S3Client.builder()
                .region(region)
                .credentialsProvider(credentialsProvider)
                .build();

        String key = filed.getName()+ "-"+UUID.randomUUID(); //+ "-" + time;

        try {
            byte[] bytes = filed.getBytes();
            s3Client.putObject(PutObjectRequest.builder().key(key).bucket(bucketName).build(), RequestBody.fromBytes(bytes));
        } catch (IOException e) {
//            e.printStackTrace();
            System.out.println(e.getMessage());
        }

        String objectUrl = s3Client.utilities().getUrl(GetUrlRequest.builder()
                        .bucket(bucketName)
                        .key(key)
                        .build())
                .toExternalForm();

        System.out.println("Uploaded object URL: " + objectUrl);

        Path privateKeyPath = Paths.get(".aws/cloudfront_private_key.pem");
//        var cannedSignerRequestCanned = createRequestForCannedPolicy(CLOUD_FRONT_DISTRIBUTION_DOMAIN_NAME, key, privateKeyPath.toAbsolutePath().toString(), CLOUD_FRONT_PUBLIC_KEY);

        var cannedSignerRequestCustom = createRequestForCustomPolicy(CLOUD_FRONT_DISTRIBUTION_DOMAIN_NAME, key, privateKeyPath.toAbsolutePath().toString(), CLOUD_FRONT_PUBLIC_KEY);

//        var signedUrlForCannedPolicy = signUrlForCannedPolicy(cannedSignerRequestCanned);
        var signedUrlForCustomPolicy = signUrlForCustomPolicy(cannedSignerRequestCustom);

        return new ResponseEntity<>(signedUrlForCustomPolicy.url() + " Multipart file upload completed successfully! "+ objectUrl, HttpStatus.OK);
    }

    public static CannedSignerRequest createRequestForCannedPolicy(String distributionDomainName,
                                                                   String fileNameToUpload,
                                                                   String privateKeyFullPath,
                                                                   String publicKeyId
    ) throws Exception {
        String protocol = "https";
        String resourcePath = "/" + fileNameToUpload;
        System.out.println("** "+ protocol + " " + distributionDomainName +  " " +resourcePath);
        String cloudFrontUrl = new URL(protocol, distributionDomainName, resourcePath).toString();
        Instant expirationDate = Instant.now().plus(1, ChronoUnit.MINUTES);
        Path path = Paths.get(privateKeyFullPath);

        return CannedSignerRequest.builder()
                .resourceUrl(cloudFrontUrl)
                .privateKey(path)
                .keyPairId(publicKeyId)
                .expirationDate(expirationDate)
                .build();
    }

    public static CustomSignerRequest createRequestForCustomPolicy(String
                                                                           distributionDomainName,
                                                                   String fileNameToUpload,
                                                                   String privateKeyFullPath, String publicKeyId) throws Exception {
        String protocol = "https";
        String resourcePath = "/" + fileNameToUpload;
        String cloudFrontUrl = new URL(protocol, distributionDomainName,
                resourcePath).toString();
//        Instant expireDate = Instant.now().plus(2, ChronoUnit.MINUTES);
//        // URL will be accessible tomorrow using the signed URL.
//        Instant activeDate = Instant.now().plus(1, ChronoUnit.MINUTES);

        // Set the Lagos time zone
        ZoneId lagosTimeZone = ZoneId.of("Africa/Lagos");

        // Set expiration and active dates
        ZonedDateTime lagosNow = ZonedDateTime.now(lagosTimeZone);
        Instant expireDate = lagosNow.plusHours(5).toInstant();
        Instant activeDate = lagosNow.plusHours(4).toInstant(); // Optional

        System.out.println(expireDate + " " + activeDate);

        Path path = Paths.get(privateKeyFullPath);
        return CustomSignerRequest.builder()
                .resourceUrl(cloudFrontUrl)
                .privateKey(path)
                .keyPairId(publicKeyId)
                .expirationDate(expireDate)
                .activeDate(activeDate) // Optional.
                // .ipRange("192.168.0.1/24") // Optional.
                .build();
    }

    public static SignedUrl signUrlForCannedPolicy(CannedSignerRequest
                                                           cannedSignerRequest) {
        //        logger.info("Signed URL: [{}]", signedUrl.url());
        return cloudFrontUtilities.getSignedUrlWithCannedPolicy(cannedSignerRequest);
    }

    public static SignedUrl signUrlForCustomPolicy(CustomSignerRequest
                                                           customSignerRequest) {
        return cloudFrontUtilities.getSignedUrlWithCustomPolicy(customSignerRequest);
    }
}
