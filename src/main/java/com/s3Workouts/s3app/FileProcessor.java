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
import software.amazon.awssdk.services.s3.*;
import software.amazon.awssdk.services.s3.model.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Time;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping
public class FileProcessor {
    @Value("${aws.bucket-name}")
    private String bucketName;

    @PostMapping("/")
    public ResponseEntity<Object> init(@RequestParam("file") MultipartFile filed) throws IOException {
        String key = filed.getName()+ "-"+UUID.randomUUID();

        ProfileCredentialsProvider credentialsProvider = ProfileCredentialsProvider.builder()
                .profileName("default")
                .build();

        Region region = Region.EU_NORTH_1;
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
    public ResponseEntity<Object> init2(@RequestParam("file") MultipartFile filed) {

        ProfileCredentialsProvider credentialsProvider = ProfileCredentialsProvider.builder()
                .profileName("default")
                .build();

        Region region = Region.EU_NORTH_1;

        S3Client s3Client = S3Client.builder()
                .region(region)
                .credentialsProvider(credentialsProvider)
                .build();
        var time = Time.from(Instant.now()).toString();

        String key = filed.getName()+ "-"+UUID.randomUUID() + "-" + time;

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

        return new ResponseEntity<>("Multipart file upload completed successfully! "+ objectUrl, HttpStatus.OK);
    }
}
