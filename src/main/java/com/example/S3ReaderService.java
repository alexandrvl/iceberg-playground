package com.example;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import java.io.InputStream;

public class S3ReaderService {
    private final S3Client s3Client;

    public S3ReaderService(S3Client s3Client) {
        this.s3Client = s3Client;
    }

    public InputStream readDataFromS3(String bucketName, String key) {
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build();

        return s3Client.getObject(getObjectRequest);
    }
}
