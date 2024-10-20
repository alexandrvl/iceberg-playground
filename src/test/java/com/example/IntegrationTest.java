package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.InputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Testcontainers
public class IntegrationTest {
    public static final String TESTUSER = "testuser";
    public static final String TESTPASSWORD = "testpassword";
    public static final String NAMESPACE = "default";
    public static final String TABLE_NAME = "example";

    @Container
    private static final PostgreSQLContainer<?> postgresContainer = new PostgreSQLContainer<>("postgres:13")
            .withDatabaseName("iceberg")
            .withUsername("iceberg")
            .withPassword("password")
            .withExposedPorts(5432);


    @Container
    private static final MinIOContainer minIOContainer = new MinIOContainer("minio/minio:RELEASE.2023-09-04T19-57-37Z")
            .withExposedPorts(9000, 9001)
            .withUserName(TESTUSER)
            .withPassword(TESTPASSWORD);

    @BeforeAll
    public static void setup() {
        postgresContainer.start();
        minIOContainer.start();
    }

    @Test
    public void testIcebergTransformation() throws Exception {
        // Initialize services with test container configurations
        // Define AWS region
        Region region = Region.US_EAST_1;

        // Create AWS credentials
        AwsBasicCredentials awsCreds = AwsBasicCredentials.create(TESTUSER, TESTPASSWORD);
        var awsCredsProvider = StaticCredentialsProvider.create(awsCreds);

        // Create S3 client with region and credentials
        S3Client s3Client = S3Client.builder()
                .endpointOverride(URI.create(minIOContainer.getS3URL()))
                .region(region)
                .credentialsProvider(awsCredsProvider)
                .forcePathStyle(true)
                .build();

        // Create a test file in MinIO
        String bucketName = "test-bucket";
        String key = "test-data.csv";
        s3Client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build());

        JdbcCatalog catalog = getJdbcCatalog();

        S3ReaderService s3ReaderService = new S3ReaderService(s3Client);
        IcebergTransformationService icebergService = new IcebergTransformationService(catalog, NAMESPACE, TABLE_NAME);
        icebergService.createTable();

        // Sample CSV header and data
        String csvHeader = "id,name,timestamp\n";
        String csvData = "1,John Doe,2023-10-01 10:00:00.000000000\n2,Jane Smith,2023-10-02 11:30:00.000000000\n";
        // Combine the header and data
        String csvContent = csvHeader + csvData;
        s3Client.putObject(PutObjectRequest.builder().bucket(bucketName).key(key).build(),
                RequestBody.fromString(csvContent));

        // Read data from S3
        InputStream inputStream = s3ReaderService.readDataFromS3(bucketName, key);

        // Transform and write to Iceberg
        icebergService.transformToIceberg(inputStream);

        // Verify that the table was created
        Table table = catalog.loadTable(TableIdentifier.of(NAMESPACE, TABLE_NAME));
        assertNotNull(table);
        try (var files = table.newScan().select("id", "name", "timestamp").planFiles()) {
            files.forEach(fileScanTask -> {
                assertEquals(1, fileScanTask.filesCount());
            });
        }
    }

    private static @NotNull JdbcCatalog getJdbcCatalog() {
        JdbcCatalog catalog = new JdbcCatalog();
        Configuration hadoopConfig = new Configuration();
        hadoopConfig.set("fs.defaultFS", "s3://test-bucket");
        hadoopConfig.set("fs.s3a.endpoint.region", Region.US_EAST_1.id());
        hadoopConfig.set("fs.s3a.endpoint", minIOContainer.getS3URL());

        hadoopConfig.set("warehouse", "s3://test-bucket");
        hadoopConfig.set("fs.s3a.threads.keepalivetime", "60"); // changed from "60s" to "60"
        hadoopConfig.set("fs.s3a.connection.establish.timeout", "60"); // changed from "60s" to "60"
        hadoopConfig.set("fs.s3a.connection.timeout", "60"); // changed from "60s" to "60"
        hadoopConfig.set("fs.s3a.multipart.purge.age", "24");

        // put env variables AWS_ACCESS_KEY=testuser;AWS_SECRET_KEY=testpassword
        hadoopConfig.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.EnvironmentVariableCredentialsProvider");
        hadoopConfig.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        hadoopConfig.set("fs.s3a.path.style.access", "true");

        catalog.setConf(hadoopConfig);
        Map<String, String> properties = new HashMap<>();
        properties.put("warehouse", "s3://test-bucket");
        properties.put("jdbc.url", postgresContainer.getJdbcUrl());
        properties.put("jdbc.user", postgresContainer.getUsername());
        properties.put("jdbc.password", postgresContainer.getPassword());
        // Add the connection URI property
        properties.put("uri", postgresContainer.getJdbcUrl());

        catalog.initialize("iceberg", properties);
        return catalog;
    }
}
