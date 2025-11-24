package com.remotefalcon.service;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.zip.GZIPInputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.bson.Document;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;

import io.quarkus.scheduler.Scheduled;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.jbosslog.JBossLog;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

@JBossLog
@ApplicationScoped
public class MongoBackupService {
    private static final DateTimeFormatter BACKUP_FILENAME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");
    private static final Pattern BACKUP_KEY_PATTERN = Pattern.compile("^mongo-backups/mongo-backup-(\\d{8}-\\d{6})\\.gz$");
    private static final int BACKUP_RETENTION_DAYS = 21;
    private static final String BACKUP_PREFIX = "mongo-backups/";

    @Inject
    MongoClient mongoClient;

    @Inject
    S3Client s3Client;

    @ConfigProperty(name = "s3.bucket.name")
    String s3BucketName;

    String tempDirectory = "/tmp/mongo-backups";

    @Scheduled(cron = "0 0 12 * * ?", timeZone = "America/Chicago")
    public void runArchiveProcess() {
        log.info("Running MongoDB backup process");

        try {
            // Create backup and upload to S3
            File backupFile = createMongoBackup();
            uploadToS3(backupFile);
            deleteOldBackups();

            log.info("MongoDB backup completed successfully");
        } catch (Exception e) {
            log.error("Failed to complete MongoDB backup", e);
        }
    }

    private File createMongoBackup() throws IOException {
        // Create temp directory if it doesn't exist
        Path tempPath = Paths.get(tempDirectory);
        if (!Files.exists(tempPath)) {
            Files.createDirectories(tempPath);
        }

        // Generate timestamped backup filename
        String timestamp = LocalDateTime.now().format(BACKUP_FILENAME_FORMATTER);
        String backupFileName = String.format("mongo-backup-%s.gz", timestamp);
        Path backupPath = tempPath.resolve(backupFileName);

        log.infof("Creating MongoDB backup at: %s", backupPath);

        // Get database and export all collections
        var database = mongoClient.getDatabase("remote-falcon");

        try (var gzipOutputStream = new java.util.zip.GZIPOutputStream(Files.newOutputStream(backupPath));
             var writer = new java.io.BufferedWriter(new java.io.OutputStreamWriter(gzipOutputStream, java.nio.charset.StandardCharsets.UTF_8))) {

            // Iterate through all collections
            for (String collectionName : database.listCollectionNames()) {
                log.infof("Backing up collection: %s", collectionName);
                var collection = database.getCollection(collectionName);

                // Write collection metadata
                writer.write(String.format("COLLECTION:%s\n", collectionName));

                // Export all documents as JSON (one per line)
                long count = 0;
                for (var document : collection.find()) {
                    writer.write(document.toJson());
                    writer.write("\n");
                    count++;
                }

                log.infof("Backed up %d documents from collection: %s", count, collectionName);
            }
        }

        File backupFile = backupPath.toFile();
        if (!backupFile.exists() || backupFile.length() == 0) {
            throw new RuntimeException("Backup file was not created or is empty");
        }

        log.infof("MongoDB backup created successfully: %s (size: %d bytes)", backupFile.getName(), backupFile.length());
        return backupFile;
    }

    private void uploadToS3(File backupFile) throws IOException {
        String s3Key = BACKUP_PREFIX + backupFile.getName();

        log.infof("Uploading backup to S3: s3://%s/%s", s3BucketName, s3Key);

        try {
            PutObjectRequest putRequest = PutObjectRequest.builder()
                .bucket(s3BucketName)
                .key(s3Key)
                .contentType("application/gzip")
                .build();

            s3Client.putObject(putRequest, RequestBody.fromFile(backupFile));

            log.infof("Successfully uploaded backup to S3: %s", s3Key);
        } finally {
            // Clean up temp file
            cleanupTempFiles(backupFile);
        }
    }

    private void cleanupTempFiles(File backupFile) {
        try {
            if (backupFile.exists()) {
                Files.delete(backupFile.toPath());
                log.infof("Deleted temporary backup file: %s", backupFile.getName());
            }

            // Clean up temp directory if empty
            Path tempPath = Paths.get(tempDirectory);
            if (Files.exists(tempPath) && Files.list(tempPath).findAny().isEmpty()) {
                Files.delete(tempPath);
                log.info("Deleted empty temp directory");
            }
        } catch (IOException e) {
            log.warnf("Failed to clean up temp files: %s", e.getMessage());
        }
    }

    public void restoreFromBackup(String backupFileName) throws IOException {
        log.infof("Starting restore from backup: %s", backupFileName);

        String s3Key = BACKUP_PREFIX + backupFileName;

        // Download backup from S3
        Path tempPath = Paths.get(tempDirectory);
        if (!Files.exists(tempPath)) {
            Files.createDirectories(tempPath);
        }

        Path localBackupPath = tempPath.resolve(backupFileName);

        log.infof("Downloading backup from S3: s3://%s/%s", s3BucketName, s3Key);

        GetObjectRequest getRequest = GetObjectRequest.builder()
            .bucket(s3BucketName)
            .key(s3Key)
            .build();

        s3Client.getObject(getRequest, ResponseTransformer.toFile(localBackupPath));

        try {
            // Restore from backup file
            restoreFromFile(localBackupPath.toFile());
            log.info("Restore completed successfully");
        } finally {
            // Clean up downloaded file
            cleanupTempFiles(localBackupPath.toFile());
        }
    }

    private void restoreFromFile(File backupFile) throws IOException {
        log.infof("Restoring from file: %s", backupFile.getName());

        var database = mongoClient.getDatabase("remote-falcon");
        MongoCollection<Document> currentCollection = null;
        long totalDocuments = 0;

        try (var fileInputStream = Files.newInputStream(backupFile.toPath());
             var gzipInputStream = new GZIPInputStream(fileInputStream);
             var reader = new BufferedReader(new InputStreamReader(gzipInputStream, java.nio.charset.StandardCharsets.UTF_8))) {

            String line;
            while ((line = reader.readLine()) != null) {
                if (line.startsWith("COLLECTION:")) {
                    String collectionName = line.substring("COLLECTION:".length());
                    currentCollection = database.getCollection(collectionName);
                    log.infof("Restoring collection: %s", collectionName);
                } else if (!line.trim().isEmpty() && currentCollection != null) {
                    // Parse JSON and insert document
                    Document document = Document.parse(line);
                    currentCollection.insertOne(document);
                    totalDocuments++;

                    if (totalDocuments % 1000 == 0) {
                        log.infof("Restored %d documents so far...", totalDocuments);
                    }
                }
            }
        }

        log.infof("Restore completed: %d documents restored", totalDocuments);
    }

    private void deleteOldBackups() {
        log.infof("Checking for backups older than %d days to delete", BACKUP_RETENTION_DAYS);

        LocalDateTime cutoff = LocalDateTime.now().minusDays(BACKUP_RETENTION_DAYS);
        String continuationToken = null;
        int deletedCount = 0;

        do {
            ListObjectsV2Request.Builder listRequestBuilder = ListObjectsV2Request.builder()
                .bucket(s3BucketName)
                .prefix(BACKUP_PREFIX);

            if (continuationToken != null) {
                listRequestBuilder.continuationToken(continuationToken);
            }

            ListObjectsV2Response response = s3Client.listObjectsV2(listRequestBuilder.build());

            for (var s3Object : response.contents()) {
                String key = s3Object.key();
                Matcher matcher = BACKUP_KEY_PATTERN.matcher(key);
                if (!matcher.matches()) {
                    continue;
                }

                String timestampPart = matcher.group(1);
                try {
                    LocalDateTime backupTime = LocalDateTime.parse(timestampPart, BACKUP_FILENAME_FORMATTER);
                    if (!backupTime.isAfter(cutoff)) {
                        deleteBackupObject(key);
                        deletedCount++;
                        log.infof("Deleted old backup: %s", key);
                    }
                } catch (DateTimeParseException e) {
                    log.warnf("Skipping backup with unparseable timestamp '%s': %s", key, e.getMessage());
                }
            }

            continuationToken = response.isTruncated() ? response.nextContinuationToken() : null;
        } while (continuationToken != null);

        log.infof("Old backup cleanup completed; deleted %d file(s)", deletedCount);
    }

    private void deleteBackupObject(String key) {
        DeleteObjectRequest deleteRequest = DeleteObjectRequest.builder()
            .bucket(s3BucketName)
            .key(key)
            .build();

        s3Client.deleteObject(deleteRequest);
    }
}
