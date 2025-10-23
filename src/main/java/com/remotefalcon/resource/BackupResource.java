package com.remotefalcon.resource;

import com.remotefalcon.service.MongoBackupService;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import jakarta.inject.Inject;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;

@Path("/backup")
public class BackupResource {

    @Inject
    MongoBackupService backupService;

    @ConfigProperty(name = "backup.auth.token", defaultValue = "")
    String authToken;

    private boolean isAuthorized(String token) {
        if (authToken == null || authToken.isBlank()) {
            return false;
        }
        return authToken.equals(token);
    }

    @POST
    @Path("/trigger")
    public Response triggerBackup(@HeaderParam("X-Backup-Token") String token) {
        if (!isAuthorized(token)) {
            return Response.status(Status.UNAUTHORIZED).entity("Unauthorized").build();
        }

        try {
            backupService.runArchiveProcess();
            return Response.ok("Backup completed successfully").build();
        } catch (Exception e) {
            return Response.serverError()
                .entity("Backup failed: " + e.getMessage())
                .build();
        }
    }

    @POST
    @Path("/restore")
    public Response restoreBackup(@HeaderParam("X-Backup-Token") String token,
                                  @QueryParam("filename") String filename) {
        if (!isAuthorized(token)) {
            return Response.status(Status.UNAUTHORIZED).entity("Unauthorized").build();
        }

        if (filename == null || filename.trim().isEmpty()) {
            return Response.status(Response.Status.BAD_REQUEST)
                .entity("Missing required query parameter: filename")
                .build();
        }

        try {
            backupService.restoreFromBackup(filename);
            return Response.ok("Restore completed successfully").build();
        } catch (Exception e) {
            return Response.serverError()
                .entity("Restore failed: " + e.getMessage())
                .build();
        }
    }
}
