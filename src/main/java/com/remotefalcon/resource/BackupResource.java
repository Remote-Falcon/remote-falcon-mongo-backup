package com.remotefalcon.resource;

import com.remotefalcon.service.MongoBackupService;

import jakarta.inject.Inject;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Response;

@Path("/backup")
public class BackupResource {

    @Inject
    MongoBackupService backupService;

    @POST
    @Path("/trigger")
    public Response triggerBackup() {
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
    public Response restoreBackup(@QueryParam("filename") String filename) {
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
