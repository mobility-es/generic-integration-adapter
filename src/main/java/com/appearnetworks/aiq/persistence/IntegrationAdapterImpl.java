package com.appearnetworks.aiq.persistence;

import com.appearnetworks.aiq.integrationframework.integration.Attachment;
import com.appearnetworks.aiq.integrationframework.integration.DocumentAndAttachmentRevision;
import com.appearnetworks.aiq.integrationframework.integration.DocumentReference;
import com.appearnetworks.aiq.integrationframework.integration.IntegrationAdapterBase;
import com.appearnetworks.aiq.integrationframework.integration.UpdateException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.logging.Logger;

@Component
public class IntegrationAdapterImpl extends IntegrationAdapterBase {

    private static Logger log = Logger.getLogger(IntegrationAdapterImpl.class.getName());

    @Autowired
    private PersistenceService persistenceService;

    @Override
    public Collection<DocumentReference> findByUser(String userId) {
        return persistenceService.list();
    }

    @Override
    public Object retrieveDocument(String docType, String docId) {
        return persistenceService.retrieve(docId);
    }

    @Override
    public Attachment retrieveAttachment(String docType, String docId, String name) {
        return persistenceService.retrieveAttachment(docType, docId, name);
    }

    @Override
    public long insertDocument(String userId, String deviceId, DocumentReference docRef, ObjectNode doc) throws UpdateException {
        try {
            log.info("Inserting " + docRef);
            return persistenceService.insert(docRef, doc);
        } catch (UpdateException e) {
            log.warning("Failure: " + e.getStatusCode());
            throw e;
        }
    }

    @Override
    public long updateDocument(String userId, String deviceId, DocumentReference docRef, ObjectNode doc) throws UpdateException {
        try {
            log.info("Updating " + docRef);
            return persistenceService.update(docRef, doc);
        } catch (UpdateException e) {
            log.warning("Failure: " + e.getStatusCode());
            throw e;
        }
    }

    @Override
    public void deleteDocument(String userId, String deviceId, DocumentReference docRef) throws UpdateException {
        try {
            log.info("Deleting " + docRef);
            persistenceService.delete(docRef);
        } catch (UpdateException e) {
            log.warning("Failure: " + e.getStatusCode());
            throw e;
        }
    }

    @Override
    public DocumentAndAttachmentRevision insertAttachment(String userId, String deviceId, String docType, String docId, String name, MediaType contentType, long contentLength, InputStream content) throws UpdateException, IOException {
        try {
            log.info("Inserting attachment: " + docType + ", " + docId + ", " + name + ", " + contentType.toString() +", " + contentLength);
            return persistenceService.insertAttachment(docId, name, content, contentType, contentLength);
        } catch (UpdateException e) {
            log.warning("Failure: " + e.getStatusCode());
            throw e;
        }
    }

    @Override
    public DocumentAndAttachmentRevision updateAttachment(String userId, String deviceId, String docType, String docId, String name, long revision, MediaType contentType, long contentLength, InputStream content) throws UpdateException, IOException {
        try {
            log.info("Updating attachment: " + docType + ", " + docId + ", " + name + ", " + contentType.toString() +", " + contentLength);
            return persistenceService.updateAttachment(docId, name, content, revision, contentType, contentLength);
        } catch (UpdateException e) {
            log.warning("Failure: " + e.getStatusCode());
            throw e;
        }
    }

    @Override
    public long deleteAttachment(String userId, String deviceId, String docType, String docId, String name, long revision) throws UpdateException {
        try {
            log.info("Deleting attachment: " + docType + ", " + docId + ", " + name);
            return persistenceService.deleteAttachment(docId, name, revision);
        } catch (UpdateException e) {
            log.warning("Failure: " + e.getStatusCode());
            throw e;
        }
    }
}
