package com.appearnetworks.aiq.persistence;

import com.appearnetworks.aiq.integrationframework.integration.DocumentAndAttachmentRevision;
import com.appearnetworks.aiq.integrationframework.integration.DocumentReference;
import com.appearnetworks.aiq.integrationframework.integration.UpdateException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.http.MediaType;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;

interface PersistenceService {

    Collection<DocumentReference> list();

    ObjectNode retrieve(String docId);

    long insert(DocumentReference docRef, ObjectNode doc) throws UpdateException;

    long update(DocumentReference docRef, ObjectNode doc) throws UpdateException;

    void delete(DocumentReference docRef) throws UpdateException;

    DocumentAndAttachmentRevision insertAttachment(String docId, String name, InputStream data, MediaType contentType, long contentLength) throws UpdateException, IOException;

    DocumentAndAttachmentRevision updateAttachment(String docId, String name, InputStream data, long revision, MediaType contentType, long contentLength) throws UpdateException, IOException;

    long deleteAttachment(String docId, String name, long revision) throws UpdateException;
}
