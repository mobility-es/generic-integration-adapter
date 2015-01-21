package com.appearnetworks.aiq.persistence;

import com.appearnetworks.aiq.integrationframework.integration.Attachment;
import com.appearnetworks.aiq.integrationframework.integration.AttachmentReference;
import com.appearnetworks.aiq.integrationframework.integration.DocumentAndAttachmentRevision;
import com.appearnetworks.aiq.integrationframework.integration.DocumentReference;
import com.appearnetworks.aiq.integrationframework.integration.UpdateException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Repository;
import org.springframework.util.FileCopyUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Repository
public class InMemoryPersistenceService implements PersistenceService {

    private static final String ATTACHMENTS = "_attachments";
    private static final String REV = "_rev";

    private final ObjectMapper mapper = new ObjectMapper();

    private final ConcurrentMap<String, StoredDocument> documents = new ConcurrentHashMap<>();

    @Override
    public Collection<DocumentReference> list() {
        ArrayList<DocumentReference> documentReferences = new ArrayList<>(documents.size());
        for (StoredDocument document : documents.values()) {
            documentReferences.add(new DocumentReference(document._id, document._type, document.get_rev()));
        }
        return documentReferences;
    }

    @Override
    public ObjectNode retrieve(String docId) {
        StoredDocument doc = documents.get(docId);
        if (doc == null) return null;

        doc.body.put(REV, doc.get_rev());

        ObjectNode attachments = mapper.createObjectNode();
        for (Map.Entry<String, StoredAttachment> entry : doc.attachments.entrySet()) {
            attachments.put(entry.getKey(), mapper.valueToTree(new AttachmentReference(entry.getValue().revision, entry.getValue().contentType)));
        }
        if (attachments.size() > 0) {
            doc.body.put(ATTACHMENTS, attachments);
        } else {
            doc.body.remove(ATTACHMENTS);
        }

        return doc.body;
    }

    @Override
    public long insert(DocumentReference docRef, ObjectNode body) throws UpdateException {
        long initialRevision = 1;

        StoredDocument existingDocument = documents.putIfAbsent(
                docRef._id,
                new StoredDocument(docRef._id, docRef._type, initialRevision, body));

        if (existingDocument == null) {
            return initialRevision;
        } else {
            throw new UpdateException(HttpStatus.CONFLICT);
        }
    }

    @Override
    public long update(DocumentReference docRef, ObjectNode body) throws UpdateException {
        long updatedRevision = docRef._rev + 1;

        boolean wasReplaced = documents.replace(
                docRef._id,
                new StoredDocument(docRef._id, docRef._type, docRef._rev, null),
                new StoredDocument(docRef._id, docRef._type, updatedRevision, body));

        if (wasReplaced)
            return updatedRevision;
        else
            throw new UpdateException(HttpStatus.PRECONDITION_FAILED);
    }

    @Override
    public void delete(DocumentReference docRef) throws UpdateException {
        boolean wasRemoved = documents.remove(
                docRef._id,
                new StoredDocument(docRef._id, docRef._type, docRef._rev, null));

        if (wasRemoved)
            return;
        else
            throw new UpdateException(HttpStatus.PRECONDITION_FAILED);
    }

    @Override
    public Attachment retrieveAttachment(String docId, String name) {
        StoredDocument document = documents.get(docId);
        if (document == null) {
            return null;
        }

        StoredAttachment attachment = document.attachments.get(name);
        if (attachment == null) {
            return null;
        }

        return new Attachment(attachment.contentType, attachment.data.length, new ByteArrayInputStream(attachment.data), attachment.revision);
    }

    @Override
    public DocumentAndAttachmentRevision insertAttachment(String docId, String name, InputStream data, MediaType contentType, long contentLength) throws UpdateException, IOException {
        StoredDocument document = documents.get(docId);
        if (document == null) {
            throw new UpdateException(HttpStatus.NOT_FOUND);
        } else {
            long initialRevision = 1;
            StoredAttachment existingAttachment = document.attachments.putIfAbsent(
                    name,
                    new StoredAttachment(contentType, FileCopyUtils.copyToByteArray(data), initialRevision)
            );
            if (existingAttachment == null) {
                return new DocumentAndAttachmentRevision(document.bumpRevision(), initialRevision);
            } else {
                throw new UpdateException(HttpStatus.CONFLICT);
            }
        }
    }

    @Override
    public DocumentAndAttachmentRevision updateAttachment(String docId, String name, InputStream data, long revision, MediaType contentType, long contentLength) throws UpdateException, IOException {
        StoredDocument document = documents.get(docId);
        if (document == null) {
            throw new UpdateException(HttpStatus.NOT_FOUND);
        } else {
            long newRevision = revision + 1;
            boolean wasReplaced = document.attachments.replace(
                    name,
                    new StoredAttachment(null, null, revision),
                    new StoredAttachment(contentType, FileCopyUtils.copyToByteArray(data), newRevision)
            );
            if (wasReplaced)
                return new DocumentAndAttachmentRevision(document.bumpRevision(), newRevision);
            else
                throw new UpdateException(HttpStatus.PRECONDITION_FAILED);
        }
    }

    @Override
    public long deleteAttachment(String docId, String name, long revision) throws UpdateException {
        StoredDocument document = documents.get(docId);
        if (document == null) {
            throw new UpdateException(HttpStatus.NOT_FOUND);
        } else {
            boolean wasRemoved = document.attachments.remove(
                    name,
                    new StoredAttachment(null, null, revision)
            );

            if (wasRemoved)
                return document.bumpRevision();
            else
                throw new UpdateException(HttpStatus.PRECONDITION_FAILED);
        }
    }
}
