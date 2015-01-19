package com.appearnetworks.aiq.persistence;

import com.appearnetworks.aiq.integrationframework.integration.Attachment;
import com.appearnetworks.aiq.integrationframework.integration.DocumentAndAttachmentRevision;
import com.appearnetworks.aiq.integrationframework.integration.DocumentReference;
import com.appearnetworks.aiq.integrationframework.integration.UpdateException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.Before;
import org.junit.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.util.FileCopyUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collection;

import static net.javacrumbs.jsonunit.JsonAssert.assertJsonEquals;
import static org.junit.Assert.*;

public class InMemoryPersistenceServiceTest {

    private static final String DOC_ID = "docId";
    private static final String DOC_TYPE = "docType";
    private static final String DATA = "FOO";
    private static final String NEW_DATA = "BAR";
    private static final String NOT_THERE = "foo";
    private static final String NAME = "attach";
    private static final MediaType ATTACHMENT_CONTENT_TYPE = MediaType.APPLICATION_OCTET_STREAM;
    private static final byte[] ATTACHMENT_DATA = new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9};
    private static final byte[] ATTACHMENT_DATA2 = new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9};

    private static final String ATTACHMENTS = "_attachments";
    private static final String REV = "_rev";
    private static final String CONTENT_TYPE = "content_type";

    private ObjectMapper mapper = new ObjectMapper();

    private PersistenceService persistenceService;
    private ObjectNode document;

    @Before
    public void setup() {
        persistenceService = new InMemoryPersistenceService();
        document = mapper.createObjectNode();
        document.put("_id", DOC_ID);
        document.put("_type", DOC_TYPE);
        document.put("data", DATA);
    }

    @Test
    public void empty() {
        assertEquals(0, persistenceService.list().size());
        assertNull(persistenceService.retrieve(DOC_ID));
        assertNull(persistenceService.retrieveAttachment(DOC_ID, NAME));
    }

    @Test
    public void insert() throws UpdateException {
        long revision = persistenceService.insert(new DocumentReference(DOC_ID, DOC_TYPE, 0), document);
        assertTrue(revision > 0);

        Collection<DocumentReference> documents = persistenceService.list();
        assertEquals(1, documents.size());
        DocumentReference documentReference = documents.iterator().next();
        assertEquals(new DocumentReference(DOC_ID, DOC_TYPE, revision), documentReference);
        document.put("_rev", revision);
        assertJsonEquals(document, persistenceService.retrieve(DOC_ID));

        assertNull(persistenceService.retrieve(NOT_THERE));
    }

    @Test
    public void insertConflict() throws UpdateException {
        persistenceService.insert(new DocumentReference(DOC_ID, DOC_TYPE, 0), document);

        try {
            persistenceService.insert(new DocumentReference(DOC_ID, DOC_TYPE, 0), document);
            fail("should throw UpdateException(CONFLICT)");
        } catch (UpdateException e) {
            assertEquals(HttpStatus.CONFLICT, e.getStatusCode());
        }
    }

    @Test
    public void update() throws UpdateException {
        long revision = persistenceService.insert(new DocumentReference(DOC_ID, DOC_TYPE, 0), document);

        document.put("data", NEW_DATA);

        long newRevision = persistenceService.update(new DocumentReference(DOC_ID, DOC_TYPE, revision), document);
        assertTrue(newRevision > revision);

        Collection<DocumentReference> documents = persistenceService.list();
        assertEquals(1, documents.size());
        DocumentReference documentReference = documents.iterator().next();
        assertEquals(new DocumentReference(DOC_ID, DOC_TYPE, newRevision), documentReference);
        document.put("_rev", newRevision);
        assertJsonEquals(document, persistenceService.retrieve(DOC_ID));
    }

    @Test
    public void updateConflict() throws UpdateException {
        long revision = persistenceService.insert(new DocumentReference(DOC_ID, DOC_TYPE, 0), document);

        document.put("data", NEW_DATA);

        try {
            persistenceService.update(new DocumentReference(DOC_ID, DOC_TYPE, revision-1), document);
            fail("should throw UpdateException(PRECONDITION_FAILED)");
        } catch (UpdateException e) {
            assertEquals(HttpStatus.PRECONDITION_FAILED, e.getStatusCode());
        }
    }

    @Test
    public void delete() throws UpdateException {
        long revision = persistenceService.insert(new DocumentReference(DOC_ID, DOC_TYPE, 0), document);

        persistenceService.delete(new DocumentReference(DOC_ID, DOC_TYPE, revision));

        Collection<DocumentReference> documents = persistenceService.list();
        assertEquals(0, documents.size());
        assertNull(persistenceService.retrieve(DOC_ID));
    }

    @Test
    public void deleteConflict() throws UpdateException {
        long revision = persistenceService.insert(new DocumentReference(DOC_ID, DOC_TYPE, 0), document);

        persistenceService.delete(new DocumentReference(DOC_ID, DOC_TYPE, revision));

        try {
            persistenceService.delete(new DocumentReference(DOC_ID, DOC_TYPE, revision-1));
            fail("should throw UpdateException(PRECONDITION_FAILED)");
        } catch (UpdateException e) {
            assertEquals(HttpStatus.PRECONDITION_FAILED, e.getStatusCode());
        }
    }

    @Test
    public void noAttachments() throws UpdateException {
        persistenceService.insert(new DocumentReference(DOC_ID, DOC_TYPE, 0), document);

        assertNull(persistenceService.retrieve(DOC_ID).get(ATTACHMENTS));
        assertNull(persistenceService.retrieveAttachment(DOC_ID, NAME));
    }

    @Test
    public void insertAttachment() throws UpdateException, IOException {
        long documentRevision = persistenceService.insert(new DocumentReference(DOC_ID, DOC_TYPE, 0), document);

        DocumentAndAttachmentRevision documentAndAttachmentRevision = persistenceService.insertAttachment(DOC_ID, NAME, new ByteArrayInputStream(ATTACHMENT_DATA), ATTACHMENT_CONTENT_TYPE, ATTACHMENT_DATA.length);
        assertTrue(documentAndAttachmentRevision.documentRev > documentRevision);
        assertTrue(documentAndAttachmentRevision.attachmentRev > 0);

        JsonNode attachments = persistenceService.retrieve(DOC_ID).get(ATTACHMENTS);
        JsonNode attachment = attachments.get(NAME);
        assertEquals(documentAndAttachmentRevision.attachmentRev, attachment.get(REV).longValue());
        assertEquals(ATTACHMENT_CONTENT_TYPE.toString(), attachment.get(CONTENT_TYPE).textValue());

        Attachment retrievedAttachment = persistenceService.retrieveAttachment(DOC_ID, NAME);
        assertEquals(ATTACHMENT_CONTENT_TYPE, retrievedAttachment.contentType);
        assertEquals(documentAndAttachmentRevision.attachmentRev, retrievedAttachment.revision);
        assertArrayEquals(ATTACHMENT_DATA, FileCopyUtils.copyToByteArray(retrievedAttachment.data));
        assertEquals(ATTACHMENT_DATA.length, retrievedAttachment.contentLength);
    }

    @Test
    public void insertAttachmentNotFound() throws UpdateException, IOException {
        try {
            persistenceService.insertAttachment(DOC_ID, NAME, new ByteArrayInputStream(ATTACHMENT_DATA), ATTACHMENT_CONTENT_TYPE, ATTACHMENT_DATA.length);
            fail("should throw UpdateException(NOT_FOUND)");
        } catch (UpdateException e) {
            assertEquals(HttpStatus.NOT_FOUND, e.getStatusCode());
        }
    }

    @Test
    public void insertAttachmentConflict() throws UpdateException, IOException {
        persistenceService.insert(new DocumentReference(DOC_ID, DOC_TYPE, 0), document);

        persistenceService.insertAttachment(DOC_ID, NAME, new ByteArrayInputStream(ATTACHMENT_DATA), ATTACHMENT_CONTENT_TYPE, ATTACHMENT_DATA.length);

        try {
            persistenceService.insertAttachment(DOC_ID, NAME, new ByteArrayInputStream(ATTACHMENT_DATA), ATTACHMENT_CONTENT_TYPE, ATTACHMENT_DATA.length);
            fail("should throw UpdateException(CONFLICT)");
        } catch (UpdateException e) {
            assertEquals(HttpStatus.CONFLICT, e.getStatusCode());
        }
    }

    @Test
    public void updateAttachment() throws UpdateException, IOException {
        persistenceService.insert(new DocumentReference(DOC_ID, DOC_TYPE, 0), document);

        DocumentAndAttachmentRevision documentAndAttachmentRevision = persistenceService.insertAttachment(
                DOC_ID, NAME, new ByteArrayInputStream(ATTACHMENT_DATA), ATTACHMENT_CONTENT_TYPE, ATTACHMENT_DATA.length);

        DocumentAndAttachmentRevision documentAndAttachmentRevision2 = persistenceService.updateAttachment(
                DOC_ID, NAME, new ByteArrayInputStream(ATTACHMENT_DATA2), documentAndAttachmentRevision.attachmentRev, ATTACHMENT_CONTENT_TYPE, ATTACHMENT_DATA2.length);
        assertTrue(documentAndAttachmentRevision2.documentRev > documentAndAttachmentRevision.documentRev);
        assertTrue(documentAndAttachmentRevision2.attachmentRev > documentAndAttachmentRevision.attachmentRev);

        JsonNode attachments = persistenceService.retrieve(DOC_ID).get(ATTACHMENTS);
        JsonNode attachment = attachments.get(NAME);
        assertEquals(documentAndAttachmentRevision2.attachmentRev, attachment.get(REV).longValue());
        assertEquals(ATTACHMENT_CONTENT_TYPE.toString(), attachment.get(CONTENT_TYPE).textValue());

        Attachment retrievedAttachment = persistenceService.retrieveAttachment(DOC_ID, NAME);
        assertEquals(ATTACHMENT_CONTENT_TYPE, retrievedAttachment.contentType);
        assertEquals(documentAndAttachmentRevision2.attachmentRev, retrievedAttachment.revision);
        assertArrayEquals(ATTACHMENT_DATA2, FileCopyUtils.copyToByteArray(retrievedAttachment.data));
        assertEquals(ATTACHMENT_DATA2.length, retrievedAttachment.contentLength);
    }

    @Test
    public void updateAttachmentNotFound() throws UpdateException, IOException {
        try {
            persistenceService.updateAttachment(DOC_ID, NAME, new ByteArrayInputStream(ATTACHMENT_DATA), 1, ATTACHMENT_CONTENT_TYPE, ATTACHMENT_DATA.length);
            fail("should throw UpdateException(NOT_FOUND)");
        } catch (UpdateException e) {
            assertEquals(HttpStatus.NOT_FOUND, e.getStatusCode());
        }
    }

    @Test
    public void updateAttachmentNotFound2() throws UpdateException, IOException {
        persistenceService.insert(new DocumentReference(DOC_ID, DOC_TYPE, 0), document);

        try {
            persistenceService.updateAttachment(DOC_ID, NAME, new ByteArrayInputStream(ATTACHMENT_DATA), 1, ATTACHMENT_CONTENT_TYPE, ATTACHMENT_DATA.length);
            fail("should throw UpdateException(PRECONDITION_FAILED)");
        } catch (UpdateException e) {
            assertEquals(HttpStatus.PRECONDITION_FAILED, e.getStatusCode());
        }
    }

    @Test
    public void updateAttachmentConflict() throws UpdateException, IOException {
        persistenceService.insert(new DocumentReference(DOC_ID, DOC_TYPE, 0), document);

        persistenceService.insertAttachment(DOC_ID, NAME, new ByteArrayInputStream(ATTACHMENT_DATA), ATTACHMENT_CONTENT_TYPE, ATTACHMENT_DATA.length);

        try {
            persistenceService.updateAttachment(DOC_ID, NAME, new ByteArrayInputStream(ATTACHMENT_DATA), 0, ATTACHMENT_CONTENT_TYPE, ATTACHMENT_DATA.length);
            fail("should throw UpdateException(PRECONDITION_FAILED)");
        } catch (UpdateException e) {
            assertEquals(HttpStatus.PRECONDITION_FAILED, e.getStatusCode());
        }
    }

    @Test
    public void deleteAttachment() throws UpdateException, IOException {
        persistenceService.insert(new DocumentReference(DOC_ID, DOC_TYPE, 0), document);

        DocumentAndAttachmentRevision documentAndAttachmentRevision = persistenceService.insertAttachment(
                DOC_ID, NAME, new ByteArrayInputStream(ATTACHMENT_DATA), ATTACHMENT_CONTENT_TYPE, ATTACHMENT_DATA.length);

        long documentRev = persistenceService.deleteAttachment(DOC_ID, NAME, documentAndAttachmentRevision.attachmentRev);
        assertTrue(documentRev > documentAndAttachmentRevision.documentRev);

        assertNull(persistenceService.retrieve(DOC_ID).get(ATTACHMENTS));

        assertNull(persistenceService.retrieveAttachment(DOC_ID, NAME));
    }

    @Test
    public void deleteAttachmentNotFound() throws UpdateException, IOException {
        try {
            persistenceService.deleteAttachment(DOC_ID, NAME, 1);
            fail("should throw UpdateException(NOT_FOUND)");
        } catch (UpdateException e) {
            assertEquals(HttpStatus.NOT_FOUND, e.getStatusCode());
        }
    }

    @Test
    public void deleteAttachmentNotFound2() throws UpdateException, IOException {
        persistenceService.insert(new DocumentReference(DOC_ID, DOC_TYPE, 0), document);

        try {
            persistenceService.deleteAttachment(DOC_ID, NAME, 1);
            fail("should throw UpdateException(PRECONDITION_FAILED)");
        } catch (UpdateException e) {
            assertEquals(HttpStatus.PRECONDITION_FAILED, e.getStatusCode());
        }
    }

    @Test
    public void deleteAttachmentConflict() throws UpdateException, IOException {
        persistenceService.insert(new DocumentReference(DOC_ID, DOC_TYPE, 0), document);

        persistenceService.insertAttachment(DOC_ID, NAME, new ByteArrayInputStream(ATTACHMENT_DATA), ATTACHMENT_CONTENT_TYPE, ATTACHMENT_DATA.length);

        try {
            persistenceService.deleteAttachment(DOC_ID, NAME, 0);
            fail("should throw UpdateException(PRECONDITION_FAILED)");
        } catch (UpdateException e) {
            assertEquals(HttpStatus.PRECONDITION_FAILED, e.getStatusCode());
        }
    }

}
