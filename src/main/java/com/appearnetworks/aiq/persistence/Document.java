package com.appearnetworks.aiq.persistence;

import com.appearnetworks.aiq.integrationframework.integration.BusinessDocument;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class Document extends BusinessDocument {

    private final ObjectNode body;
    public final ConcurrentMap<String, StoredAttachment> attachments = new ConcurrentHashMap<>();

    public Document(String _id, String _type, long _rev, ObjectNode body) {
        super(_id, _type, _rev);
        this.body = body;
    }

    public ObjectNode getBody() {
        return body;
    }

    public long bumpRevision() {
        return ++_rev;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Document that = (Document) o;

        if (_rev != that._rev) return false;
        if (_id != null ? !_id.equals(that._id) : that._id != null) return false;
        if (_type != null ? !_type.equals(that._type) : that._type != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = _id != null ? _id.hashCode() : 0;
        result = 31 * result + (_type != null ? _type.hashCode() : 0);
        result = 31 * result + (int) (_rev ^ (_rev >>> 32));
        return result;
    }

}
