package com.appearnetworks.aiq.persistence;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class StoredDocument {

    public final String _id;
    public final String _type;
    private long _rev;
    public final ObjectNode body;
    public final ConcurrentMap<String, StoredAttachment> attachments;

    public StoredDocument(String _id, String _type, long _rev, ObjectNode body) {
        this._id = _id;
        this._type = _type;
        this._rev = _rev;
        this.body = body;
        this.attachments = new ConcurrentHashMap<>();
    }

    public long get_rev() {
        return _rev;
    }

    public long bumpRevision() {
        return ++_rev;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StoredDocument that = (StoredDocument) o;

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
