package com.appearnetworks.aiq.persistence;

import org.springframework.http.MediaType;

public final class StoredAttachment {
    private final MediaType contentType;
    private final byte[] data;
    private final long revision;

    public StoredAttachment(MediaType contentType, byte[] data, long revision) {
        this.contentType = contentType;
        this.data = data;
        this.revision = revision;
    }

    public MediaType getContentType() {
        return contentType;
    }

    public byte[] getData() {
        return data;
    }

    public long getRevision() {
        return revision;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StoredAttachment that = (StoredAttachment) o;

        if (revision != that.revision) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return (int) (revision ^ (revision >>> 32));
    }
}
