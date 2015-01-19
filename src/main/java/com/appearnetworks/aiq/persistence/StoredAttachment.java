package com.appearnetworks.aiq.persistence;

import org.springframework.http.MediaType;

public final class StoredAttachment {
    public final MediaType contentType;
    public final byte[] data;
    public final long revision;

    public StoredAttachment(MediaType contentType, byte[] data, long revision) {
        this.contentType = contentType;
        this.data = data;
        this.revision = revision;
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
