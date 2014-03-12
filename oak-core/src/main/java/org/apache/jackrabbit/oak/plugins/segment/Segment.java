/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.segment;

import static com.google.common.base.Objects.equal;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkPositionIndexes;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.newConcurrentMap;
import static org.apache.jackrabbit.oak.plugins.segment.SegmentIdFactory.isDataSegmentId;
import static org.apache.jackrabbit.oak.plugins.segment.SegmentWriter.BLOCK_SIZE;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;

import com.google.common.base.Charsets;
import com.google.common.cache.Weigher;

public class Segment {

    /**
     * Number of bytes used for storing a record identifier. One byte
     * is used for identifying the segment and two for the record offset
     * within that segment.
     */
    static final int RECORD_ID_BYTES = 1 + 2;

    /**
     * The limit on segment references within one segment. Since record
     * identifiers use one byte to indicate the referenced segment, a single
     * segment can hold references to up to 255 segments plus itself.
     */
    static final int SEGMENT_REFERENCE_LIMIT = (1 << 8) - 1; // 255

    /**
     * The number of bytes (or bits of address space) to use for the
     * alignment boundary of segment records.
     */
    static final int RECORD_ALIGN_BITS = 2; // align at the four-byte boundary

    static int align(int value) {
        int mask = -1 << RECORD_ALIGN_BITS;
        return (value + ~mask) & mask;
    }

    /**
     * Maximum segment size. Record identifiers are stored as three-byte
     * sequences with the first byte indicating the segment and the next
     * two the offset within that segment. Since all records are aligned
     * at four-byte boundaries, the two bytes can address up to 256kB of
     * record data.
     */
    public static final int MAX_SEGMENT_SIZE = 1 << (16 + RECORD_ALIGN_BITS); // 256kB

    /**
     * The size limit for small values. The variable length of small values
     * is encoded as a single byte with the high bit as zero, which gives us
     * seven bits for encoding the length of the value.
     */
    static final int SMALL_LIMIT = 1 << 7;

    /**
     * The size limit for medium values. The variable length of medium values
     * is encoded as two bytes with the highest bits of the first byte set to
     * one and zero, which gives us 14 bits for encoding the length of the
     * value. And since small values are never stored as medium ones, we can
     * extend the size range to cover that many longer values.
     */
    static final int MEDIUM_LIMIT = (1 << (16 - 2)) + SMALL_LIMIT;

    public static final Weigher<UUID, Segment> WEIGHER =
            new Weigher<UUID, Segment>() {
                @Override
                public int weigh(UUID key, Segment value) {
                    return value.size();
                }
            };

    private static final UUID[] NO_REFS = new UUID[0];

    private final SegmentStore store;

    private final UUID uuid;

    private final UUID[] refids;

    private final ByteBuffer data;

    private final boolean current;

    /**
     * String records read from segment. Used to avoid duplicate
     * copies and repeated parsing of the same strings.
     */
    private final ConcurrentMap<Integer, String> strings = newConcurrentMap();

    /**
     * Template records read from segment. Used to avoid duplicate
     * copies and repeated parsing of the same templates.
     */
    private final ConcurrentMap<Integer, Template> templates = newConcurrentMap();

    private final byte[] recentCacheHits;

    public Segment(
            SegmentStore store, SegmentIdFactory factory,
            UUID uuid, ByteBuffer data) {
        this.store = checkNotNull(store);
        this.uuid = checkNotNull(uuid);
        this.data = checkNotNull(data);

        int refpos = data.position();
        if (isDataSegmentId(uuid)) {
            int refs = data.get(refpos) & 0xff;
            int roots = data.getShort(refpos + 1) & 0xffff;
            refpos += align(3 + roots * 3);
            refids = new UUID[refs];
            for (int i = 0; i < refs; i++) {
                refids[i] = factory.getSegmentId(
                        data.getLong(refpos + i * 16),
                        data.getLong(refpos + i * 16 + 8));
            }
            recentCacheHits = new byte[(data.remaining() + 16 * 8 - 1) / (16 * 8)];
        } else {
            recentCacheHits = null;
            refids = NO_REFS;
        }

        this.current = false;
    }

    Segment(SegmentStore store, UUID uuid, UUID[] refids, ByteBuffer data) {
        this.store = checkNotNull(store);
        this.uuid = checkNotNull(uuid);
        this.refids = checkNotNull(refids);
        this.data = checkNotNull(data);
        this.recentCacheHits = new byte[(data.remaining() + 16 * 8 - 1) / (16 * 8)];
        this.current = true;
    }

    public void dropOldCacheEntries() {
        checkState(recentCacheHits != null);
        for (Integer key : strings.keySet().toArray(new Integer[0])) {
            int bitpos = (pos(key, 1) - data.position()) / 16;
            if ((recentCacheHits[bitpos / 8] & (0x80 >>> (bitpos % 8))) == 0) {
                strings.remove(key);
            }
        }
        for (Integer key : templates.keySet().toArray(new Integer[0])) {
            int bitpos = (pos(key, 1) - data.position()) / 16;
            if ((recentCacheHits[bitpos / 8] & (0x80 >>> (bitpos % 8))) == 0) {
                templates.remove(key);
            }
        }
        Arrays.fill(recentCacheHits, (byte) 0);
    }

    /**
     * Maps the given record offset to the respective position within the
     * internal {@link #data} array. The validity of a record with the given
     * length at the given offset is also verified.
     *
     * @param offset record offset
     * @param length record length
     * @return position within the data array
     */
    private int pos(int offset, int length) {
        checkPositionIndexes(offset, offset + length, MAX_SEGMENT_SIZE);
        int pos = data.limit() - MAX_SEGMENT_SIZE + offset;
        checkState(pos >= data.position());
        return pos;
    }

    /**
     * Returns the store that contains this segment.
     *
     * @return containing segment store
     */
    @Nonnull
    SegmentStore getStore() {
        return store;
    }

    public UUID getSegmentId() {
        return uuid;
    }

    public List<UUID> getReferencedIds() {
        return Arrays.asList(refids);
    }

    public int size() {
        return data.remaining();
    }

    /**
     * Writes this segment to the given output stream.
     *
     * @param stream stream to which this segment will be written
     * @throws IOException on an IO error
     */
    public void writeTo(OutputStream stream) throws IOException {
        ByteBuffer buffer = data.duplicate();
        WritableByteChannel channel = Channels.newChannel(stream);
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }

    byte readByte(int offset) {
        return data.get(pos(offset, 1));
    }

    short readShort(int offset) {
        return data.getShort(pos(offset, 2));
    }

    int readInt(int offset) {
        return data.getInt(pos(offset, 4));
    }

    long readLong(int offset) {
        return data.getLong(pos(offset, 8));
    }

    /**
     * Returns the identified segment.
     *
     * @param uuid segment identifier
     * @return identified segment
     */
    @Nonnull
    Segment getSegment(UUID uuid) {
        if (equal(uuid, this.uuid)) {
            return this; // optimization for the common case (OAK-1031)
        }
        Segment segment = store.readSegment(uuid);
        checkState(segment != null); // sanity check
        return segment;
    }

    /**
     * Returns the segment that contains the identified record.
     *
     * @param id record identifier
     * @return segment that contains the identified record
     */
    Segment getSegment(RecordId id) {
        return getSegment(checkNotNull(id).getSegmentId());
    }

    /**
     * Reads the given number of bytes starting from the given position
     * in this segment.
     *
     * @param position position within segment
     * @param buffer target buffer
     * @param offset offset within target buffer
     * @param length number of bytes to read
     */
     void readBytes(int position, byte[] buffer, int offset, int length) {
        checkNotNull(buffer);
        checkPositionIndexes(offset, offset + length, buffer.length);
        ByteBuffer d = data.duplicate();
        d.position(pos(position, length));
        d.get(buffer, offset, length);
    }

    RecordId readRecordId(int offset) {
        int pos = pos(offset, RECORD_ID_BYTES);
        return internalReadRecordId(pos);
    }

    private RecordId internalReadRecordId(int pos) {
        UUID refid;
        int refpos = data.get(pos) & 0xff;
        if (refpos != 0xff) {
            refid = refids[refpos];
        } else {
            refid = uuid;
        }

        int offset =
                (((data.get(pos + 1) & 0xff) << 8) | (data.get(pos + 2) & 0xff))
                << RECORD_ALIGN_BITS;

        return new RecordId(refid, offset);
    }

    String readString(final RecordId id) {
        return getSegment(id).readString(id.getOffset());
    }

    private String readString(int offset) {
        String string = strings.get(offset);
        if (string == null) {
            string = loadString(offset);
            strings.putIfAbsent(offset, string); // only keep the first copy
        }
        int bitpos = (pos(offset, 1) - data.position()) / 16;
        recentCacheHits[bitpos / 8] |= 0x80 >>> (bitpos % 8);
        return string;
    }

    private String loadString(int offset) {
        int pos = pos(offset, 1);
        long length = internalReadLength(pos);
        if (length < SMALL_LIMIT) {
            byte[] bytes = new byte[(int) length];
            ByteBuffer buffer = data.duplicate();
            buffer.position(pos + 1);
            buffer.get(bytes);
            return new String(bytes, Charsets.UTF_8);
        } else if (length < MEDIUM_LIMIT) {
            byte[] bytes = new byte[(int) length];
            ByteBuffer buffer = data.duplicate();
            buffer.position(pos + 2);
            buffer.get(bytes);
            return new String(bytes, Charsets.UTF_8);
        } else if (length < Integer.MAX_VALUE) {
            int size = (int) ((length + BLOCK_SIZE - 1) / BLOCK_SIZE);
            ListRecord list =
                    new ListRecord(this, internalReadRecordId(pos + 8), size);
            SegmentStream stream = new SegmentStream(
                    store, new RecordId(uuid, offset), list, length);
            try {
                return stream.getString();
            } finally {
                stream.close();
            }
        } else {
            throw new IllegalStateException("String is too long: " + length);
        }
    }

    MapRecord readMap(RecordId id) {
        return new MapRecord(this, id);
    }

    Template readTemplate(final RecordId id) {
        return getSegment(id).readTemplate(id.getOffset());
    }

    private Template readTemplate(int offset) {
        Template template = templates.get(offset);
        if (template == null) {
            template = loadTemplate(offset);
            templates.putIfAbsent(offset, template); // only keep the first copy
        }
        int bitpos = (pos(offset, 1) - data.position()) / 16;
        recentCacheHits[bitpos / 8] |= 0x80 >>> (bitpos % 8);
        return template;
    }

    private Template loadTemplate(int offset) {
        int head = readInt(offset);
        boolean hasPrimaryType = (head & (1 << 31)) != 0;
        boolean hasMixinTypes = (head & (1 << 30)) != 0;
        boolean zeroChildNodes = (head & (1 << 29)) != 0;
        boolean manyChildNodes = (head & (1 << 28)) != 0;
        int mixinCount = (head >> 18) & ((1 << 10) - 1);
        int propertyCount = head & ((1 << 18) - 1);
        offset += 4;

        PropertyState primaryType = null;
        if (hasPrimaryType) {
            RecordId primaryId = readRecordId(offset);
            primaryType = PropertyStates.createProperty(
                    "jcr:primaryType", readString(primaryId), Type.NAME);
            offset += Segment.RECORD_ID_BYTES;
        }

        PropertyState mixinTypes = null;
        if (hasMixinTypes) {
            String[] mixins = new String[mixinCount];
            for (int i = 0; i < mixins.length; i++) {
                RecordId mixinId = readRecordId(offset);
                mixins[i] =  readString(mixinId);
                offset += Segment.RECORD_ID_BYTES;
            }
            mixinTypes = PropertyStates.createProperty(
                    "jcr:mixinTypes", Arrays.asList(mixins), Type.NAMES);
        }

        String childName = Template.ZERO_CHILD_NODES;
        if (manyChildNodes) {
            childName = Template.MANY_CHILD_NODES;
        } else if (!zeroChildNodes) {
            RecordId childNameId = readRecordId(offset);
            childName = readString(childNameId);
            offset += Segment.RECORD_ID_BYTES;
        }

        PropertyTemplate[] properties =
                new PropertyTemplate[propertyCount];
        for (int i = 0; i < properties.length; i++) {
            RecordId propertyNameId = readRecordId(offset);
            offset += Segment.RECORD_ID_BYTES;
            byte type = readByte(offset++);
            properties[i] = new PropertyTemplate(
                    i, readString(propertyNameId),
                    Type.fromTag(Math.abs(type), type < 0));
        }

        return new Template(
                primaryType, mixinTypes, properties, childName);
    }

    long readLength(RecordId id) {
        return getSegment(id).readLength(id.getOffset());
    }

    long readLength(int offset) {
        return internalReadLength(pos(offset, 1));
    }

    private long internalReadLength(int pos) {
        int length = data.get(pos++) & 0xff;
        if ((length & 0x80) == 0) {
            return length;
        } else if ((length & 0x40) == 0) {
            return ((length & 0x3f) << 8
                    | data.get(pos++) & 0xff)
                    + SMALL_LIMIT;
        } else {
            return (((long) length & 0x3f) << 56
                    | ((long) (data.get(pos++) & 0xff)) << 48
                    | ((long) (data.get(pos++) & 0xff)) << 40
                    | ((long) (data.get(pos++) & 0xff)) << 32
                    | ((long) (data.get(pos++) & 0xff)) << 24
                    | ((long) (data.get(pos++) & 0xff)) << 16
                    | ((long) (data.get(pos++) & 0xff)) << 8
                    | ((long) (data.get(pos++) & 0xff)))
                    + MEDIUM_LIMIT;
        }
    }

    long readBlobLength(int offset) {
        long high = readInt(offset + 2);
        long low = readInt(offset + 6);
        return high << 32 | low;
    }

    //------------------------------------------------------------< Object >--

    @Override
    public String toString() {
        StringWriter string = new StringWriter();
        PrintWriter writer = new PrintWriter(string);

        int rootcount = 0;
        int length = data.remaining();
        if (!current) {
            rootcount = data.getShort(data.position() + 1) &0xffff;
            length -= (align(3 + rootcount * 3) + refids.length * 16);
        }

        writer.format(
                "Segment %s (%d bytes, %d ref%s, %d root%s)%n",
                uuid, length,
                refids.length, (refids.length != 1 ? "s" : ""),
                rootcount, (rootcount != 1 ? "s" : ""));
        writer.println("--------------------------------------------------------------------------");
        if (refids.length > 0) {
            for (int i = 0; i < refids.length; i++) {
                writer.format("reference %02x: %s%n", i, refids[i]);
            }
            writer.println("--------------------------------------------------------------------------");
        }
        int pos = data.limit() - ((length + 15) & ~15);
        while (pos < data.limit()) {
            writer.format("%04x: ", (MAX_SEGMENT_SIZE - data.limit() + pos) >> RECORD_ALIGN_BITS);
            for (int i = 0; i < 16; i++) {
                if (i > 0 && i % 4 == 0) {
                    writer.append(' ');
                }
                if (pos + i >= data.position()) {
                    byte b = data.get(pos + i);
                    writer.format("%02x ", b & 0xff);
                } else {
                    writer.append("   ");
                }
            }
            writer.append(' ');
            for (int i = 0; i < 16; i++) {
                if (pos + i >= data.position()) {
                    byte b = data.get(pos + i);
                    if (b >= ' ' && b < 127) {
                        writer.append((char) b);
                    } else {
                        writer.append('.');
                    }
                } else {
                    writer.append(' ');
                }
            }
            writer.println();
            pos += 16;
        }
        writer.println("--------------------------------------------------------------------------");

        writer.close();
        return string.toString();
    }

}
