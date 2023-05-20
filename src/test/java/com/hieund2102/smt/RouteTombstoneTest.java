package com.hieund2102.smt;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class RouteTombstoneTest {
    String topicPrefx = "tombstone-";
    private final RouteTombstone<SourceRecord> xform = new RouteTombstone();


    final Schema simpleStructSchema = SchemaBuilder.struct().name("name").version(1).doc("doc").field("magic", Schema.OPTIONAL_INT64_SCHEMA).build();
    final Struct simpleStruct = new Struct(simpleStructSchema).put("magic", 42L);


    @Test
    void applyToMessageWithNullKey() {
        xform.configure(xform.config().defaultValues());
        SourceRecord record = new SourceRecord(null, null, "test", 0, simpleStructSchema, simpleStruct);
        SourceRecord output = xform.apply(record);
        assertEquals(record, output);

        SourceRecord tombstone = new SourceRecord(null, null, "test", 0, simpleStructSchema, null);
        SourceRecord outputTombstone = xform.apply(tombstone);
        assertEquals(outputTombstone.topic(), tombstone.topic());
    }

    @Test
    void applyToMessageWithKey() {
        xform.configure(xform.config().defaultValues());
        SourceRecord record = new SourceRecord(null, null, "test", 0, simpleStructSchema, simpleStruct, simpleStructSchema, simpleStruct);
        SourceRecord output = xform.apply(record);
        assertEquals(record, output);

        SourceRecord tombstone = new SourceRecord(null, null, "test", 0, simpleStructSchema, simpleStruct, simpleStructSchema, null);
        SourceRecord outputTombstone = xform.apply(tombstone);
        assertNotEquals(outputTombstone.topic(), tombstone.topic());

    }
}