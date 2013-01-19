package com.googlecode.lazyrecords.azuretable.mappings;

import com.googlecode.lazyrecords.*;
import com.googlecode.lazyrecords.mappings.DateMapping;
import com.googlecode.lazyrecords.mappings.IntegerMapping;
import com.googlecode.lazyrecords.mappings.LongMapping;
import com.googlecode.lazyrecords.mappings.StringMappings;
import com.googlecode.totallylazy.*;

import com.googlecode.totallylazy.time.DateFormatConverter;
import com.googlecode.totallylazy.time.Dates;
import com.microsoft.windowsazure.services.core.storage.*;
import com.microsoft.windowsazure.services.table.client.*;
import com.microsoft.windowsazure.services.table.client.TableQuery.*;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static com.googlecode.lazyrecords.SourceRecord.record;
import static com.googlecode.totallylazy.Callables.second;
import static com.googlecode.totallylazy.Predicates.is;
import static com.googlecode.totallylazy.Predicates.notNullValue;
import static com.googlecode.totallylazy.Predicates.where;
import static com.googlecode.totallylazy.Sequences.sequence;

public class AzureTableMappings {
    public static final DateFormat DATE_FORMAT = Dates.format("yyyy-MM-dd'T'HH:mm:ss'Z'");
    private final StringMappings stringMappings;

    public AzureTableMappings(StringMappings stringMappings) {
        this.stringMappings = stringMappings;
    }

    public AzureTableMappings() {
        this(new StringMappings().
                add(Integer.class, new IntegerMapping()).
                add(Long.class, new LongMapping()).
                add(Date.class, new DateMapping(new DateFormatConverter(DATE_FORMAT))));
    }

    public StringMappings stringMappings() {
        return stringMappings;
    }

    public Function1<DynamicTableEntity, Record> asRecord(final Sequence<Keyword<?>> definitions) {
        return new Function1<DynamicTableEntity, Record>() {
            public Record call(DynamicTableEntity tableEntity) throws Exception {
                Record result =  sequence(tableEntity.getProperties().entrySet()).fold(record(tableEntity), asField(definitions));
                return result;
            }
        };
    }

    public Function2<Record, Map.Entry<String, EntityProperty>, Record> asField(final Sequence<Keyword<?>> definitions) {
        return new Function2<Record, Map.Entry<String, EntityProperty>, Record>() {
            public Record call(Record mapRecord, Map.Entry<String, EntityProperty> attribute) throws Exception {
                Keyword<?> keyword = Keywords.matchKeyword(attribute.getKey(), definitions);
                String attrValue = attribute.getValue().getValueAsString();
                Object value = stringMappings.toValue(keyword.forClass(), attrValue);
                return mapRecord.set(Unchecked.<Keyword<Object>>cast(keyword), value);
            }
        };
    }

    public Function1<Record, DynamicTableEntity> asTableEntity(final Definition definition) {
        return new Function1<Record, DynamicTableEntity>() {
            public DynamicTableEntity call(Record record) throws Exception {
                DynamicTableEntity tableEntity = new DynamicTableEntity(new HashMap<String, EntityProperty>(Maps.map(
                        record.fields().filter(where(second(Object.class), is(notNullValue()))).
                        map(asEntityProperty()))));
                if (record instanceof SourceRecord) {
                    SourceRecord<DynamicTableEntity> source = (SourceRecord<DynamicTableEntity>)record;
                    DynamicTableEntity sourceEntity = source.value();
                    tableEntity.setPartitionKey(sourceEntity.getPartitionKey());
                    tableEntity.setRowKey(sourceEntity.getRowKey());
                    tableEntity.setEtag(sourceEntity.getEtag());
                } else {
                    tableEntity.setPartitionKey(definition.name().toString());
                    tableEntity.setRowKey(UUID.randomUUID().toString());
                }
                return tableEntity;
            }
        };
    }

    public Function1<Pair<Keyword<?>, Object>, Pair<String, EntityProperty>> asEntityProperty() {
        return new Function1<Pair<Keyword<?>, Object>, Pair<String, EntityProperty>>() {
            public Pair<String, EntityProperty> call(Pair<Keyword<?>, Object> pair) throws Exception {
                Keyword<?> keyword = pair.first();
                Object value = pair.second();
                EntityProperty prop = null;
                if (keyword.forClass() == Boolean.class) {
                    prop = new EntityProperty(((Boolean)value).booleanValue());
                } else if (keyword.forClass() == Date.class) {
                    prop = new EntityProperty((Date)value);
                } else if (keyword.forClass() == Double.class) {
                    prop = new EntityProperty(((Number)value).doubleValue());
                } else if (keyword.forClass() == Integer.class) {
                    prop = new EntityProperty(((Number)value).intValue());
                } else if (keyword.forClass() == Long.class) {
                    prop = new EntityProperty(((Number)value).longValue());
                } else if (keyword.forClass() == UUID.class) {
                    prop = new EntityProperty((UUID)value);
                } else {
                    String valueAsString = AzureTableMappings.this.stringMappings.toString(Unchecked.<Class<Object>>cast(keyword.forClass()), value);
                    prop = new EntityProperty(valueAsString);
                }
                return Pair.pair(keyword.name(), prop);
            }
        };
    }

}
