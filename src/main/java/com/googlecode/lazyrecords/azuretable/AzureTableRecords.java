package com.googlecode.lazyrecords.azuretable;

import com.googlecode.lazyrecords.*;
import com.googlecode.lazyrecords.azuretable.mappings.AzureTableMappings;
import com.googlecode.totallylazy.Function1;
import com.googlecode.totallylazy.Predicate;
import com.googlecode.totallylazy.Sequence;
import com.googlecode.totallylazy.Sequences;
import com.googlecode.totallylazy.Value;
import com.googlecode.totallylazy.numbers.Numbers;

import com.microsoft.windowsazure.services.core.storage.*;
import com.microsoft.windowsazure.services.table.client.*;
import com.microsoft.windowsazure.services.table.client.TableQuery.*;

import java.util.List;

import static com.googlecode.lazyrecords.SelectCallable.select;
import static com.googlecode.lazyrecords.sql.expressions.SelectBuilder.from;
import static com.googlecode.totallylazy.numbers.Numbers.sum;

public class AzureTableRecords extends AbstractRecords {
    private final CloudTableClient client;
    private final AzureTableMappings mappings;
    private final Logger logger;
    private final Schema schema;

    private static final int MAX_BATCH_OPS = 100;

    public AzureTableRecords(final CloudTableClient client, final AzureTableMappings mappings, final Logger logger, final Schema schema) {
        this.mappings = mappings;
        this.client = client;
        this.logger = logger;
        this.schema = schema;
    }

    public AzureTableRecords(final CloudTableClient client) {
        this(client, new AzureTableMappings(), new IgnoreLogger(), new AzureTableSchema(client));
    }

    public Sequence<Record> get(Definition definition) {
        return new AzureTableSequence<Record>(client, TableQuery.from(definition.name(), DynamicTableEntity.class), mappings.stringMappings(), mappings.asRecord(definition.fields()), logger);
    }

    private Function1<Keyword<?>, String> keywordToName() {
        return new Function1<Keyword<?>, String>() {
            public String call(Keyword<?> keyword) throws Exception {
                return keyword.name();
            }
        };
    }

    public Number add(final Definition definition, Sequence<Record> records) {
        if (records.isEmpty()) {
            return 0;
        }

        return records.recursive(Sequences.<Record>splitAt(MAX_BATCH_OPS)).
                mapConcurrently(putAttributes(definition)).
                reduce(sum());
    }

    public Number remove(Definition definition, Predicate<? super Record> predicate) {
        if (!schema.exists(definition)) {
            return 0;
        }
        Sequence<Record> items = get(definition).filter(predicate).realise();
        if (items.isEmpty()) {
            return 0;
        }

        return items.recursive(Sequences.<Record>splitAt(MAX_BATCH_OPS)).
                mapConcurrently(deleteAttributes(definition)).
                reduce(sum());
    }

    @Override
    public Number remove(Definition definition) {
        /*
        Record head = get(definition).map(select(Keywords.keyword("count(*)", String.class))).head();
        Number result = Numbers.valueOf(head.get(Keywords.keyword("Count", String.class))).get();
        */
        schema.undefine(definition);
        schema.define(definition);
        //return result;
        return 0;
    }

    private Function1<Sequence<Record>, Number> putAttributes(final Definition definition) {
        return new Function1<Sequence<Record>, Number>() {
            public Number call(Sequence<Record> batch) throws Exception {
                TableBatchOperation op = new TableBatchOperation();
                op.addAll(batch.map(mappings.asTableEntity(definition)).map(asInsertOrReplaceOperation()).toList());
                client.execute(definition.name(), op);
                return batch.size();
            }
        };
    }

    private Function1<TableEntity, TableOperation> asInsertOrReplaceOperation() {
        return new Function1<TableEntity, TableOperation>() {
            public TableOperation call(TableEntity tableEntity) throws Exception {
                return TableOperation.insertOrReplace(tableEntity);
            }
        };
    }

    private Function1<Sequence<Record>, Number> deleteAttributes(final Definition definition) {
        return new Function1<Sequence<Record>, Number>() {
            public Number call(Sequence<Record> batch) throws Exception {
                TableBatchOperation op = new TableBatchOperation();
                op.addAll(batch.map(mappings.asTableEntity(definition)).map(asDeleteOperation()).toList());
                client.execute(definition.name(), op);
                return batch.size();
            }
        };
    }

    private Function1<TableEntity, TableOperation> asDeleteOperation() {
        return new Function1<TableEntity, TableOperation>() {
            public TableOperation call(TableEntity tableEntity) throws Exception {
                return TableOperation.delete(tableEntity);
            }
        };
    }

}
