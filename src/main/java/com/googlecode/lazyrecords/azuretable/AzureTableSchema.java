package com.googlecode.lazyrecords.azuretable;

import com.google.common.base.Throwables;
import com.googlecode.lazyrecords.Definition;
import com.googlecode.lazyrecords.Grammar;
import com.googlecode.lazyrecords.Schema;
import com.microsoft.windowsazure.services.core.storage.*;
import com.microsoft.windowsazure.services.table.client.*;
import com.microsoft.windowsazure.services.table.client.TableQuery.*;

public class AzureTableSchema implements Schema {
    private final CloudTableClient client;

    public AzureTableSchema(CloudTableClient client) {
        this.client = client;
    }

    @Override
    public void define(Definition definition) {
        try {
            System.out.println("*** Creating " + definition.name());
            CloudTable table = new CloudTable(definition.name(), client);
            table.createIfNotExist();
            return;
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public boolean exists(Definition definition) {
        try {
            return client.listTables(definition.name()).iterator().hasNext();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void undefine(Definition definition) {
        try {
            System.out.println("*** Deleting " + definition.name());
            new AzureTableRecords(client).remove(definition, Grammar.all());
            /*
            CloudTable table = new CloudTable(definition.name(), client);
            table.deleteIfExists();
            */
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
