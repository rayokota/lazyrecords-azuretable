package com.googlecode.lazyrecords.azuretable;

import com.googlecode.lazyrecords.Logger;
import com.googlecode.lazyrecords.MemoryLogger;
import com.googlecode.lazyrecords.Record;
import com.googlecode.lazyrecords.SchemaBasedRecordContract;
import com.googlecode.lazyrecords.azuretable.mappings.AzureTableMappings;
import com.googlecode.totallylazy.Sequence;
import com.googlecode.totallylazy.matchers.Matchers;
import com.googlecode.totallylazy.matchers.NumberMatcher;
import org.junit.Ignore;
import org.junit.Test;

import java.io.InputStream;
import java.util.Map;

import static com.googlecode.totallylazy.Sequences.repeat;
import static com.googlecode.totallylazy.matchers.NumberMatcher.is;
import static org.hamcrest.MatcherAssert.assertThat;

// Include the following imports to use table APIs
import com.microsoft.windowsazure.services.core.storage.*;
import com.microsoft.windowsazure.services.table.client.*;
import com.microsoft.windowsazure.services.table.client.TableQuery.*;

public class AzureTableRecordsTest extends SchemaBasedRecordContract<AzureTableRecords> {
    //private AmazonSimpleDBClient amazonSimpleDBClient;

    // Define the connection-string with your values
    public static final String storageConnectionString =
            "DefaultEndpointsProtocol=http;" +
                    "AccountName=yokotaresearch;" +
                    "AccountKey=PgZ072aI8h+HlSGSjK/oYLpPiKW1a33OOfD59Uzwd3o4lEcSqTRZifGKDXs3TNJgaxs8U72EO3AfIrdAOwOVeQ==";

    @Ignore
    @Test
    public void test() throws Exception {
        // Retrieve storage account from connection-string
        CloudStorageAccount storageAccount =
                CloudStorageAccount.parse(storageConnectionString);

        // Create the table client.
        CloudTableClient tableClient = storageAccount.createCloudTableClient();

        String tableName = "people";
        CloudTable table = new CloudTable(tableName, tableClient);
        try {
            table.createIfNotExist();
        } catch (Exception e) {
            e.printStackTrace();
        }

        for (String s : tableClient.listTables()) {
            System.out.println("*** table : " + s);
        }

        System.out.println("Done");
    }

    @Override
    protected AzureTableRecords createRecords() throws Exception {
        supportsRowCount = false;
        CloudStorageAccount storageAccount =
                CloudStorageAccount.parse(storageConnectionString);

        // Create the table client.
        CloudTableClient tableClient = storageAccount.createCloudTableClient();
        schema = new AzureTableSchema(tableClient);
        return new AzureTableRecords(tableClient);
    }

    @Override
    @Ignore("Not Supported by Azure Table")
    public void supportsAliasingAKeyword() throws Exception {
    }

    /*
	@Override
	@Ignore("Still thinking about lexical representation of BigDecimal")
	public void supportsBigDecimal() throws Exception {
	}

	@Override
	@Ignore("Not implemented yet")
	public void supportsSortingByMultipleKeywords() throws Exception {
	}

	@Override
    @Ignore("Not Supported by AWS")
    public void supportsAliasingAKeyword() throws Exception {
    }

    @Test
    public void canAddMoreThat25RecordsAtATimeAndReceiveMoreThanAHundred() throws Exception {
        assertThat(records.get(books).size(), is(3));
        Sequence<Record> newBooks = repeat(Record.constructors.record().set(isbn, zenIsbn)).take(100);
        assertThat(newBooks.size(), NumberMatcher.is(100));
        records.add(books, newBooks);
        assertThat(records.get(books).size(), is(103));
    }

    @Test
    public void memorisesAndThereforeOnlyExecutesSqlOnce() throws Exception {
        MemoryLogger logger = new MemoryLogger();
        Sequence<Record> result = simpleDbRecords(logger).get(people).sortBy(age);
        Record head = result.head();
        Sequence<Map<String, ?>> logs = logger.data();
        assertThat(head, Matchers.is(result.head())); // Check iterator
        assertThat(logs, Matchers.is(logger.data())); // Check queries
    }
    */
}
