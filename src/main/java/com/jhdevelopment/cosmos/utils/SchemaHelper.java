package com.jhdevelopment.cosmos.utils;

import com.microsoft.azure.documentdb.Document;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

/**
 * Created by Josh on 6/18/2017.
 */
public class SchemaHelper {
    public Struct toStruct(Document document, Schema schema, String databaseName, String collectionName) {
        final Struct messageStruct = new Struct(schema);
        final long timeStamp = document.getTimestamp().getTime();
        messageStruct.put("timestamp", timeStamp);
        messageStruct.put("lsn", document.getInt("_lsn"));
        messageStruct.put("etag", document.getETag());
        messageStruct.put("database", databaseName);
        messageStruct.put("collection", collectionName);
        final Document modifiedDocument = (Document) document.get("o");
        messageStruct.put("object", modifiedDocument.toJson());

        return messageStruct;
    }
}
