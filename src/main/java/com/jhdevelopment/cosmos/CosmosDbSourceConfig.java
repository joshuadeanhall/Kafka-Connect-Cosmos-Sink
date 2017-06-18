package com.jhdevelopment.cosmos;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;


public class CosmosDbSourceConfig extends AbstractConfig {

    public static final String ENDPOINT_URI = "cosmos.endpoint.uri";
    private static final String ENDPOINT_URI_DOC = "CosmosDb Endpoint URI";
    public static final String COSMOS_KEY = "cosmos.key";
    private static final String COSMOS_KEY_DOC = "CosmosDb Key";
    public static final String COSMOS_DATABASE = "cosmos.database";
    private static final String COSMOS_DATABASE_DOC = "CosmosDb Database Name";
    public static final String COSMOS_COLLECTION_NAMES = "cosmos.collection.names";
    public static final String COSMOS_COLLECTION_NAME = "cosmos.collection.name";
    private static final String COSMOS_COLLECTION_DOC = "Comma seperated list of collections";
    public static final String COSMOS_MAX_DOCUMENTS_PER_PARTITION = "cosmos.max.documents";
    private static final String COSMOS_MAX_DOCUMENTS_PER_PARTITION_DOC = "Number of docuents that each partition returns per poll request";



    public static ConfigDef config = new ConfigDef()
            .define(ENDPOINT_URI, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, ENDPOINT_URI_DOC)
            .define(COSMOS_KEY, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, COSMOS_KEY_DOC)
            .define(COSMOS_DATABASE, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, COSMOS_DATABASE_DOC)
            .define(COSMOS_COLLECTION_NAMES, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, COSMOS_COLLECTION_DOC)
            .define(COSMOS_MAX_DOCUMENTS_PER_PARTITION, ConfigDef.Type.INT, ConfigDef.Importance.HIGH, COSMOS_MAX_DOCUMENTS_PER_PARTITION_DOC);


    public CosmosDbSourceConfig(ConfigDef definition, Map<String, String> originals) {
        super(definition, originals);
    }
}
