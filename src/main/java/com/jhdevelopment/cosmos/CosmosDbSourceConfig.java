package com.jhdevelopment.cosmos;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

/**
 * Created by Josh on 6/17/2017.
 */
public class CosmosDbSourceConfig extends AbstractConfig {

    public static final String ENDPOINT_URI = "cosmos.endpoint.uri";
    private static final String ENDPOINT_URI_DOC = "CosmosDb Endpoint URI";
    public static final String COSMOS_KEY = "cosmos.key";
    private static final String COSMOS_KEY_DOC = "CosmosDb Key";
    public static final String COSMOS_COLLECTION_LINKS = "cosmos.collection.links";
    private static final String COSMOS_COLLECTION_DOC = "Comma seperated list of collections";


    public static ConfigDef config = new ConfigDef()
            .define(ENDPOINT_URI, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, ENDPOINT_URI_DOC)
            .define(COSMOS_KEY, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, COSMOS_KEY_DOC);


    public CosmosDbSourceConfig(ConfigDef definition, Map<String, String> originals) {
        super(definition, originals);
    }
}
