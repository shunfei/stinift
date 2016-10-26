package com.sf.stinift.plugin.mongo;

import com.google.common.collect.ImmutableList;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.sf.stinift.config.Config;
import com.sf.stinift.exchange.Pushable;
import com.sf.stinift.exchange.Row;
import com.sf.stinift.log.Logger;
import com.sf.stinift.plugin.JobParamErrorException;
import com.sf.stinift.plugin.Reader;
import com.sf.stinift.resource.Resource;
import com.sf.stinift.resource.mongo.MongoConnection;
import com.sf.stinift.worker.WorkerContext;

import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MongoReader extends Reader {
    private static final Logger log = new Logger(MongoReader.class);

    private String mongoName;
    private String collectionName;
    private String fields;
    private String filter;

    private volatile MongoConnection conn;

    public MongoReader(String mongoName, String collectionName, String fields, String filter) {
        this.mongoName = mongoName;
        this.collectionName = collectionName;
        this.fields = fields;
        this.filter = filter;
    }

    @Override
    public String info() {
        return String.format(
                "mongo:[%s], fields[%s], filter[%s]",
                mongoName,
                fields,
                filter
        );
    }

    @Override
    public boolean start(WorkerContext context, Pushable pushable) throws Exception {
        Resource resource = context.getResource(mongoName);
        if (resource == null) {
            throw new RuntimeException(String.format("mongo resource [%s] not found!", mongoName));
        }
        conn = (MongoConnection) resource;
        try {
            String[] fields = this.fields.split(",");

            DB database = conn.getDataSource();
            DBCursor result;
            if (StringUtils.isBlank(filter)) {
                result = database.getCollection(collectionName).find(new BasicDBObject(), parseFieldsToDBObject(fields));
            } else {
                result = database.getCollection(collectionName).find(BasicDBObject.parse(filter), parseFieldsToDBObject(fields));
            }
            for (DBObject dbObject : result) {
                try {
                    Map<String, String> objectMap = dbObjectToMap((BasicDBObject) dbObject);
                    Row row = new Row(fields.length);
                    for (int index = 0; index < fields.length; index++) {
                        row.setField(index, objectMap.get(fields[index]));
                    }
                    boolean suc = pushable.push(row);
                    if (!suc) {
                        return false;
                    }
                } catch (Exception e) {
                    log.error(e, "fail one row");
                }
            }
            return pushable.flush();
        } finally {
            conn.close();
            conn = null;
        }
    }

    private Map<String, String> dbObjectToMap(BasicDBObject dbObject) {
        Map<String, String> result = new HashMap<String, String>();

        for (String key : dbObject.keySet()) {
            Object value = dbObject.get(key);
            if (value == null) {
                result.put(key, null);
            } else if (value.getClass() == BasicDBObject.class) {
                Map<String, String> sub = dbObjectToMap((BasicDBObject) value);
                for (Map.Entry<String, String> entry : sub.entrySet()) {
                    result.put(key + "." + entry.getKey(), entry.getValue());
                }
            } else if (value.getClass() == BasicDBList.class) {
                result.put(key, dbArrayToString((BasicDBList) value));
            } else {
                result.put(key, value.toString());
            }
        }

        return result;
    }

    private String dbArrayToString(BasicDBList list) {
        String[] result = new String[list.size()];
        for (int i = 0; i < list.size(); i++) {
            result[i] = list.get(i).toString();
        }
        return StringUtils.join(result, Config.properties.getProperty(Config.ARRAY_DELIMITER));
    }

    private BasicDBObject parseFieldsToDBObject(String[] fields) {
        BasicDBObject dbObject = new BasicDBObject();
        for (String field : fields) {
            dbObject.put(field, 1);
        }
        return dbObject;
    }

    @Override
    public void interrupt() {
        MongoConnection conn = this.conn;
        if (conn != null) {
            conn.close();
        }
    }

    public static class Creator extends Reader.Creator {
        @JsonProperty
        public String mongo;
        @JsonProperty
        public String collection;
        @JsonProperty
        public String fields;
        @JsonProperty
        public String filter;

        private boolean valid() {
            return mongo != null &&
                    collection.matches("^[a-zA-Z0-9_]+$") &&
                    fields.matches("^[a-zA-Z0-9_,\\.]+$");
        }

        @Override
        public List<MongoReader> create() throws JobParamErrorException {
            if (!valid()) {
                throw new JobParamErrorException();
            }
            return ImmutableList.of(new MongoReader(mongo, collection, fields, filter));
        }
    }
}
