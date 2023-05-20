package com.hieund2102.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.Map;

public class RouteTombstone<R extends ConnectRecord<R>> implements Transformation<R> {
    public static final String OVERVIEW_DOC = "Route tombstone messages to another topic";

    private interface ConfigName {
        String TARGET_TOPIC_PREFIX = "target.topic.prefix";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.TARGET_TOPIC_PREFIX,
                    ConfigDef.Type.STRING,
                    "tombstone-",
                    ConfigDef.Importance.HIGH,
                    "Prefix for target topic's name");
    private String topicPrefix;

    public R apply(R r) {
        if (r.key() != null && r.value() == null) {
            return r.newRecord(topicPrefix + r.topic(),
                    null,
                    r.keySchema(),
                    r.key(),
                    null,
                    null,
                    r.timestamp(),
                    r.headers());
        }
        return r;
    }

    public ConfigDef config() {
        return CONFIG_DEF;
    }

    public void close() {
        // do nothing
    }

    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        this.topicPrefix = config.getString(ConfigName.TARGET_TOPIC_PREFIX);

    }
}
