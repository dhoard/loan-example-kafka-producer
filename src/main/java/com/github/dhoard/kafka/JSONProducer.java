package com.github.dhoard.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.confluent.kafka.schemaregistry.json.jackson.JsonOrgModule;
import io.confluent.monitoring.clients.interceptor.MonitoringProducerInterceptor;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JSONProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(JSONProducer.class);

    private static final String BOOTSTRAP_SERVERS = "cp-5-5-x.address.cx:9092";

    private static final String TOPIC = "ingest";

    private static final Random RANDOM = new Random();

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    static {
        OBJECT_MAPPER.registerModule(new JsonOrgModule());
        OBJECT_MAPPER.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
    }

    public static void main(String[] args) throws Exception {
        new JSONProducer().run(args);
    }

    public void run(String[] args) throws Exception {
        KafkaProducer<String, String> kafkaProducer = null;

        try {
            Properties properties = new Properties();

            properties.setProperty(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

            properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

            properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

            properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");

            properties.setProperty(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                MonitoringProducerInterceptor.class.getName());

            kafkaProducer = new KafkaProducer<String, String>(properties);

            String poolId = randomString(10);
            JSONObject poolJSONObject = new JSONObject();
            poolJSONObject.put("id", poolId);
            poolJSONObject.put("type", "LOAN_GROUP");
            poolJSONObject.put("timestamp", getISOTimestamp());
            poolJSONObject.put("description", "This is a loan group");

            int loanCount = 5;
            String[] loanIds = new String[loanCount];
            JSONObject[] loanJSONObjectArray = new JSONObject[loanCount];

            for (int i = 0; i < 5; i++) {
                String loanId = randomString(10);
                JSONObject loanJSONObject = new JSONObject();
                loanJSONObject.put("id", loanId);
                loanJSONObject.put("type", "LOAN");
                loanJSONObject.put("timestamp", getISOTimestamp());
                loanJSONObject.put("description", "This is loan");

                loanJSONObjectArray[i] = loanJSONObject;

                JSONArray loanIdJSONArray = null;

                if (poolJSONObject.has("loanIds")) {
                    loanIdJSONArray = poolJSONObject.getJSONArray("loanIds");
                } else {
                    loanIdJSONArray = new JSONArray();
                    poolJSONObject.put("loanIds", loanIdJSONArray);
                }

                loanIdJSONArray.put(loanId);
            }

            poolJSONObject.put("loanCount", loanJSONObjectArray.length);

            LOGGER.info("poolJSONObject = [" + poolJSONObject + "]");

            for (int i = 0; i < loanJSONObjectArray.length; i++) {
                LOGGER.info("loanJSONObject = [" + loanJSONObjectArray[i] + "]");
            }

            for (int i = 0; i < loanJSONObjectArray.length; i++) {
                produce(
                    kafkaProducer,
                    loanJSONObjectArray[i].getString("id"),
                    loanJSONObjectArray[i].toString());
            }

            produce(kafkaProducer, poolJSONObject.getString("id"), poolJSONObject.toString());

        } finally {
            if (null != kafkaProducer) {
                kafkaProducer.flush();
                kafkaProducer.close();
            }
        }
    }

    private void produce(KafkaProducer<String, String> kafkaProducer, String key, String value)
        throws InterruptedException, ExecutionException {

        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(
            TOPIC, key, value);

        ExtendedCallback extendedCallback = new ExtendedCallback(producerRecord);
        Future<RecordMetadata> future = kafkaProducer
            .send(producerRecord, extendedCallback);

        future.get();

        if (extendedCallback.getException() != null) {
            LOGGER.error("Exception", extendedCallback.getException());
        }

        if (extendedCallback.isError()) {
            LOGGER.error("isError = [" + extendedCallback.isError() + "]");
        }
    }

    public class ExtendedCallback implements Callback {

        private ProducerRecord producerRecord;

        private RecordMetadata recordMetadata;

        private Exception exception;

        public ExtendedCallback(ProducerRecord<String, String> producerRecord) {
            this.producerRecord = producerRecord;
        }

        public boolean isError() {
            return (null != this.exception);
        }

        private RecordMetadata getRecordMetadata() {
            return this.recordMetadata;
        }

        public Exception getException() {
            return this.exception;
        }

        public void onCompletion(RecordMetadata recordMetadata, Exception exception) {
            this.recordMetadata = recordMetadata;

            if (null == exception) {
                //logger.info("Received, key = [" + producerRecord.key() + "] value = [" + producerRecord.value() + "] topic = [" + recordMetadata.topic() + "] partition = [" + recordMetadata.partition() + "] offset = [" + recordMetadata.offset() + "] timestamp = [" + toISOTimestamp(recordMetadata.timestamp(), "America/New_York") + "]");
            }

            this.exception = exception;
        }
    }

    private static String getISOTimestamp() {
        return toISOTimestamp(System.currentTimeMillis(), "America/New_York");
    }

    private static String toISOTimestamp(long milliseconds, String timeZoneId) {
        return Instant.ofEpochMilli(milliseconds).atZone(ZoneId.of(timeZoneId)).toString().replace("[" + timeZoneId + "]", "");
    }

    private static long randomLong(int min, int max) {
        if (max == min) {
            return min;
        }

        if (min > max) {
            throw new IllegalArgumentException("min must be <= max, min = [" + min + "] max = [" + max + "]");
        }

        return + (long) (Math.random() * (max - min));
    }

    private String randomString(int length) {
        return RANDOM.ints(48, 122 + 1)
            .filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
            .limit(length)
            .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
            .toString();
    }

    private String fileToString(String filename) throws IOException {
        return new String(Files.readAllBytes(Paths.get(filename)));
    }
}
