import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;
import java.util.concurrent.Future;

public class Main {
    private final static String TOPIC = "my-timeouttest-topic-nonexist";
    private final static String BOOTSTRAP_SERVERS =
            "kafka.local:31090,kafka.local:31091,kafka.local:31092";

    public static void main(String[] args) {
        try
        {
            if (args.length == 0) {
                runProducer(1);
            } else {
                runProducer(Integer.parseInt(args[0]));
            }
        } catch(Exception ex)
        {
            System.out.println("Hard exception.");
            System.out.println(ex.getStackTrace());
        }
    }

    private static Producer<Long, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.RETRIES_CONFIG, 5);
        props.put(ProducerConfig.ACKS_CONFIG, "-1");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 2);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 5000);

        //props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 5000);

        return new KafkaProducer<>(props);
    }

    static void runProducer(final int sendMessageCount) throws Exception {
        final Producer<Long, String> producer = createProducer();
        long time = 0; //System.currentTimeMillis();

        try {
            for (long index = time; index < time + sendMessageCount; index++) {
                final ProducerRecord<Long, String> record =
                        new ProducerRecord<>(TOPIC, index,
                                "Hello " + index);

                producer.send(record, (metadata, exception) -> {
                    long elapsedTime = System.currentTimeMillis() - time;
                    if(exception != null) {
                        exception.printStackTrace();
                    }
                    if (metadata != null) {
                        System.out.println("Success!");
//                        System.out.printf("sent record(key=%s value=%s) " +
//                                        "meta(partition=%d, offset=%d) time=%d\n",
//                                record.key(), record.value(), metadata.partition(),
//                                metadata.offset(), elapsedTime);
                    } else {
                        exception.printStackTrace();
                    }
                });

//                long elapsedTime = System.currentTimeMillis() - time;
//                System.out.printf("sent record(key=%s value=%s) " +
//                                "meta(partition=%d, offset=%d) time=%d\n",
//                        record.key(), record.value(), metadata.partition(),
//                        metadata.offset(), elapsedTime);

                Thread.sleep(7000);

            }
        } finally {
            producer.flush();
            producer.close();
        }
    }
}