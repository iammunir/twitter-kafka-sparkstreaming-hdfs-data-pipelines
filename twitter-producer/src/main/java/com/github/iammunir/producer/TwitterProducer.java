package com.github.iammunir.producer;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(TwitterProducer.class);
        logger.info("setup");

        Properties properties = new Properties();
        try {
            InputStream inputStream = TwitterProducer.class.getClassLoader().getResourceAsStream("config.properties");
            properties.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("FATAL: Problem reading the config properties");
        }

        // set up blocking queues, size based on expected TPS of the stream
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(10000);

        // create a twitter client
        Client client = createTwitterClient(properties, msgQueue);

        // establish a connection.
        client.connect();

        // create a kafka producer
        KafkaProducer<String, String> producer = createProducer(properties);

        // loop to send tweets to kafka topic
        String topic = properties.getProperty("kafka.topic");
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (msg != null) {
                producer.send(
                        new ProducerRecord<>(topic, null, msg),
                        new Callback() {
                            @Override
                            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                                if (e != null) {
                                    logger.error("Error happened ", e);
                                } else {
                                    logger.info(recordMetadata.toString());
                                }
                            }
                        }
                );
            }
        }
        logger.info("Application has been stopped");
    }

    public static Client createTwitterClient(Properties properties, BlockingQueue<String> msgQueue) {

        // get tokens
        String consumerKey = properties.getProperty("twitter.consumerKey");
        String consumerSecret = properties.getProperty("twitter.consumerSecret");
        String token = properties.getProperty("twitter.token");
        String secret = properties.getProperty("twitter.secret");
        String keyword1 = properties.getProperty("twitter.keyword1");

        // declare the host to connect to and the endpoint
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        // Optional: set up some track terms
        List<String> terms = Lists.newArrayList(keyword1);
        hosebirdEndpoint.trackTerms(terms);

        // setup authentication
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

        // create client builder
        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();
    }

    public static KafkaProducer<String, String> createProducer(Properties properties) {

        String bootstrapServer = properties.getProperty("kafka.server");

        // setup properties
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<>(kafkaProps);
    }
}
