package com.github.iammunir.consumer;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.*;
import org.apache.spark.streaming.Time;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class SparkstreamingConsumer {

    public static Logger logger = Logger.getLogger(SparkstreamingConsumer.class.getName());

    public static String pathToAvroFolder = null;
    public static String pathToParquetFolder = null;

    public static String localPathAvro = null;
    public static String localPathParquet = null;

    public static void main(String[] args) throws InterruptedException {

        Logger.getLogger("org").setLevel(Level.ERROR);

        // setup properties
        Properties properties = new Properties();
        try {
            InputStream inputStream = SparkstreamingConsumer.class.getClassLoader().getResourceAsStream("config.properties");
            properties.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("FATAL: Problem reading the config properties");
        }
        pathToAvroFolder = properties.getProperty("hdfs.path.avro");
        pathToParquetFolder = properties.getProperty("hdfs.path.parquet");

        localPathAvro = properties.getProperty("local.path.avro");
        localPathParquet = properties.getProperty("local.path.parquet");

        String envMaster = properties.getProperty("env.master");

        // create config and context
        SparkConf conf = new SparkConf().setAppName("SparkstreamingConsumer").setMaster(envMaster);
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(5));

        // create stream
        JavaInputDStream<ConsumerRecord<String, String>> stream = createStream(streamingContext, properties);

        // read records
        JavaDStream<String> records = stream.map(stringStringConsumerRecord -> stringStringConsumerRecord.value());

        // process raw data
        records.foreachRDD(
                new VoidFunction2<JavaRDD<String>, Time>() {
                    @Override
                    public void call(JavaRDD<String> stringJavaRDD, Time time) throws Exception {

                        // Get the singleton instance of SparkSession
                        SparkSession spark = SparkSession.builder()
                                .config(stringJavaRDD.context().getConf())
                                .getOrCreate();

                        // Convert RDD[String] to RDD[case class] to DataFrame
                        JavaRDD<JavaRowTweet> rowRDD = stringJavaRDD.map(new Function<String, JavaRowTweet>() {
                            public JavaRowTweet call(String tweet) {
                                JavaRowTweet record = new JavaRowTweet();
                                record.setTweet(tweet);
                                return record;
                            }
                        });

                        Dataset<Row> tweetsDataFrame = spark.createDataFrame(rowRDD, JavaRowTweet.class);

                        long count = tweetsDataFrame.count();
                        if (count > 0) {
                            // save to avro file
                            saveAvroToHDFS(tweetsDataFrame, count);
                        }

                    }
                }
        );

        records.foreachRDD(
                new VoidFunction2<JavaRDD<String>, Time>() {
                    @Override
                    public void call(JavaRDD<String> stringJavaRDD, Time time) throws Exception {

                        // Get the singleton instance of SparkSession
                        SparkSession spark = SparkSession.builder()
                                .config(stringJavaRDD.context().getConf())
                                .getOrCreate();

                        JavaRDD<JavaRowClean> rowCleanRDD = stringJavaRDD.map(new Function<String, JavaRowClean>() {
                            @Override
                            public JavaRowClean call(String tweet) throws Exception {
                                JavaRowClean record = new JavaRowClean();
                                record.transformTweet(tweet);
                                return record;
                            }
                        });

                        Dataset<Row> cleanedTweetsDataFrame = spark.createDataFrame(rowCleanRDD, JavaRowClean.class);

                        long count = cleanedTweetsDataFrame.count();
                        if (count > 0) {
                            // save to parquet file
                            String millisecString = String.valueOf(time.milliseconds());
                            saveParquetToHDFS(cleanedTweetsDataFrame, millisecString, count);
                        }
                    }
                }
        );

        streamingContext.start();
        streamingContext.awaitTermination();
    }

    public static JavaInputDStream<ConsumerRecord<String, String>> createStream(JavaStreamingContext streamingContext, Properties properties) {

        String topic = properties.getProperty("kafka.topic");
        String bootstrapServer = properties.getProperty("kafka.server");
        String groupId = properties.getProperty("kafka.groupid");

        // kafka config
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        Collection<String> topics = Collections.singletonList(topic);

        return KafkaUtils.createDirectStream(
                streamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
        );
    }

    public static void saveParquetToHDFS(Dataset<Row> data, String milis, long count) {
        data.coalesce(1).write()
                .parquet(localPathParquet + milis + ".parquet");
        logger.info("stored " + count + " data into parquet folder");
    }

    public static void saveAvroToHDFS(Dataset<Row> data, long count) {

        data.coalesce(1).write()
                .format("avro")
                .mode("append")
                .save(localPathAvro);

        logger.info("stored " + count + " data into avro folder");
    }

}
