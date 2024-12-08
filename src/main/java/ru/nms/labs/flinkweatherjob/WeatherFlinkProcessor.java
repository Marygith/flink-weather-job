package ru.nms.labs.flinkweatherjob;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class WeatherFlinkProcessor {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final String kafkaBroker = "localhost:29092";
        final String inputTopic = "ORIGINAL_WEATHER_MESSAGES";
        final String outputTopic = "SECTOR_DISPERSION";

        KafkaSource<SectorWeather> source = KafkaSource.<SectorWeather>builder()
                .setBootstrapServers(kafkaBroker)
                .setTopics(inputTopic)
                .setGroupId("group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new JsonDeserializationSchema<>(SectorWeather.class))
                .build();

        KafkaRecordSerializationSchema<SectorDispersion> serializer = KafkaRecordSerializationSchema.builder()
                .setValueSerializationSchema(new JsonSerializationSchema<SectorDispersion>())
                .setTopic(outputTopic)
                .build();

        KafkaSink<SectorDispersion> sink = KafkaSink.<SectorDispersion>builder()
                .setBootstrapServers(kafkaBroker)
                .setRecordSerializer(serializer)
                .build();

        DataStream<SectorWeather> weatherStream = env.fromSource(
                source,
                WatermarkStrategy.forMonotonousTimestamps(),
                "Kafka Source"
        );

        weatherStream
                .keyBy(new KeySelector<SectorWeather, Tuple2<Integer, String>>() {
                    @Override
                    public Tuple2<Integer, String> getKey(SectorWeather weather) {
                        return Tuple2.of(weather.getSector(), toDateFromDateTime(weather.getObservationDate()));
                    }
                })
                .window(EventTimeSessionWindows.withGap(Time.minutes(1)))
                .process(new WeatherWindowProcessor())
                .sinkTo(sink);

        env.execute("Weather Batch Processor");
    }

    private static String toDateFromDateTime(String dateTime) {
        return LocalDate.parse(dateTime, DateTimeFormatter.ISO_DATE_TIME).format(DateTimeFormatter.ISO_DATE);
    }
}
