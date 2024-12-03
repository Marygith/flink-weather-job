package ru.nms.labs.flinkweatherjob;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.formats.json.JsonSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import ru.nms.labs.model.SectorDispersion;
import ru.nms.labs.model.SectorWeather;


public class WeatherFlinkProcessor {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final String kafkaBroker = "localhost:9092";
        final String inputTopic = "ORIGINAL_WEATHER_MESSAGES";
        final String outputTopic = "SECTOR_DISPERSION";

        KafkaSource<SectorWeather> source = KafkaSource.<SectorWeather>builder()
                .setBootstrapServers(kafkaBroker)
                .setTopics(inputTopic)
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new JsonDeserializationSchema<>(SectorWeather.class))
                .build();

        KafkaRecordSerializationSchema<SectorDispersion> serializer = KafkaRecordSerializationSchema.builder()
                .setValueSerializationSchema(  new JsonSerializationSchema<SectorDispersion>())
                .setTopic(outputTopic)
                .build();

        KafkaSink<SectorDispersion> sink = KafkaSink.<SectorDispersion>builder()
                .setBootstrapServers(kafkaBroker)
                .setRecordSerializer(serializer)
                .build();

        DataStream<SectorWeather> weatherStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        weatherStream
                .keyBy(SectorWeather::getSector)
                .window(GlobalWindows.create())
                .trigger(new WeatherBatchTrigger())
                .allowedLateness(Time.minutes(5))
                .aggregate(new WeatherAggregator())
                .sinkTo(sink);

        env.execute("Weather Batch Processor");
    }
}
