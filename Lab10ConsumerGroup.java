package com.curso.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Duration;
import java.util.*;

public class Lab10ConsumerGroup {
    public static void main(String[] args) {
        // Generamos un ID aleatorio de 4 letras para distinguir visualmente cada terminal
        String workerId = UUID.randomUUID().toString().substring(0, 4).toUpperCase();
        
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        // EL SECRETO: Todos comparten el mismo group.id para trabajar en equipo
        props.put("group.id", "retail-fraud-group"); 
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // Interceptamos la asignación de particiones para verlo en consola
        consumer.subscribe(Collections.singletonList("retail-bulk"), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                System.out.println("\n[WORKER " + workerId + "] ⚠️ PARTICIONES REVOCADAS (Me quitaron trabajo): " + partitions);
            }
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                System.out.println("\n[WORKER " + workerId + "] ✅ NUEVAS PARTICIONES ASIGNADAS: " + partitions);
            }
        });

        System.out.println("[WORKER " + workerId + "] Listo y esperando transacciones de retail...");
        
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            if (!records.isEmpty()) {
                // Por cada lote recibido, imprimimos un punto para no saturar la pantalla
                System.out.print("."); 
            }
        }
    }
}