package com.curso.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Lab9Extractor {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        // AHA MOMENT: Como estamos en la máquina física, apuntamos a localhost expuesto por Docker
        props.put("bootstrap.servers", "localhost:9092"); 
        props.put("group.id", "pandas-extractor-group");
        
        // AHA MOMENT 2: Deserializador de Bytes puros, porque C envió memoria cruda, no JSON.
        props.put("key.deserializer", ByteArrayDeserializer.class.getName());
        props.put("value.deserializer", ByteArrayDeserializer.class.getName());
        props.put("auto.offset.reset", "earliest"); // Leer desde el principio para extraer todo

        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("iot-embedded"));

        System.out.println("Iniciando extracción a telemetria_limpia.csv...");
        PrintWriter csvWriter = new PrintWriter(new FileWriter("telemetria_limpia.csv"));
        // Escribimos la cabecera que Pandas usará como columnas
        csvWriter.println("ts_offset,sensor_num,valor_raw,umbral_raw,tipo,estado,planta");

        int recordsProcesados = 0;

        while (true) {
            // Hacemos polling al broker buscando bytes nuevos
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(1000));
            if (records.isEmpty()) {
                System.out.println("Esperando telemetría... (Ctrl+C para salir)");
                continue;
            }

            for (ConsumerRecord<byte[], byte[]> record : records) {
                // Desempaquetado Binario (Java usa Big-Endian de forma nativa igual que la red)
                ByteBuffer buffer = ByteBuffer.wrap(record.value());
                int tsOffset = buffer.getInt();
                short sensorNum = buffer.getShort();
                short valorRaw = buffer.getShort();
                short umbralRaw = buffer.getShort();
                byte flags = buffer.get();

                // Operaciones de bits inversas: extraemos la información del byte empaquetado
                int tipo = (flags & 0xE0) >> 5;
                int estado = (flags & 0x18) >> 3;
                int planta = (flags & 0x07);

                // Escribimos la fila limpia en el CSV
                csvWriter.printf("%d,%d,%d,%d,%d,%d,%d\n", 
                    tsOffset, sensorNum, valorRaw, umbralRaw, tipo, estado, planta);
                recordsProcesados++;
            }
            csvWriter.flush(); // Asegurar que se escriba a disco inmediatamente
            System.out.println("Lote procesado. Total extraído: " + recordsProcesados + " filas.");
        }
    }
}