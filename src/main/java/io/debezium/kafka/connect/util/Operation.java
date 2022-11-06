package io.debezium.kafka.connect.util;

public interface Operation {
        String INSERT = "c";
        String UPDATE = "u";
        String DELETE = "d";
}
