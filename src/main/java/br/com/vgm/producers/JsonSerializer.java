package br.com.vgm.producers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.common.serialization.Serializer;

public class JsonSerializer<T> implements Serializer<T> {

  private final ObjectMapper mapper = new ObjectMapper();

  @Override
  public byte[] serialize(String topic, T data) {
    try {
      return mapper.writeValueAsString(data).getBytes();
    } catch (JsonProcessingException e) {
      return "".getBytes();
    }
  }

}
