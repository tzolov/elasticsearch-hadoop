package org.elasticsearch.hadoop.crunch;

import java.util.Map;

import org.apache.crunch.CrunchRuntimeException;
import org.apache.crunch.MapFn;
import org.apache.crunch.types.PType;
import org.apache.crunch.types.PTypeFamily;
import org.apache.crunch.types.writable.WritableTypeFamily;
import org.apache.hadoop.io.MapWritable;
import org.codehaus.jackson.map.ObjectMapper;
import org.elasticsearch.hadoop.util.WritableUtils;

public class ESTypes {

  private static class JacksonMapWritableInputMapFn<T> extends MapFn<MapWritable, T> {

    private final Class<T> clazz;
    private transient ObjectMapper mapper;

    public JacksonMapWritableInputMapFn(Class<T> clazz) {
      this.clazz = clazz;
    }

    @Override
    public void initialize() {
      this.mapper = new ObjectMapper();
    }

    @Override
    public T map(MapWritable input) {
      try {        
        String valueAsString = mapper.writeValueAsString(WritableUtils.fromWritable(input));
        if (clazz.isAssignableFrom(String.class)) {
          return (T) valueAsString;
        }
        return mapper.readValue(valueAsString, clazz);
      } catch (Exception e) {
        throw new CrunchRuntimeException(e);
      }
    }
  }

  private static class JacksonMapWritableOutputMapFn<T> extends MapFn<T, MapWritable> {

    private transient ObjectMapper mapper;

    @Override
    public void initialize() {
      this.mapper = new ObjectMapper();
    }

    @Override
    public MapWritable map(T input) {
      try {
        String json = mapper.writeValueAsString(input);
        return (MapWritable) WritableUtils.toWritable(mapper.readValue(json, Map.class));
      } catch (Exception e) {
        throw new CrunchRuntimeException(e);
      }
    }
  }

  public static <T> PType<T> jsonMapWritable(Class<T> clazz, PTypeFamily typeFamily) {
    return typeFamily.derived(clazz, new JacksonMapWritableInputMapFn<T>(clazz), new JacksonMapWritableOutputMapFn<T>(),
        typeFamily.records(MapWritable.class));
  }
  
  public static PType<Map> map() {
    return jsonMapWritable(Map.class, WritableTypeFamily.getInstance());
  }
}
