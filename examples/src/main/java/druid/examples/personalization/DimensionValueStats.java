package druid.examples.personalization;

import com.beust.jcommander.internal.Maps;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;

import java.io.Serializable;
import java.util.Map;

public class DimensionValueStats implements Serializable
{
  private int count=0;
  private Map<String,Integer> dimensionValues = Maps.newHashMap();

  public void incrementCount(){
    count++;
  }

  public Map<String, Integer> getDimensionValues()
  {
    return dimensionValues;
  }

  public String toString(){
    ObjectMapper mapper = new ObjectMapper();
    try {
      return mapper.writeValueAsString(this);
    }
    catch (JsonProcessingException e) {
      throw Throwables.propagate(e);
    }
  }

}
