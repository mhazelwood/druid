package druid.examples.personalization;

import com.beust.jcommander.internal.Maps;

import java.util.Map;

public class DimensionValueStats
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
}
