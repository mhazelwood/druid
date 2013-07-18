/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package druid.examples.personalization;

import com.beust.jcommander.internal.Lists;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class FilterParser extends Parser
{
  private final List<String> dimensionList = Lists.newArrayList();

  public FilterParser()
  {
  }

  @Override
  public void parse(UserInformation user, Map<String, Object> set)
  {
    addFilter(user, set);
    Collections.sort(dimensionList);
    addDimensionNameCrossTerm(user, dimensionList);
  }

  public void addFilter(
      UserInformation user, Map<String, Object> set
  )
  {
    String type = (String) set.get("type");
    if (type.equals("and") || type.equals("or")) {
      for (Map<String, Object> filter : (List<Map<String, Object>>) set.get("filters")) {
        parse(user, filter);
      }
    } else if (type.equals("not")) {
        parse(user, (Map<String, Object>) set.get("filter"));
    } else if (!set.get("attribute").equals("timestamp")) {
        String dimensionName = (String) set.get("attribute");
        String dimensionValue;
        if (type.equals("is")) {
          dimensionValue = (String) set.get("value");
          addDimensionValue(user,dimensionName, dimensionValue);

        }
        else if (set.get("type").equals("in"))
        {
          for (String dimValue : (ArrayList<String>) set.get("values"))
          {
            addDimensionValue(user, dimensionName, dimValue);
          }
        }
      }
  }

  public void addDimensionValue(UserInformation user, String dimensionName, String dimensionValue){
    dimensionList.add(dimensionName);
    if (user.getStatMap().get(dimensionName)==null){
      user.getStatMap().put(dimensionName,new DimensionValueStats());
    }
    DimensionValueStats dim= user.getStatMap().get(dimensionName);
    dim.incrementCount();
    incrementMap(dimensionValue,dim.getDimensionValues());
  }

  public void addDimensionNameCrossTerm(UserInformation user, List<String> dimensionList)
  {
    StringBuilder builder = new StringBuilder();
    if (dimensionList.size() > 0) {
      for (String dimensionName : dimensionList) {
        builder.append(dimensionName);
        builder.append(",");
      }
      incrementMap(builder.toString(), user.getDimensionNameCrossTerm());
      incrementMap("total", user.getDimensionNameCrossTerm());
    }
  }
}
