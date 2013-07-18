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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FilterParser extends Parser
{
  private final List<String> dimensionList;

  public FilterParser(List<String> dimensionList)
  {
    this.dimensionList = dimensionList;
  }

  @Override
  public void parse(UserInformation user,Map<String,Object> set){
    addFilter(user,set);
    Collections.sort(dimensionList);
    addDimensionNameCrossTerm(user, dimensionList);
    addDimensionNameCrossTerm(user, dimensionList);
  }
  public void addFilter(
      UserInformation user, Map<String, Object> set
  )
  {
    if (set.get("type").equals("and") || set.get("type").equals("or")){
      for (Map<String,Object> filter:(List<Map<String,Object>>)set.get("filters")){
        parse(user, filter);
      }
    }
    else if (set.get("type").equals("not")){
      parse(user, (Map<String, Object>) set.get("filter"));
    }

    else if(!set.get("attribute").equals("timestamp")){
      if (set.get("type").equals("is")){
        incrementMap((String) set.get("attribute"),user.getDimensionNames());
        dimensionList.add((String) set.get("attribute"));
        if (user.getDimensionValues().get(set.get("attribute"))==null){
          user.getDimensionValues().put((String) set.get("attribute"), new HashMap<String,Integer>());
        }

        incrementMap((String) set.get("value"),user.getDimensionValues().get(set.get("attribute")));
      }
      else if (set.get("type").equals("in")){
        String attribute = (String) set.get("attribute");
        dimensionList.add((String) set.get("attribute"));
        for(String name: (ArrayList<String>) set.get("values")){
          addDimensionType(user, attribute);
          incrementMap(name,user.getDimensionValues().get(attribute));
        }
      }
    }                                Bu
  }

  public void addDimensionNameCrossTerm(UserInformation user, List<String> dimensionList){
    StringBuilder builder = new StringBuilder();
    for (String dimensionName:dimensionList){
      builder.append(dimensionName);
      builder.append(",");
    }
    incrementMap(builder.toString(),user.getDimensionNameCrossTerm());
  }
}
