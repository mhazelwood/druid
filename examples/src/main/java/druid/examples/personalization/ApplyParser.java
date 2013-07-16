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

import java.util.List;
import java.util.Map;

public class ApplyParser extends Parser
{
  private List<String> dimensionList;

  public ApplyParser(List<String> dimensionList)
  {
    this.dimensionList=dimensionList;
  }

  @Override
  public void parse(
      UserInformation user, Map<String, Object> set
  )
  {
    incrementMap((String) set.get("name"),user.getMetricTypes());
    StringBuilder builder = new StringBuilder();
    for (String dimensionName:dimensionList){
      builder.append(dimensionName);
      builder.append(",");
    }
    incrementMap(builder.toString()+",    "+set.get("name"),user.getNameMetricCrossTerm());
  }
}
