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

import com.google.common.collect.Maps;

import java.util.Map;

public class UserInformation
{
  private final String userName;
  private final Map<String,DimensionValueStats> statMap =Maps.newHashMap();
  private final Map<String,Integer> nameMetricCrossTerm = Maps.newHashMap();
  private final Map<String,Integer> dimensionNameCrossTerm = Maps.newHashMap();

  public Map<String, Integer> getDimensionNameCrossTerm()
  {
    return dimensionNameCrossTerm;
  }

  public Map<String, Integer> getNameMetricCrossTerm()
  {
    return nameMetricCrossTerm;
  }

  private final Map<String,Integer> metricTypes = Maps.newHashMap();

  public UserInformation(
      String userName
  )
  {
    this.userName = userName;
  }

  public String getUserName()
  {
    return userName;
  }

  public Map<String,DimensionValueStats> getStatMap()
  {
    return statMap;
  }

  public Map<String, Integer> getMetricTypes()
  {
    return metricTypes;
  }
}
