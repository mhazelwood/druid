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

import java.util.Map;

public abstract class Parser
{
  public abstract void parse(UserInformation user, Map<String,Object> set);

  public void incrementMap(String key, Map<String,Integer> map){
    if (map.get(key)!=null){
      map.put(key,map.get(key)+1);
    }
    else{
      map.put(key,1);
    }
  }
}
