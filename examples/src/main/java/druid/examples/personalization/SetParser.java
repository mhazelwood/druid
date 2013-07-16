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

import java.util.List;
import java.util.Map;

public class SetParser
{
  private List<String> dimensionList;
  public void parse(List<Map<String,Object>> query, UserInformation user){
    dimensionList = Lists.newArrayList();
    for (Map<String,Object> operationSet:query){
      this.parseOperation((String) operationSet.get("operation"), operationSet, user);
    }

  }

  public void parseOperation(String setType, Map<String,Object> set, UserInformation user){
    Parser parser=null;
    if (setType.equals("filter")){
      parser=new FilterParser(dimensionList);
    }
    else if (setType.equals("split")){
      parser = new SplitParser();
    }
    else if (setType.equals("apply")){
      parser = new ApplyParser(dimensionList);
    }
    if (parser!=null){
      parser.parse(user, set);
    }
  }

}
