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

import com.beust.jcommander.internal.Maps;
import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.testng.Assert;

import java.util.Map;

public class ApplyParserTest
{
  private final UserInformation user = new UserInformation("Joe Schmoe");
  @Test
  public void testParse() throws Exception
  {
    Map<String,Object> set = Maps.newHashMap();
    set.put("name","revenue");
    Parser applyParser = new ApplyParser(ImmutableList.<String>of("publisher"));
    applyParser.parse(user, set);
    Assert.assertEquals( user.getMetricTypes().get("revenue"),new Integer(1));
  }
}
