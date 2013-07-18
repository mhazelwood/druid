///*
//* Druid - a distributed column store.
//* Copyright (C) 2012  Metamarkets Group Inc.
//*
//* This program is free software; you can redistribute it and/or
//* modify it under the terms of the GNU General Public License
//* as published by the Free Software Foundation; either version 2
//* of the License, or (at your option) any later version.
//*
//* This program is distributed in the hope that it will be useful,
//* but WITHOUT ANY WARRANTY; without even the implied warranty of
//* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//* GNU General Public License for more details.
//*
//* You should have received a copy of the GNU General Public License
//* along with this program; if not, write to the Free Software
//* Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
//*/
//package druid.examples.personalization;
//
//import com.beust.jcommander.internal.Lists;
//import com.beust.jcommander.internal.Maps;
//import com.google.common.collect.ImmutableList;
//import org.junit.BeforeClass;
//import org.junit.Test;
//import org.testng.Assert;
//
//import java.util.List;
//import java.util.Map;
//
//public class FilterParserTest
//{
//
//  private static final Map<String,Object> basicIsFilter1 = Maps.newHashMap();
//  private static final Map<String,Object> basicIsFilter2 = Maps.newHashMap();
//  @BeforeClass
//  public static void setUp(){
//    basicIsFilter1.put("type","is");
//    basicIsFilter1.put("attribute","publisher");
//    basicIsFilter1.put("value","zynga");
//
//    basicIsFilter2.put("type","is");
//    basicIsFilter2.put("attribute","site");
//    basicIsFilter2.put("value","site1");
//
//  }
//
//  @Test
//  public void testIsFilter() throws Exception
//  {
//    UserInformation user = new UserInformation("Joe Schmoe");
//    Parser filterParser = new FilterParser();
//    filterParser.parse(user, basicIsFilter1);
//    Assert.assertEquals(user.getDimensionNames().get("publisher"), new Integer(1));
//    Assert.assertEquals(user.getDimensionValues().get("publisher").get("zynga"),new Integer(1));
//  }
//
//  @Test
//  public void testIsFilterIncrementsCorrectly() throws Exception{
//    UserInformation user = new UserInformation("Joe Schmoe");
//    Parser filterParser = new FilterParser();
//    filterParser.parse(user, basicIsFilter1);
//    filterParser.parse(user, basicIsFilter1);
//    Assert.assertEquals(user.getDimensionNames().get("publisher"), new Integer(2));
//    Assert.assertEquals(user.getDimensionValues().get("publisher").get("zynga"), new Integer(2));
//  }
//
//  @Test
//  public void testWithinFilter() throws Exception
//  {
//    UserInformation user = new UserInformation("Joe Schmoe");
//    Map<String,Object> set = Maps.newHashMap();
//    set.put("type","within");
//    set.put("attribute","site");
//    set.put("value", ImmutableList.<String>of("site1","site2","site3"));
//    Parser filterParser = new FilterParser();
//    filterParser.parse(user,set);
//    Assert.assertEquals(user.getDimensionNames().get("site"), new Integer(3));
//    Assert.assertEquals(user.getDimensionValues().get("site").get("site1"), new Integer(1));
//    Assert.assertEquals(user.getDimensionValues().get("site").get("site2"), new Integer(1));
//    Assert.assertEquals(user.getDimensionValues().get("site").get("site3"), new Integer(1));
//  }
//
//  @Test
//  public void testNotFilter() throws Exception
//  {
//    UserInformation user = new UserInformation("Joe Schmoe");
//    Map<String,Object> set = Maps.newHashMap();
//    set.put("type","not");
//    set.put("filter",basicIsFilter2);
//    Parser filterParser = new FilterParser();
//    filterParser.parse(user,set);
//    Assert.assertEquals(user.getDimensionNames().get("site"),new Integer(1));
//    Assert.assertEquals(user.getDimensionValues().get("site").get("site1"), new Integer(1));
//  }
//
//  @Test
//  public void testOrFilter() throws Exception
//  {
//    UserInformation user = new UserInformation("Joe Schmoe");
//    Map<String,Object> set = Maps.newHashMap();
//    set.put("type","or");
//    List<Map<String,Object>> filterList = Lists.newArrayList();
//    filterList.add(basicIsFilter1);
//    filterList.add(basicIsFilter2);
//    set.put("filters",filterList);
//    Parser filterParser = new FilterParser();
//    filterParser.parse(user,set);
//    Assert.assertEquals(user.getDimensionNames().get("publisher"), new Integer(1));
//    Assert.assertEquals(user.getDimensionValues().get("publisher").get("zynga"), new Integer(1));
//    Assert.assertEquals(user.getDimensionNames().get("site"),new Integer(1));
//    Assert.assertEquals(user.getDimensionValues().get("site").get("site1"), new Integer(1));
//  }
//
//  @Test
//  public void testAndFilter() throws Exception
//  {
//    UserInformation user = new UserInformation("Joe Schmoe");
//    Map<String,Object> set = Maps.newHashMap();
//    set.put("type","and");
//    List<Map<String,Object>> filterList = Lists.newArrayList();
//    filterList.add(basicIsFilter1);
//    filterList.add(basicIsFilter2);
//    set.put("filters",filterList);
//    Parser filterParser = new FilterParser();
//    filterParser.parse(user,set);
//    Assert.assertEquals(user.getDimensionNames().get("publisher"), new Integer(1));
//    Assert.assertEquals(user.getDimensionValues().get("publisher").get("zynga"), new Integer(1));
//    Assert.assertEquals(user.getDimensionNames().get("site"),new Integer(1));
//    Assert.assertEquals(user.getDimensionValues().get("site").get("site1"), new Integer(1));
//  }
//
//
//}
