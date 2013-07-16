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
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BasicParseWithUsername
{
  private final static SetParser parser = new SetParser();
  private final static Map<String,UserInformation> userMap = Maps.newHashMap();
  public static void main(String [] args){
    BufferedReader br = null;
    ObjectMapper mapper = new ObjectMapper();
    TypeReference<Map<String,Object>> typeReference = new TypeReference<Map<String, Object>>()
    {
      @Override
      public Type getType()
      {
        return super.getType();    //To change body of overridden methods use File | Settings | File Templates.
      }
    };
    try{
      String currentLine;
      br = new BufferedReader(new FileReader("/Users/dhruvparthasarathy/Desktop/userQueries2.log"));
      while ((currentLine = br.readLine()) != null) {
      String nameRegExp="( )([^ ]*@[^ ]*)";
        String queryRegExp="(body=)(.*)";
        Pattern namePattern = Pattern.compile(nameRegExp);
        Matcher nameMatcher = namePattern.matcher(currentLine);
        Pattern queryPattern = Pattern.compile(queryRegExp);
        Matcher queryMatcher = queryPattern.matcher(currentLine);
        if (nameMatcher.find()&&queryMatcher.find()){
          String userName = nameMatcher.group(2);
          UserInformation user = userMap.get(userName);
          if (user==null){
            user = new UserInformation(nameMatcher.group(2));
            userMap.put(userName,user);
          }
          Map<String,Object> map = mapper.readValue(queryMatcher.group(2),typeReference);
          try{
            List<Map<String,Object>> operationList = (List<Map<String,Object>>) map.get("query");
            parser.parse(operationList, user);
          }
          catch (Exception e){
            e.printStackTrace();
          }
        }
      }
    }
    catch (FileNotFoundException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
    catch (IOException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
    int x=5;
  }


}
