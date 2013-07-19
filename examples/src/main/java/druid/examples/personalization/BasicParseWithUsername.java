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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;

import javax.sql.rowset.serial.SerialBlob;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Type;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BasicParseWithUsername
{
  private final static SetParser parser = new SetParser();
  private final static Map<String,UserInformation> userMap = Maps.newHashMap();

  private static Connection con = null;
  private static PreparedStatement ps = null;
  private static ResultSet rs = null;
  private static String url = "jdbc:mysql://127.0.0.1:3306/dhruv";
  private static String user = "root";
  private static String password = "";

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
      con = DriverManager.getConnection(url, user, password);
      Statement st = (Statement) con.createStatement();
      String currentLine;
      br = new BufferedReader(new FileReader("/Users/dhruvparthasarathy/Desktop/userQueries2.log"));
      while ((currentLine = br.readLine()) != null) {
      String nameRegExp="( )([^ ]*@[^ ]*)";
        String queryRegExp="(body=)(.*)";
        Matcher nameMatcher = getMatcher(nameRegExp, currentLine);
        Matcher queryMatcher = getMatcher(queryRegExp, currentLine);
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
      for (String user: userMap.keySet()){
        ps = con.prepareStatement("INSERT INTO USERS(NAME, DIMENSION_VALUES, DIMENSION_NAMES) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE DIMENSION_VALUES=?, DIMENSION_NAMES=?");
        ps.setString(1, user);
        ps.setBlob(2, getBlob(userMap.get(user).getDimensionValues()));
        ps.setBlob(3, getBlob(userMap.get(user).getDimensionNameCrossTerm()));
        ps.setBlob(4, getBlob(userMap.get(user).getDimensionValues()));
        ps.setBlob(5, getBlob(userMap.get(user).getDimensionNameCrossTerm()));
        ps.executeUpdate();
      }
      con.close();
    }
    catch (FileNotFoundException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
    catch (IOException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
    catch (SQLException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
    int x=5;
  }

  public static Blob getBlob(Object obj) throws IOException, SQLException
  {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ObjectOutputStream out = new ObjectOutputStream(bos);
    out.writeObject(obj);
    byte[] byteStream = bos.toByteArray();
    return new SerialBlob(byteStream);
  }

  public static Object deserializeBlob(Blob blob) throws IOException, ClassNotFoundException, SQLException
  {
    ByteArrayInputStream b = new ByteArrayInputStream(blob.getBytes(1,(int)blob.length()));
    ObjectInputStream o = new ObjectInputStream(b);
    blob.free();
    return o.readObject();
  }


  public static Matcher getMatcher(String regexp, String currentLine){
    Pattern pattern = Pattern.compile(regexp);
    Matcher matcher = pattern.matcher(currentLine);
    return matcher;
  }


}
