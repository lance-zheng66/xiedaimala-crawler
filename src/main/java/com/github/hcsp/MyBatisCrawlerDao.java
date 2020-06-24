package com.github.hcsp;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

public class MyBatisCrawlerDao implements CrawlerDao{
  private SqlSessionFactory sqlSessionFactory;
  public MyBatisCrawlerDao() {
    try {
      String resource = "db/mybatis/config.xml";
      InputStream inputStream = Resources.getResourceAsStream(resource);
      sqlSessionFactory = new SqlSessionFactoryBuilder().build(inputStream);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

  }

  //避免多个线程爬同一个代码
  @Override
  public synchronized String getNextLinkThenDelete() throws SQLException {
    try (SqlSession session = sqlSessionFactory.openSession(true)) {
      String url = (String) session.selectOne("com.github.hcsp.MyMapper.selectNextAvailableLink");
      if (url!=null){
        session.delete("com.github.hcsp.MyMapper.deleteLink", url);
      }
      return url;
    }
  }

  @Override
  public void insertNewsIntoDatabase(String url, String title, String content) throws SQLException {
    try (SqlSession session = sqlSessionFactory.openSession(true)) {
      session.insert("com.github.hcsp.MyMapper.insertNews", new News(url, title, content));
    }
  }

  @Override
  public boolean isLinkProcessed(String link) throws SQLException {
    try (SqlSession session = sqlSessionFactory.openSession()) {
      int count= (Integer) session.selectOne("com.github.hcsp.MyMapper.countLink", link);
      return count!=0;
    }
  }

  @Override
  public void insertProcessedLink(String link) {
    //insert into links_already_processed(link)values(#link)
    Map<String, Object> param = new HashMap<>();
    param.put("tableName", "links_already_processed");
    param.put("link", link);
    try (SqlSession session = sqlSessionFactory.openSession(true)) {
      session.insert("com.github.hcsp.MyMapper.insertLink", param);
    }
  }


  @Override
  public void insertLinkToBeProcesssed(String link) {
 //insert into links_already_processed(link)values(#link)
    Map<String, Object> param = new HashMap<>();
    param.put("tableName", "links_to_be_processed");
    param.put("link", link);
    try (SqlSession session = sqlSessionFactory.openSession(true)) {
      session.insert("com.github.hcsp.MyMapper.insertLink", param);
    }
  }
}
