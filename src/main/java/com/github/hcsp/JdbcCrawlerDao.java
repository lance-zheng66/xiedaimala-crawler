package com.github.hcsp;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class JdbcCrawlerDao implements CrawlerDao {

  private final Connection connection;

  public JdbcCrawlerDao() {
    try {
      this.connection = DriverManager
          .getConnection("jdbc:h2:file:D:/maven-projects/xiedaimala-crawler/news");
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public String getNextLink(String sql) throws SQLException {

    ResultSet resultSet = null;

    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      resultSet = statement.executeQuery();
      while (resultSet.next()) {
        return resultSet.getString(1);
      }
    } finally {
      if (resultSet != null) {
        resultSet.close();
      }
    }
    return null;
  }

  public String getNextLinkThenDelete() throws SQLException {
    String link = getNextLink("select link from LINKS_TO_BE_PROCESSED LIMIT 1");
    if (link != null) {
      updateDataBase(link, "DELETE FROM LINKS_TO_BE_PROCESSED where link = ?");
    }
    return link;
  }

  public void updateDataBase(String link, String sql) throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setString(1, link);
      statement.executeUpdate();
    }
  }

  public void insertNewsIntoDatabase(String url, String title, String content) throws SQLException {
    try (PreparedStatement statement = connection.prepareStatement(
        "insert into news(url,title,content,create_at, modified_at)values ( ?,?,?,now(),now() )")) {
      statement.setString(1, url);
      statement.setString(2, title);
      statement.setString(3, content);
      statement.executeUpdate();
    }
  }

  public boolean isLinkProcessed(String link) throws SQLException {
    ResultSet resultSet = null;
    try (PreparedStatement statement =
        connection.prepareStatement("SELECT LINK from LINKS_ALREADY_PROCESSED where link = ?")) {
      statement.setString(1, link);
      resultSet = statement.executeQuery();
      while (resultSet.next()) {
        return true;
      }
    } finally {
      if (resultSet != null) {
        resultSet.close();
      }
    }
    return false;
  }
}
