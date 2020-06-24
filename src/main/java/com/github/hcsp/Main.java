package com.github.hcsp;

// 用于调度多个线程
public class Main {
  public static void main(String[] args) {
    CrawlerDao dao = new MyBatisCrawlerDao();
    for (int i = 0; i < 8; ++i) {
      new Crawler(dao).start();
    }
  }
}
