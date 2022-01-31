package com.chandler.spark.streaming

import java.sql.{Connection, DriverManager}

object MySQLUtils {

  def getConnection() = {
    Class.forName("com.mysql.jdbc.Driver")
    DriverManager.getConnection("jdbc:mysql://8.130.29.166:3306/spark","root","rootroot")
  }

  def close(connection:Connection) = {
    if(null != connection) {
      connection.close();
    }
  }
}
