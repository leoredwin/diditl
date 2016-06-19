import java.sql.{Connection, DriverManager}

/**
 * Created by leo on 16/5/20.
 */
class MysqlDao {
  var connection: Connection = null

  def initConnection(): Unit = {
    val driver = "com.mysql.jdbc.Driver"
    val url = ""
    val username = ""
    val password = ""
    Class.forName(driver)
    connection = DriverManager.getConnection(url, username, password)
  }

  def insertCluster(cluster: Cluster): Boolean = {
    if (connection == null) {
      initConnection()
    }
    val districtHash = cluster.getDistrict_hash;
    val districtId = cluster.getDistrict_id;
    val prep = connection.prepareStatement("insert into tb_cluster_map (district_hash,district_id) values(?,?)")
    prep.setString(1, districtHash);
    prep.setString(2, districtId);
    prep.executeUpdate() > 0
  }

  def insertPoi(poi: Poi): Boolean = {
    if (connection == null) {
      initConnection()
    }
    val prep = connection.prepareStatement("insert into tb_poi_data (district_hash,poi_class) values(?,?)")
    prep.setString(1, poi.getDistrict_hash);
    prep.setString(2, poi.getPoi_class);
    prep.executeUpdate() > 0
  }

  def insertOrder(order: Order): Boolean = {
    if (connection == null) {
      initConnection()
    }
    val prep = connection.prepareStatement("insert into tb_order_data (order_id,driver_id,passenger_id,start_district_hash,dest_district_hash,price,time,order_time) values(?,?,?,?,?,?,?,?)")
    prep.setString(1, order.getOrder_id);
    prep.setString(2, order.getDriver_id);
    prep.setString(3, order.getPassenger_id);
    prep.setString(4, order.getStart_district_hash);
    prep.setString(5, order.getDest_district_hash);
    prep.setDouble(6, order.getPrice);
    prep.setString(7, order.getOrderTime);
    prep.setString(8, order.getTime);
    prep.executeUpdate() > 0
  }

  def insertTraffic(traffic: Traffic): Boolean = {
    if (connection == null) {
      initConnection()
    }
    val prep = connection.prepareStatement("insert into tb_traffic_data (district_hash,tj_level,tj_time,traffic_time) values(?,?,?,?)")
    prep.setString(1, traffic.getDistrict_hash);
    prep.setString(2, traffic.getTj_level);
    prep.setString(3, traffic.getTrafficTime);
    prep.setString(4, traffic.getTj_time);
    prep.executeUpdate() > 0
  }

  def insertWeather(weather: Weather): Boolean = {
    if (connection == null) {
      initConnection()
    }
    val prep = connection.prepareStatement("insert into tb_weather_data (time,weather,temperature,PM,weater_time) values(?,?,?,?,?)")
    prep.setString(1, weather.getWeatherTime);
    prep.setString(2, weather.getWeather);
    prep.setString(3, weather.getTemperature);
    prep.setString(4, weather.getPm25);
    prep.setString(5, weather.getTime);
    prep.executeUpdate() > 0
  }

  def close(): Unit = {
    if (connection != null && !connection.isClosed) {
      connection.close()
    }
  }

}
