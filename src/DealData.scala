import java.io.File
import java.text.SimpleDateFormat

import scala.collection.mutable
import scala.io.Source

/**
 * Created by leo on 16/6/3.
 */
object DealData {
  val dateFormater = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
  val dateFormater1 = new SimpleDateFormat("yyyy-MM-dd")

  def main(args: Array[String]) {

    val mysqlDao = new MysqlDao


    //区域信息处理
    val cluster = Source.fromFile("/Users/leo/Downloads/season_1/training_data/cluster_map/cluster_map").getLines().map(param => {
      val tmp = param.split("\t")
      tmp(0) -> tmp(1)
    }).toMap

    val clusterData = mutable.Map[String,Cluster]()
    cluster.foreach(param => {
      val clusterBean = new Cluster
      clusterBean.setDistrict_hash(param._1);
      clusterBean.setDistrict_id(param._2);
      clusterData.put(param._1, clusterBean);
    })

    clusterData.values.foreach(param => {
      mysqlDao.insertCluster(param);
    })


    //poi信息梳理
    val poi = Source.fromFile("/Users/leo/Downloads/season_1/training_data/poi_data/poi_data").getLines().map(param => {
      val tmp1 = param.substring(0, param.indexOf("\t"));
      val tmp2 = param.substring(param.indexOf("\t") + 1).replaceAll("\t", "-");
      tmp1 -> tmp2
    }).toMap

    val poiData = mutable.Map[String,Poi]()
    poi.foreach(param => {
      val poiBean = new Poi
      poiBean.setDistrict_hash(param._1);
      poiBean.setPoi_class(param._2);
      poiData.put(param._1, poiBean);
    })

    poiData.values.foreach(param => {
      mysqlDao.insertPoi(param);
    })


    val orderFileDir = new File("/Users/leo/Downloads/season_1/training_data/order_data");
    val trafficFileDir = new File("/Users/leo/Downloads/season_1/training_data/traffic_data");
    val weatherFileDir = new File("/Users/leo/Downloads/season_1/training_data/weather_data")
    /*  val fileNameFilter = new FilenameFilter {
        override def accept(dir: File, name: String): Boolean = {
          !name.equals(".DS_Store")
        }
      }*/

     println("开始处理order数据。。。。")
     val orderFileMap = orderFileDir.listFiles().map(file => {
       val fileName = file.getName.substring(file.getName.lastIndexOf("_") + 1);
       fileName -> file
     }).toMap

     val orderData = mutable.Map[String,Order]()
     orderFileMap.foreach(param=>{
       Source.fromFile(param._2).getLines().foreach(param => {
         val tmp = param.split("\t")
         val order = new Order
         order.setOrder_id(tmp(0))
         order.setDriver_id(tmp(1))
         order.setPassenger_id(tmp(2))
         order.setStart_district_hash(tmp(3))
         order.setDest_district_hash(tmp(4))
         order.setPrice(tmp(5).toDouble)
         order.setTime(tmp(6))
         order.setOrderTime(dateFormater.parse(tmp(6)).getTime.toString)
         orderData.put(tmp(6),order);
       })
     })

     orderData.values.foreach(param => {
       mysqlDao.insertOrder(param);
     })

     println("处理order数据结束")
    println("开始处理traffic数据。。。。")
    val trafficFileMap = trafficFileDir.listFiles().map(file => {
      val fileName = file.getName.substring(file.getName.lastIndexOf("_") + 1);
      fileName -> file
    }).toMap

    val trafficData = mutable.Map[String,Traffic]()
    trafficFileMap.foreach(param => {
      Source.fromFile(param._2).getLines().foreach(param => {
        val tmp = param.split("\t")
        val traffic = new Traffic
        traffic.setDistrict_hash(tmp(0))
        traffic.setTj_level(tmp(1) + " " + tmp(2) + " " + tmp(3) + " " + tmp(4))
        traffic.setTj_time(tmp.last)
        traffic.setTrafficTime(tmp.last)
        trafficData.put(tmp(0), traffic);
      })
    })

    trafficData.values.foreach(param => {
      mysqlDao.insertTraffic(param);
    })

    println("开始处理weather数据。。。。")
    val weatherFileMap = weatherFileDir.listFiles().map(file => {
      val fileName = file.getName.substring(file.getName.lastIndexOf("_") + 1);
      fileName -> file
    }).toMap

    val weatherData = mutable.Map[String,Weather]()
    weatherFileMap.foreach(param1 => {
      Source.fromFile(param1._2).getLines().foreach(param => {
        val tmp = param.split("\t")
        val weather = new Weather
        weather.setTime(tmp(0))
        weather.setWeather(tmp(1))
        weather.setTemperature(tmp(2))
        weather.setPm25(tmp(3))
        weather.setWeatherTime(tmp(0))
        weatherData.put(tmp(0), weather);
      })
    })

    weatherData.values.foreach(param => {
      mysqlDao.insertWeather(param);
    })

  }

}
