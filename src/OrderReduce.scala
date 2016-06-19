import java.io.{File, FilenameFilter}
import java.text.SimpleDateFormat
import java.util.Calendar

import scala.collection.mutable
import scala.io.Source

/**
 * Created by leo on 16/5/20.
 */
object OrderReduce {

  val dateFormater = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
  val dateFormater1 = new SimpleDateFormat("yyyy-MM-dd")

  def main(args: Array[String]) {

    val cluster = Source.fromFile("/Users/leo/Downloads/season_1/training_data/cluster_map/cluster_map").getLines().map(param => {
      val tmp = param.split("\t")
      tmp(0) -> tmp(1)
    }).toMap

    val poi = Source.fromFile("/Users/leo/Downloads/season_1/training_data/poi_data/poi_data").getLines().map(param => {
      val tmp1 = param.substring(0, param.indexOf("\t"));
      val tmp2 = param.substring(param.indexOf("\t") + 1).replaceAll("\t", "-");
      tmp1 -> tmp2
    }).toMap

    val mysqlDao = new MysqlDao

    val orderFileDir = new File("/Users/leo/Downloads/season_1/training_data/order_data");
    val trafficFileDir = new File("/Users/leo/Downloads/season_1/training_data/traffic_data");
    val weatherFileDir = new File("/Users/leo/Downloads/season_1/training_data/weather_data")
  /*  val fileNameFilter = new FilenameFilter {
      override def accept(dir: File, name: String): Boolean = {
        !name.equals(".DS_Store")
      }
    }*/
    val orderFileMap = orderFileDir.listFiles().map(file => {
      val fileName = file.getName.substring(file.getName.lastIndexOf("_") + 1);
      fileName -> file
    }).toMap

    val trafficFileMap = trafficFileDir.listFiles().map(file => {
      val fileName = file.getName.substring(file.getName.lastIndexOf("_") + 1);
      fileName -> file
    }).toMap

    val weatherFileMap = weatherFileDir.listFiles().map(file => {
      val fileName = file.getName.substring(file.getName.lastIndexOf("_") + 1);
      fileName -> file
    }).toMap
    println("开始处理数据。。。。")
    //开始处理
    orderFileDir.listFiles().foreach(file => {
      val fileName = file.getName.substring(file.getName.lastIndexOf("_") + 1);

      val trafficFile = trafficFileMap.get(fileName)

      val weatherFile = weatherFileMap.get(fileName)

      val data = mutable.Map[String,EtlBean]()

      val trafficMap = Source.fromFile(trafficFile.get).getLines().map(param => {
        val tmp = param.split("\t");
        val key = new StringBuilder().append(tmp(0)).append("_").append(getTimePeriod(tmp(5)))
        key.toString -> new StringBuilder().append(tmp(1)).append("-").append(tmp(2)).append("-").append(tmp(3)).append("-").append(tmp(4)).toString
      }).toMap
      val weatherMap = Source.fromFile(weatherFile.get).getLines().map(param => {
        getTimePeriod(param.substring(0, param.indexOf("\t"))).toString -> param
      }).toMap


      Source.fromFile(file).getLines().foreach(param => {
        val orderItems = param.split("\t")
        val period = getTimePeriod(orderItems(6))
        val driver = orderItems(1)
        val district = orderItems(3)
        val price = orderItems(5)
        val etlBean = data.get(district + "_" + period)
        if (etlBean.isEmpty) {
          val etlBean = new EtlBean()
          etlBean.setAll_order(1)
          etlBean.setAll_price(java.lang.Double.parseDouble(price))
          if (driver.equals("NULL")) {
            etlBean.setGap_order(1)
          } else {
            etlBean.setAccpt_order(1)
            etlBean.setGap_order(0)
          }


          //获取天气信息
          val weather = getOptionFromMap(weatherMap, period, null).get.split("\t")

          etlBean.setWeather(weather(1))
          etlBean.setPm25(weather(3))
          etlBean.setStart_district_id(cluster.get(district).get)
          etlBean.setTemperature(weather(2))
          etlBean.setTime_period(period + "")
          etlBean.setDate(orderItems(6))
          etlBean.setPoi_class(poi.get(district).get)
          etlBean.setTj_level(getOptionFromMap(trafficMap, period, district + "_").getOrElse(null))
          val week = Calendar.getInstance()
          week.setTime(dateFormater.parse(orderItems(6)))
          if (week.get(Calendar.DAY_OF_WEEK) < 6)
            etlBean.setIsweek(0)
          else
            etlBean.setIsweek(1)
          data.put(district + "_" + period, etlBean)
        } else {
          etlBean.get.setAll_order(etlBean.get.getAll_order + 1)
          if (driver.equals("NULL")) {
            etlBean.get.setGap_order(etlBean.get.getGap_order + 1)
          } else {
            etlBean.get.setAccpt_order(etlBean.get.getAccpt_order + 1)
          }
          etlBean.get.setAll_price(etlBean.get.getAll_price + java.lang.Double.parseDouble(price))
        }
      })
      data.values.foreach(etlBean => {
        mysqlDao.insertEtlData(etlBean)
      })
      println("一批数据处理完毕。。。")
    })
    println("处理数据结束。。。。")

  }

  def getTimePeriod(time: String): Long = {
    (dateFormater.parse(time).getTime - dateFormater1.parse(time).getTime) / 600000 + 1;
  }

  def getOptionFromMap(map: Map[String, String], period: Long, addedIndex: String): Option[String] = {
    var tmp: Option[String] = Option.empty;
    var acc = 0;
    var flag: Boolean = false;
    while (tmp.isEmpty && !flag) {
      var outIndex = false;
      var index = period + acc;
      if (index > 144) {
        outIndex = true;
      } else {
        if (addedIndex != null) {
          tmp = map.get(addedIndex + index)
        } else {
          tmp = map.get(index + "")
        }
      }
      if (tmp.isEmpty) {
        index = period - acc
        if (index < 1 && outIndex) {
          flag = true
        } else {
          if (addedIndex != null) {
            tmp = map.get(addedIndex + index)
          } else {
            tmp = map.get(index + "")
          }
        }
      }
      acc = acc + 1
    }
    tmp
  }


}
