
/**
 * Created by leo on 16/5/20.
 */
public class EtlBean {

    private String start_district_id;
    private String date;
    private String time_period;
    private String tj_level;
    private String poi_class;
    private String weather;
    private String temperature;
    private String pm25;
    private int accpt_order;
    private int gap_order;
    private double all_price;
    private int all_order;
    private int isweek;

    public String getStart_district_id() {
        return start_district_id;
    }

    public void setStart_district_id(String start_district_id) {
        this.start_district_id = start_district_id;
    }

    public String getDate() {
        if (date != null)
            return "'" + date + "'";
        return null;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getTime_period() {
        return time_period;
    }

    public void setTime_period(String time_period) {
        this.time_period = time_period;
    }

    public String getTj_level() {
        if (tj_level != null)
            return "'" + tj_level + "'";
        return null;
    }

    public void setTj_level(String tj_level) {
        this.tj_level = tj_level;
    }

    public String getPoi_class() {
        if (poi_class != null)
            return "'" + poi_class + "'";
        return null;
    }

    public void setPoi_class(String poi_class) {
        this.poi_class = poi_class;
    }

    public String getWeather() {
        return weather;
    }

    public void setWeather(String weather) {
        this.weather = weather;
    }

    public String getTemperature() {
        return temperature;
    }

    public void setTemperature(String temperature) {
        this.temperature = temperature;
    }

    public String getPm25() {
        return pm25;
    }

    public void setPm25(String pm25) {
        this.pm25 = pm25;
    }

    public int getAccpt_order() {
        return accpt_order;
    }

    public void setAccpt_order(int accpt_order) {
        this.accpt_order = accpt_order;
    }

    public int getGap_order() {
        return gap_order;
    }

    public void setGap_order(int gap_order) {
        this.gap_order = gap_order;
    }

    public double getAll_price() {
        return all_price;
    }

    public void setAll_price(double all_price) {
        this.all_price = all_price;
    }

    public int getAll_order() {
        return all_order;
    }

    public void setAll_order(int all_order) {
        this.all_order = all_order;
    }

    public int getIsweek() {
        return isweek;
    }

    public void setIsweek(int isweek) {
        this.isweek = isweek;
    }

    @Override
    public String toString() {
        return "EtlBean{" +
                "start_district_id='" + start_district_id + '\'' +
                ", date='" + date + '\'' +
                ", time_period='" + time_period + '\'' +
                ", tj_level='" + tj_level + '\'' +
                ", poi_class='" + poi_class + '\'' +
                ", weather='" + weather + '\'' +
                ", temperature='" + temperature + '\'' +
                ", pm25='" + pm25 + '\'' +
                ", accpt_order=" + accpt_order +
                ", gap_order=" + gap_order +
                ", all_price=" + all_price +
                ", all_order=" + all_order +
                '}';
    }
}
