/**
 * Created by leo on 16/6/3.
 */
public class Order {
    private String order_id;
    private String driver_id;
    private String passenger_id;
    private String start_district_hash;
    private String dest_district_hash;
    private double price;
    private String time;
    private String orderTime;

    public String getOrder_id() {
        return order_id;
    }

    public void setOrder_id(String order_id) {
        this.order_id = order_id;
    }

    public String getDriver_id() {
        return driver_id;
    }

    public void setDriver_id(String driver_id) {
        this.driver_id = driver_id;
    }

    public String getPassenger_id() {
        return passenger_id;
    }

    public void setPassenger_id(String passenger_id) {
        this.passenger_id = passenger_id;
    }

    public String getStart_district_hash() {
        return start_district_hash;
    }

    public void setStart_district_hash(String start_district_hash) {
        this.start_district_hash = start_district_hash;
    }

    public String getDest_district_hash() {
        return dest_district_hash;
    }

    public void setDest_district_hash(String dest_district_hash) {
        this.dest_district_hash = dest_district_hash;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getOrderTime() {
        return orderTime;
    }

    public void setOrderTime(String orderTime) {
        this.orderTime = orderTime;
    }
}
