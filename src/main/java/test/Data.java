package test;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Data {
    private Integer id;
    private String value;
    private Map<Integer, String> dataCollection;

    public Data(Integer id, String value) {
        this.id = id;
        this.value = value;
        this.dataCollection = new ConcurrentHashMap<Integer, String>();
    }

    public Data() {
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public void addOrReplaceValue(Integer key, String value) {
        dataCollection.put(key, value);
    }

    public String getCollectionValue(Integer key) {
        return dataCollection.get(key);
    }

    @Override
    public String toString() {
        return "Data{" +
                "id=" + id +
                ", value='" + value + '\'' +
                ", dataCollection=" + dataCollection +
                '}';
    }
}
