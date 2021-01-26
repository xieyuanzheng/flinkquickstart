package org.myorg.quickstart.stream.keySelect;

import java.io.Serializable;

public class WordCountData implements Serializable {

    private static final long serialVersionUID = 3838761161860672554L;

    private String name;
    private int count;

    public WordCountData() {
    }

    public WordCountData(String name, int count) {
        this.name = name;
        this.count = count;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "WordCountData{" +
                "name='" + name + '\'' +
                ", count=" + count +
                '}';
    }
}
