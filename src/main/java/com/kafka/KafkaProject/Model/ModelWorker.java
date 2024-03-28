package com.kafka.KafkaProject.Model;

import java.util.Date;

public class ModelWorker {

    private String  date;
    private String values;

    public ModelWorker(String date, String values) {
        this.date = date;
        this.values = values;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public String getValues() {
        return values;
    }

    public void setValues(String values) {
        this.values = values;
    }
}
