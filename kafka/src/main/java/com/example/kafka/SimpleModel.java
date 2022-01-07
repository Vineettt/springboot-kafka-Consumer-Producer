package com.example.kafka;

public class SimpleModel {
    private String field1;

    public String getField1() {
        return this.field1;
    }

    public void setField1(String field1) {
        this.field1 = field1;
    }

    public String getField2() {
        return this.field2;
    }

    public void setField2(String field2) {
        this.field2 = field2;
    }
    private String field2;

    public SimpleModel(){}

    public SimpleModel(String field1, String field2){
        this.field1 = field1;
        this.field2 = field2;
    }
}
