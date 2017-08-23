package com.darren.hadoop.transfer;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ComplexParameter  implements Serializable  {
    /**
     * 
     */
    private static final long serialVersionUID = 3689550651827430036L;
    private String name;
    private SimpleParameter silpleParameter;
    private Map<String, String> simpleMap = new HashMap<>();
    private Map<String, SimpleParameter> complexMap = new HashMap<>();
    private List<SimpleParameter> complexList = new ArrayList<>();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public SimpleParameter getSilpleParameter() {
        return silpleParameter;
    }

    public void setSilpleParameter(SimpleParameter silpleParameter) {
        this.silpleParameter = silpleParameter;
    }

    public Map<String, String> getSimpleMap() {
        return simpleMap;
    }

    public void setSimpleMap(Map<String, String> simpleMap) {
        this.simpleMap = simpleMap;
    }

    public Map<String, SimpleParameter> getComplexMap() {
        return complexMap;
    }

    public void setComplexMap(Map<String, SimpleParameter> complexMap) {
        this.complexMap = complexMap;
    }

    public List<SimpleParameter> getComplexList() {
        return complexList;
    }

    public void setComplexList(List<SimpleParameter> complexList) {
        this.complexList = complexList;
    }

    @Override
    public String toString() {
        return "ComplexParameter [name=" + name + ", silpleParameter=" + silpleParameter + ", simpleMap=" + simpleMap
                + ", complexMap=" + complexMap + ", complexList=" + complexList + "]";
    }

}
