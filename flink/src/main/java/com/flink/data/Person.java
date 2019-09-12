package com.flink.data;

import lombok.Data;

/**
 * @author fangpc
 */

@Data
public class Person {

    private String name;

    private int id;

    public Person(String name, int id) {
        this.name = name;
        this.id = id;
    }
}
