package com.dekapx.spark.model;

public class Student {
    int id;
    private String firstName;
    private String lastName;
    private double marks;

    public Student() {
    }

    public Student(int id, String firstName, String lastName, double marks) {
        this.id = id;
        this.firstName = firstName;
        this.lastName = lastName;
        this.marks = marks;
    }

    public int getId() {
        return id;
    }

    public String getFirstName() {
        return firstName;
    }
    public String getLastName() {
        return lastName;
    }
    public double getMarks() {
        return marks;
    }
}
