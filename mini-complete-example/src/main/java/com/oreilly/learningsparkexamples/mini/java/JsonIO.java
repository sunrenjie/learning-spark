/**
 *  json I/O:
 *  load json from file to class objects, filter, write to output file.
 */
package com.oreilly.learningsparkexamples.mini.java;

import org.codehaus.jackson.map.ObjectMapper;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.lang.Iterable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;


public class JsonIO {
    public static class Person implements Serializable {
        public String name;
        public String favorite;
    }

    public static class ParseJson implements FlatMapFunction<Iterator<String>, Person> {
        public Iterable<Person> call(Iterator<String> lines) throws Exception {
            ArrayList<Person> people = new ArrayList<Person>();
            ObjectMapper mapper = new ObjectMapper();
            while (lines.hasNext()) {
                String l = lines.next();
                try {
                    people.add(mapper.readValue(l, Person.class));
                } catch (Exception e) {

                }
            }
            return people;
        }
    }

    public static class WriteJson implements FlatMapFunction<Iterator<Person>, String> {
        public Iterable<String> call(Iterator<Person> people) throws Exception {
            ArrayList<String> text = new ArrayList<>();
            ObjectMapper mapper = new ObjectMapper();
            while (people.hasNext()) {
                Person person = people.next();
                text.add(mapper.writeValueAsString(person));
            }
            return text;
        }
    }

    public static Person getPerson(String name, String favorate) {
        Person p = new Person();
        p.name = name;
        p.favorite = favorate;
        return p;
    }

    public static class LikesPandas implements Function<Person, Boolean> {
        public Boolean call(Person person) {
            return person.favorite.equalsIgnoreCase("panda");
        }
    }

    public static void main(String[] args) throws Exception {
        String inputFile = args[0];
        String outputFile = args[1];
        // Create a Java Spark Context.
        SparkConf conf = new SparkConf().setAppName("wordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);
        /* // to manually construct a RDD of persons
           List<Person> people = new ArrayList<>();
           people.add(getPerson("Google", "Panda"));
           people.add(getPerson("Microsoft", "Bird"));
           JavaRDD<Person> input = sc.parallelize(people);
        */
        JavaRDD<Person> input = sc.textFile(inputFile).mapPartitions(
                new ParseJson()).filter(new LikesPandas());
        input.mapPartitions(new WriteJson()).saveAsTextFile(outputFile);
    }
}
