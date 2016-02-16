/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
package com.example.sparkwordcount;

import java.util.*;

import org.apache.commons.beanutils.converters.IntegerArrayConverter;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.SparkConf;
import scala.Tuple2;

public class JavaWordCount {
    public static Split currentSplit = new Split();

    public static List<Split> splitList = new ArrayList();

    public static int currentIndex = 0;
  public static void main(String[] args) {
      SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Spark Count");
    JavaSparkContext sc = new JavaSparkContext(sparkConf);
    String a="/home/yezi/data/playtennis.txt";
      System.out.println(System.currentTimeMillis());
      long time = System.currentTimeMillis();
      double entropy;
      double gainRatio;
      double bestGainRatio;
      String classLabel;
      int AttributeNumber = 6;
      int attributeIndex = 0;
      splitList.add(currentSplit);
      int splitListSize = splitList.size();
      GainRatio C45;
      Split newSplit;
      while (splitListSize > currentIndex) {
          currentSplit = splitList.get(currentIndex);
          C45 = new GainRatio();
          // split each document into words
          JavaPairRDD<String, Integer> tokenized = sc.textFile(args[0]).flatMapToPair(new PairFlatMapFunction<String, String,Integer>() {
              @Override
              public Iterable<Tuple2<String, Integer>> call(String s) throws Exception {
                      List<Tuple2<String, Integer>> b = new ArrayList<Tuple2<String, Integer>>();
                      Split split = currentSplit;
                      int sp_size = 0;
                      boolean flag = true;
                      StringTokenizer strTokenizer = new StringTokenizer(s);
                      int featureCount = strTokenizer.countTokens() - 1;
                      String features[] = new String[featureCount];
                      for (int i = 0; i < featureCount; i++) {
                          features[i] = strTokenizer.nextToken();
                      }
                      String classLabel = strTokenizer.nextToken();
                      sp_size = split.featureIndex.size();
                      for (int indexID = 0; indexID < sp_size; indexID++) {
                          int currentIndexID = (Integer) split.featureIndex.get(indexID);
                          String attValue = (String) split.featureValue.get(indexID);
                          if (!features[currentIndexID].equals(attValue)) {
                              flag = false;
                              break;
                          }
                      }
                      Tuple2<String, Integer> a = new Tuple2<String, Integer>("a", 1);
                      if (flag) {
                          for (int l = 0; l < featureCount; l++) {
                              if (!split.featureIndex.contains(l)) {
                                  //indexID,value,class,1
                                  a = new Tuple2<String, Integer>(l + " " + features[l] + " " + classLabel, 1);
                                  b.add(a);
                              }

                          }
                          if (sp_size == featureCount) {
                              a = new Tuple2<String, Integer>(featureCount + " " + "null" + " " + classLabel, 1);
                              b.add(a);
                          }
                      }
                      return b;
              }
          }).reduceByKey(new Function2<Integer, Integer, Integer>() {
              @Override
              public Integer call(Integer integer, Integer integer2) throws Exception {
                  return integer+integer2;
              }
          },1);
          /*flatMap(
                          new FlatMapFunction<String, Tuple2<String, Integer>>() {
                              @Override
                              public Iterable<Tuple2<String, Integer>> call(String s) throws Exception {
                                  List<Tuple2<String, Integer>> b = new ArrayList<Tuple2<String, Integer>>();
                                  Split split = currentSplit;
                                  int sp_size = 0;
                                  boolean flag = true;
                                  StringTokenizer strTokenizer = new StringTokenizer(s);
                                  int featureCount = strTokenizer.countTokens() - 1;
                                  String features[] = new String[featureCount];
                                  for (int i = 0; i < featureCount; i++) {
                                      features[i] = strTokenizer.nextToken();
                                  }
                                  String classLabel = strTokenizer.nextToken();
                                  sp_size = split.featureIndex.size();
                                  for (int indexID = 0; indexID < sp_size; indexID++) {
                                      int currentIndexID = (Integer) split.featureIndex.get(indexID);
                                      String attValue = (String) split.featureValue.get(indexID);
                                      if (!features[currentIndexID].equals(attValue)) {
                                          flag = false;
                                          break;
                                      }
                                  }
                                  Tuple2<String, Integer> a = new Tuple2<String, Integer>("a", 1);
                                  if (flag) {
                                      for (int l = 0; l < featureCount; l++) {
                                          if (!split.featureIndex.contains(l)) {
                                              //indexID,value,class,1
                                              a = new Tuple2<String, Integer>(l + " " + features[l] + " " + classLabel, 1);
                                              b.add(a);
                                          }

                                      }
                                      if (sp_size == featureCount) {
                                          a = new Tuple2<String, Integer>(featureCount + " " + "null" + " " + classLabel, 1);
                                          b.add(a);
                                      }
                                  }
                                  return b;
                              }
                              }).reduce(new*/
                                /*        // count the occurrence of each word
          JavaPairRDD<String, Integer> counts = tokenized.mapToPair(
                  new PairFunction<String, String, Integer>() {
                      @Override
                      public Tuple2<String, Integer> call(String s) {
                          return new Tuple2<String, Integer>(s, 1);
                      }
                  }
          ).reduceByKey(
                  new Function2<Integer, Integer, Integer>() {
                      @Override
                      public Integer call(Integer i1, Integer i2) {
                          return i1 + i2;
                      }
                  }, 1        //number of reducers = 1
          );*/
                              tokenized.saveAsTextFile(args[1]);
                              System.exit(0);
      }
  }
}
