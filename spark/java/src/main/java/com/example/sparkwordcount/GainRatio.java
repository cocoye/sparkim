package com.example.sparkwordcount;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

class GainRatio {

    final static int LINE_NUMBER = 1500000;
    //linenumber为reduce输出的line_number
    int lineNumber = 0;
    static String reduceResults[][] = new String[LINE_NUMBER][4];
    //或者最多分100类
    int currentNode[] = new int[100];
    String majorityLabel = null;

    public double currentNodeEntropy() {
        double entropy;
        //当前属性的index
        int currentIndex = Integer.parseInt(reduceResults[0][0]);
        int labelMark[] = new int[LINE_NUMBER];
        int j = 0;
        int tempIndex = -1;
        int maxNumber = 0;

        while (currentIndex == Integer.parseInt(reduceResults[j][0])) {
            // 当前索引值为reduce中输出的j行的索引值
            if (labelMark[j] == 0) {
                String classLabel = reduceResults[j][2];
                tempIndex++;
                int i = j;
                while (currentIndex == Integer.parseInt(reduceResults[i][0])) {
                    if (labelMark[i] == 0) {
                        if (classLabel.contentEquals(reduceResults[i][2])) {
                            currentNode[tempIndex] += Integer.parseInt(reduceResults[i][3]);
                            labelMark[i] = 1;
                        }
                    }
                    i++;
                    if (i == lineNumber)
                        break;
                }
                if (currentNode[tempIndex] > maxNumber) {
                    maxNumber = currentNode[tempIndex];
                    majorityLabel = classLabel;
                }
                System.out.print("currentNodeValue: " + currentNode[tempIndex] + "\n" + "classLabel:" + classLabel + "\n");
            } else {
                j++;
            }
            if (j == lineNumber)
                break;
        }
        entropy = entropy(currentNode);
        return entropy;

    }

    public double entropy(int c[]) {
        double entropy = 0;
        int i = 0, j = 0;
        int sum = 0;
        double p;
        while (c[i] != 0) {
            sum += c[i];
            i++;
        }
        while (c[j] != 0) {
            p = (double) c[j] / sum;
            entropy += -(p * (Math.log(p) / Math.log(2)));
            j++;
        }
        return entropy;
    }


    public void getReduceResults() {
        try {
            FileInputStream fileInputStream = new FileInputStream(Config.pathToOutput() + JavaWordCount.currentIndex);
            DataInputStream in = new DataInputStream(fileInputStream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            String line;
            StringTokenizer itr;

            while ((line = br.readLine()) != null) {
                itr = new StringTokenizer(line);
                reduceResults[lineNumber][0] = itr.nextToken();
                reduceResults[lineNumber][1] = itr.nextToken();
                reduceResults[lineNumber][2] = itr.nextToken();
                reduceResults[lineNumber][3] = itr.nextToken();
                lineNumber++;
            }
            reduceResults[lineNumber][0] = null;
            reduceResults[lineNumber][1] = null;
            reduceResults[lineNumber][2] = null;
            reduceResults[lineNumber][3] = null;
            in.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public double gainRatioCalculator(int index, double entropy) {
        //100 is considered as max ClassLabels
        int s[][] = new int[LINE_NUMBER][100];
        int sum[] = new int[LINE_NUMBER]; //
        String currentAttributeValue = "";
        double gainRatio;
        int j = 0;
        int m = -1;                                         //m为分裂的索引,即同一属性属性值的个数
        int lines = lineNumber;                             //reduce 共有多少行

        for (int i = 0; i < lines; i++) {
            //遍历每一行reduce
            if (index == Integer.parseInt(reduceResults[i][0])) {                //如果当前属性索引恰好为reduce某一行索引
                if (reduceResults[i][1].contentEquals(currentAttributeValue)) {  //如果当前属性值恰好为reduce相应行的属性值
                    j++;
                    s[m][j] = Integer.parseInt(reduceResults[i][3]);      //c[m][j]为当前属性索引属性值的相应类的个数
                    sum[m] += s[m][j];
                } else {
                    j = 0;
                    m++;
                    currentAttributeValue = reduceResults[i][1];
                    s[m][j] = Integer.parseInt(reduceResults[i][3]);
                    sum[m] = s[m][j];
                }
            }
        }
        int i = 0;
        int sumAll = 0;
        // 计算每一个属性究竟有多少个instance
        while (sum[i] != 0) {
            sumAll += sum[i];
            i++;
        }
        double pEntropySum = 0;
        for (int k = 0; k <= m; k++) {
            double p = (double) sum[k] / sumAll;
            pEntropySum += p * entropy(s[k]);
        }
        double splitInfo = entropy(sum);
        gainRatio = (entropy - pEntropySum) / (splitInfo);
        return gainRatio;

    }

    //参数为属性索引
    public String getAttributeValues(int n) {
        int flag = 0;
        String values = "";
        String temp = "";
        for (int z = 0; z < LINE_NUMBER; z++) {
            if (reduceResults[z][0] != null) {
                if (n == Integer.parseInt(reduceResults[z][0])) {
                    //如果n等于第z行reduce 输出的属性索引
                    flag = 1;
                    if (!reduceResults[z][1].contentEquals(temp)) {
                        //取出reduce第z行描述的属性的属性值
                        values = values + " " + reduceResults[z][1];
                        temp = reduceResults[z][1];
                    }
                } else {
                    if (flag == 1)
                        break;
                }
            } else
                break;
        }
        return values;
    }

    public String majorityLabel() {
        return majorityLabel;
    }

}

