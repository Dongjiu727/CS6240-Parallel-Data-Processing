package org.CS6240_project;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.ObjectOutputStream;
import org.apache.hadoop.io.IOUtils;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

public class SerializeDecisionTreeModel {

    public static class DecisionTreeModel implements java.io.Serializable {
        public String makePrediction(int overallScore, int positiveCount, int negativeCount) {
            if (overallScore > 0) {
                return "positive";
            } else if (overallScore < 0) {
                return "negative";
            } else {
                if (negativeCount > positiveCount) {
                    return "negative";
                } else if (negativeCount < positiveCount) {
                    return "positive";
                } else {
                    return "neutral";
                }
            }
        }
    }

    public static void main(String[] args) {
        DecisionTreeModel tree = new DecisionTreeModel();

        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream("decisionTreeModel.ser"))) {
            oos.writeObject(tree);
        } catch (IOException e) {
            System.out.println("failed");
            e.printStackTrace();
        }
    }
}

