package org.CS6240_project;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;


import java.io.*;
import java.util.*;
import java.util.regex.Pattern;


public class TwitterDataProcessor {

    private static Set<String> stopWords = loadStopWords();
    private static Pattern emojiPattern = Pattern.compile("[\\p{So}\\p{Cn}]");

    public static void main(String[] args) {
        long startTime = System.nanoTime();
        /* File datasetDir = new File("/Users/xiexiaoyang/Documents/NEU learning/Spring 2024/CS6240/project/source data/2023/1/1/0");
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, HashMap<String, Integer>> userTermFrequencies = new HashMap<>();
        String outputCsvFilePath = "/Users/xiexiaoyang/Documents/NEU learning/Spring 2024/CS6240/project/source data/2023/twitter_data.csv";

        if (datasetDir.exists() && datasetDir.isDirectory()) {
            File[] bz2Files = datasetDir.listFiles((dir, name) -> name.endsWith(".bz2"));
            if (bz2Files != null) {
                for (File bz2File : bz2Files) {
                    processBz2File(bz2File, objectMapper, userTermFrequencies);
                }

                // Write term frequencies to CSV
                try (PrintWriter csvWriter = new PrintWriter(new FileWriter(outputCsvFilePath))) {
                    userTermFrequencies.forEach((userId, terms) -> {
                        terms.forEach((term, frequency) -> {
                            csvWriter.println(String.format("\"%s\",\"%s\",%d", userId, term, frequency));
                        });
                    });
                } catch (IOException e) {
                    System.err.println("Error writing to CSV file: " + e.getMessage());
                }
            } else {
                System.out.println("No .bz2 files found in the directory.");
            }
        } else {
            System.out.println("The specified path does not exist or is not a directory.");
        }

        long endTime = System.nanoTime();
        long durationInNano = (endTime - startTime);
        double durationInSeconds = durationInNano / 1_000_000_000.0; // Total execution time in seconds
        System.out.println("Execution time in seconds: " + durationInSeconds);
    }
    */
        String baseDirPath = "/Users/xiexiaoyang/Documents/NEU learning/Spring 2024/CS6240/project/source data/2023/1";
        Map<String, HashMap<String, Integer>> userTermFrequencies = new HashMap<>();
        String outputCsvFilePath = "/Users/xiexiaoyang/Documents/NEU learning/Spring 2024/CS6240/project/source data/2023/twitter_data.csv";

        File baseDir = new File(baseDirPath);
        processDirectory(baseDir, userTermFrequencies);

        // Write term frequencies to CSV
        writeTermFrequenciesToCsv(outputCsvFilePath, userTermFrequencies);

        long endTime = System.nanoTime();
        long durationInNano = (endTime - startTime);
        double durationInSeconds = durationInNano / 1_000_000_000.0; // Total execution time in seconds
        System.out.println("Execution time in seconds: " + durationInSeconds);
    }

    private static void processDirectory(File dir, Map<String, HashMap<String, Integer>> userTermFrequencies) {
        ObjectMapper objectMapper = new ObjectMapper();
        // List all directories in the current directory
        File[] subDirs = dir.listFiles(File::isDirectory);
        if (subDirs != null) {
            for (File subDir : subDirs) {
                processDirectory(subDir, userTermFrequencies); // Recursively process each subdirectory
            }
        }
        // Process all bz2 files in the current directory
        File[] bz2Files = dir.listFiles((d, name) -> name.endsWith(".bz2"));
        if (bz2Files != null) {
            for (File bz2File : bz2Files) {
                processBz2File(bz2File, objectMapper, userTermFrequencies);
            }
        }
    }

    private static void processBz2File(File bz2File, ObjectMapper objectMapper,
                                        Map<String, HashMap<String, Integer>> userTermFrequencies) {
        try (
                FileInputStream fin = new FileInputStream(bz2File);
                BufferedInputStream bis = new BufferedInputStream(fin);
                BZip2CompressorInputStream bzIn = new BZip2CompressorInputStream(bis);
                BufferedReader reader = new BufferedReader(new InputStreamReader(bzIn))
        ) {
            String line;
            while ((line = reader.readLine()) != null) {
                JsonNode rootNode = objectMapper.readTree(line);
                JsonNode dataNode = rootNode.path("data");
                String author_id = dataNode.path("author_id").asText();
                String text = dataNode.path("text").asText();

                // Refactor text processing into a separate method
                processText(text, author_id, userTermFrequencies);
            }
        } catch (IOException e) {
            System.err.println("Error processing file " + bz2File.getName() + ": " + e.getMessage());
        }
    }

    // Define a new method for processing text
    private static void processText(String text, String authorId, Map<String, HashMap<String, Integer>> userTermFrequencies) {
        StringTokenizer tokenizer = new StringTokenizer(text);
        while (tokenizer.hasMoreTokens()) {
            String term = tokenizer.nextToken().toLowerCase();
            if (!stopWords.contains(term) && !emojiPattern.matcher(term).find()) {
                userTermFrequencies.computeIfAbsent(authorId, k -> new HashMap<>())
                        .merge(term, 1, Integer::sum);
            }
        }
    }


    private static Set<String> loadStopWords() {
        // Load stop words from a resource file or define them here
        Set<String> stopWords = new HashSet<>();
        // Example: Adding some common English stop words
        stopWords.addAll(Arrays.asList("a", "an", "and", "are", "as", "at", "be", "but", "by",
                "for", "if", "in", "into", "is", "it", "no", "not", "of",
                "on", "or", "such", "that", "the", "their", "then",
                "there", "these", "they", "this", "to", "was", "will", "with"));
        return stopWords;
    }

    private static void writeTermFrequenciesToCsv(String filePath, Map<String, HashMap<String, Integer>> userTermFrequencies) {
        try (PrintWriter csvWriter = new PrintWriter(new FileWriter(filePath))) {
            userTermFrequencies.forEach((userId, terms) -> {
                terms.forEach((term, frequency) -> {
                    csvWriter.println(String.format("\"%s\",\"%s\",%d", userId, term, frequency));
                });
            });
        } catch (IOException e) {
            System.err.println("Error writing to CSV file: " + e.getMessage());
        }
    }
}
