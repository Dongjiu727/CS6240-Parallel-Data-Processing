REGISTER /Users/xiexiaoyang/pig-0.17.0/lib/piggybank.jar;
-- REGISTER file:/home/hadoop/lib/pig/piggybank.jar;

DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader;
-- Set 10 reduce tasks
SET default_parallel 10;

-- Load Data
Flight1 = LOAD '/input/data.csv' USING CSVLoader(',');
F1 = FOREACH Flight1 GENERATE
                              (int) $0 AS year1,
                              (int) $2 AS month1,
                        (chararray) $5 AS flightDate1,
                        (chararray) $11 AS origin1,
                        (chararray) $17 AS dest1,
                              (int) $24 AS depTime1,
                              (int) $35 AS arrTime1,
                              (int) $37 AS arrDelayMinutes1,
                              (int) $41 AS cancelled1,
                              (int) $43 AS diverted1;

F2 = FOREACH Flight1 GENERATE
                             (int) $0 AS year2,
                             (int) $2 AS month2,
                       (chararray) $5 AS flightDate2,
                       (chararray) $11 AS origin2,
                       (chararray) $17 AS dest2,
                             (int) $24 AS depTime2,
                             (int) $35 AS arrTime2,
                             (int) $37 AS arrDelayMinutes2,
                             (int) $41 AS cancelled2,
                             (int) $43 AS diverted2;

-- FILTER data BY origin, destination, cancelled, diverted ,Year and Month
   F1_filter = FILTER F1 BY origin1 == 'ORD'
                       AND dest1 != 'JFK'
                       AND cancelled1 == 0
                       AND diverted1 == 0
                       AND ((year1 == 2007 AND month1 >= 6) OR (year1 == 2008 AND month1 <= 5));

   F2_filter = FILTER F2 BY origin2 != 'ORD'
                       AND dest2 == 'JFK'
                       AND cancelled2 == 0
                       AND diverted2 == 0
                       AND ((year2 == 2007 AND month2 >= 6) OR (year2 == 2008 AND month2 <= 5));

-- JOIN F1_filter and F2_filter ON flightDate and dest1, origin2
F1F2_sameDate_dest = JOIN F1_filter BY (flightDate1, dest1), F2_filter BY (flightDate2, origin2);

-- FILTER data BY Flight1 arrTime and Flight 2 depTime
F1F2_time = FILTER F1F2_sameDate_dest BY (arrTime1 < depTime2);

-- GENERATE Flight 1 arrDelayMin + Flight 2 arrDelayMin AS totalDelay
F1F2_totalDelay = FOREACH F1F2_time GENERATE (arrDelayMinutes1 + arrDelayMinutes2) AS totalDelay;

-- GROUP the relation
F1F2_group = GROUP F1F2_totalDelay ALL;

-- GENERATE AVE (totalDelay) AS finalTotalDelay, COUNT (totalDelay) AS countPairs
result = FOREACH F1F2_group GENERATE AVG(F1F2_totalDelay.totalDelay) AS finalTotalDelay, COUNT(F1F2_totalDelay.totalDelay) AS countPairs;

-- STORE the output result
STORE result INTO '/Users/xiexiaoyang/output_directory';