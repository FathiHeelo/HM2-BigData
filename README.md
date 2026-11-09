# Students: MongoDB and Spark SQL

Big Data homework, written in Scala. A 100-row CSV of students is loaded into MongoDB, queried
with indexes, and analysed with Spark SQL. The per-course summary is written back to MongoDB
with the MongoDB Spark Connector.

## Layout

```
build.sbt                     dependencies and run settings
project/build.properties      sbt version
src/main/scala/
  ImportStudents.scala        Part 1
  QueryByCourse.scala         Part 2
  QueryByGpa.scala            Part 3
  SparkAnalysis.scala         Part 4
  Common.scala                MongoDB connection and input helpers
src/main/resources/
  log4j2.properties           keeps Spark and driver logs at WARN on stderr
data/                         students.csv (input), students.json (written by ImportStudents)
results/                      output of each program from an actual run, plus validation.txt
```

## Requirements

- JDK 17 or newer
- sbt 1.x (the project pins the sbt version in `project/build.properties`)
- MongoDB running locally

Dependencies (resolved by sbt on first run): Scala 2.13, `spark-sql` 4.2.0,
`mongo-spark-connector` 10.7.0 and `mongodb-driver-sync` 5.1.4. The driver version is the one
the connector itself resolves, so Parts 1-3 and Part 4 use the same driver.

The programs connect to `mongodb://localhost:27017` unless the `MONGO_URI` environment
variable is set:

```
set MONGO_URI=mongodb://localhost:27017        (Windows cmd)
$env:MONGO_URI = "mongodb://localhost:27017"   (PowerShell)
export MONGO_URI=mongodb://localhost:27017     (Linux/macOS)
```

## Running

Run from the repository root, in this order:

```
sbt "runMain ImportStudents"
sbt "runMain QueryByCourse"
sbt "runMain QueryByCourse --course 103"
sbt "runMain QueryByGpa"
sbt "runMain QueryByGpa --gpa 3.5"
sbt "runMain QueryByGpa --course 103 --gpa 3.5"
sbt "runMain SparkAnalysis"
```

`QueryByCourse` and `QueryByGpa` without arguments prompt for input and validate it: the
course ID must be an integer, and the GPA must be a number between 0.0 and 4.0 (NaN and
Infinity are rejected). In `QueryByGpa` pressing Enter skips a condition. Passing any argument
skips the prompts; an invalid argument prints an error and exits with code 2.
`results/validation.txt` shows these checks on piped input (`abc`, `5`, `-1`, Enter/Enter).

Add `--error` (for example `sbt --error "runMain ImportStudents"`) to hide sbt's own build
messages. Each `sbt` command starts a new JVM; for repeated runs, start `sbt` once and type
`runMain ...` at its prompt.

`build.sbt` sets `fork := true` so each program runs in its own JVM, adds the `--add-opens`
options Spark needs on JDK 17+, and sets `run / connectInput := true` so the prompts can read
from the terminal.

## Part 1: import

The `CourseIDs` column is not quoted and contains commas:

```
1,Alice,Johnson,1998-03-15,[101,102,103],3.8
```

A normal CSV reader splits the course list into several columns. Each line is split on commas
instead: the first 4 fields are id, first name, last name and date of birth, the last field is
the GPA, and everything in between is joined back into the course list (`parseLine`).

Documents are inserted into `university.students` with one `insertMany` call and these types:
`_id` int, `dateOfBirth` BSON date (UTC), `courseIds` array of int, `gpa` double. The
collection is dropped first so the program can be re-run. `students.json` is written with one
document per line and `dateOfBirth` as an ISO string (`1998-03-15T00:00:00Z`).

Indexes: `_id_` (default), `courseIds_1` (multikey, since `courseIds` is an array) and `gpa_1`.

## Part 2 and 3: MongoDB queries

- `QueryByCourse`: `{courseIds: <id>}`, sorted by last name then first name. Course 103
  returns 36 students; course 999 returns 0. `explain()` shows the winning plan
  `SORT <- FETCH <- IXSCAN (courseIds_1, isMultiKey: true)` with bounds `[103, 103]`, so the
  multikey index is used and only matching documents are fetched. The program searches the
  nested plan for IXSCAN stages and prints the index name. The sort happens in memory because
  no index covers `lastName`.
- `QueryByGpa`: `{gpa: {$gt: <value>}}`, sorted by GPA descending then last name. GPA > 3.5
  returns 49 students; course 103 and GPA > 3.5 returns 9.

## Part 4: Spark SQL

### Loading (Option B) and writing (connector)

The DataFrame is loaded with Option B: `spark.read.json("data/students.json")` with an
explicit schema (`courseIds array<int>`, `dateOfBirth timestamp`). Option B was chosen because
the explicit schema fixes the column types instead of relying on the connector to infer them
from sampled documents, and because Spark then reads exactly the data exported in Part 1.
The program prints the schema and confirms both types (`courseIds is ArrayType(IntegerType):
True`, `dateOfBirth is TimestampType: True`).

The MongoDB Spark Connector is used for 4.4: the per-course summary is written to
`university.course_stats` with `format("mongodb")` and read back the same way. The connector
is a normal sbt dependency, so it is on the classpath of `sbt run` without `spark-submit`.

`spark.sql.session.timeZone=UTC` keeps dates from shifting. The DataFrame is registered as the
`students` temp view. Every query is a single `spark.sql` statement, and the SQL is printed
before its output.

### Results

| Query | Result |
|---|---|
| 4.1.1 students in course 103 | 36, same `_id` set as MongoDB (`matches Part 2: True`) |
| 4.1.2 GPA > 3.5 | 49, same `_id` set as MongoDB (`matches Part 3: True`) |
| 4.2.1 `enrollments` view (`explode(courseIds)`) | 218 rows |
| 4.2.2 per course: students, avg GPA | 101: 48, 3.56 / 105: 47, 3.5 / 104: 44, 3.59 / 102: 43, 3.5 / 103: 36, 3.39 |
| 4.2.3 students by number of courses | 1 course: 8, 2: 66, 3: 26 |
| 4.3.1 by birth year: students, avg GPA | 1997: 47, 3.54 / 1998: 29, 3.5 / 1999: 24, 3.45 |
| 4.3.2 top 3 GPAs per course | 94 rows |
| 4.3.3 same first and last name | 7 pairs |
| 4.3.4 above the average of every course taken | 49 students |
| 4.4 course summary saved to MongoDB | 5 documents in `university.course_stats`, 0 differing rows |

For 4.1 the program also queries MongoDB with the Java driver and compares the `_id` sets. If
MongoDB is not reachable it prints a message and continues.

### Ties in 4.3.2

`DENSE_RANK() OVER (PARTITION BY courseId ORDER BY gpa DESC)` gives tied students the same
rank, and `rnk <= 3` keeps the top three distinct GPA values per course (3.9, 3.8, 3.7;
for course 103, which has no 3.7, it is 3.9, 3.8, 3.6).
There are only 9 distinct GPA values (3.1 to 3.9) across 100 students, so many students share
each value, and the query returns 94 rows instead of 15. `ROW_NUMBER` would cut ties
arbitrarily; `RANK` would leave gaps after ties.

### 4.3.3

Self-join of `students` on first and last name with `a._id < b._id`, which drops self-matches
and returns each pair once. Pairs: 53/79, 12/90, 56/82, 71/97, 28/80, 16/42, 18/70. Each pair
has different dates of birth, so they are different people with the same name.

### 4.3.4 and DECIMAL

Enrollments are joined with per-course averages, grouped by student, and kept with
`HAVING bool_and(gpa > avg_gpa)`. The averages are computed on `CAST(gpa AS DECIMAL(4,1))`.
With doubles, course 105's average, which is exactly 3.5, comes out as `3.4999999999999996`,
so student 48 (GPA 3.5, courses 103 and 105) would wrongly count as above average. With
DECIMAL the result is 49 students and student 48 is not included. For this data the 49
students are the same as those with GPA > 3.5.

### 4.4

The 4.2.2 summary is written with `format("mongodb")` to `university.course_stats` using
`courseId` as `_id`, then read back with the connector and compared to the original with
`EXCEPT` in both directions (0 differing rows). If the MongoDB write fails, the program writes
`course_stats.parquet` instead and reads that back.

## Assignment checklist

| Requirement | Where | Result |
|---|---|---|
| 1.1 Parse CSV (unquoted CourseIDs with commas), proper types | `ImportStudents.scala` `parseLine`, `toDocument`; Part 1 above | `results/import_students.txt` (first parsed document) |
| 1.2.1 Single `insertMany`, safe to re-run | `ImportStudents.scala` (drops collection first) | `results/import_students.txt` (`Inserted: 100`) |
| 1.2.2 Multikey index on `courseIds`, index on `gpa` | `ImportStudents.scala` | `results/import_students.txt` (index list) |
| 1.2.3 Save `students.json`, one document per line | `ImportStudents.scala` `toJsonLine` | `data/students.json` |
| 1.2.4 Print inserted count and `listIndexes()` | `ImportStudents.scala` | `results/import_students.txt` |
| Part 2.1 Query students by course | `QueryByCourse.scala` | `results/query_by_course_103.txt` |
| Part 2.2 Course ID as user input, validated integer | `QueryByCourse.scala` (`askCourseId`, `--course`) | `results/validation.txt` |
| Part 2.3 ID, full name, GPA sorted by last name, then count | `QueryByCourse.scala` | `results/query_by_course_103.txt` (36) |
| Part 2.4 `explain()` and IXSCAN check | `QueryByCourse.scala` `findStages`; Part 2 and 3 above | `results/query_by_course_103.txt` (IXSCAN on `courseIds_1`) |
| Part 3.1 Query GPA strictly greater than a value | `QueryByGpa.scala` | `results/query_by_gpa_3.5.txt` |
| Part 3.2 GPA as user input, validated 0.0-4.0 | `QueryByGpa.scala` (`askMinGpa`, `--gpa`), `Common.scala` `parseGpa` | `results/validation.txt` |
| Part 3.3 Sorted by GPA descending, then count | `QueryByGpa.scala` | `results/query_by_gpa_3.5.txt` (49) |
| Part 3.4 Combined course and GPA query | `QueryByGpa.scala` (`--course` with `--gpa`) | `results/query_by_course_103_gpa_3.5.txt` (9) |
| Part 4 setup: SparkSession, load (Option B), `printSchema()`, type check, `students` view | `SparkAnalysis.scala`; Loading above | `results/spark_analysis.txt` (start) |
| 4.1.1 Course 103 with `array_contains`, matches Part 2 | `SparkAnalysis.scala` | `results/spark_analysis.txt` (36, `matches Part 2: True`) |
| 4.1.2 GPA > 3.5 sorted descending, matches Part 3 | `SparkAnalysis.scala` | `results/spark_analysis.txt` (49, `matches Part 3: True`) |
| 4.2.1 `explode(courseIds)` as `enrollments` view | `SparkAnalysis.scala` | `results/spark_analysis.txt` (218 rows) |
| 4.2.2 Per course: count, avg (2 decimals), min, max | `SparkAnalysis.scala` | `results/spark_analysis.txt` |
| 4.2.3 Students with 1, 2, 3 courses (`size`) | `SparkAnalysis.scala` | `results/spark_analysis.txt` (8 / 66 / 26) |
| 4.3.1 Count and avg GPA by birth year | `SparkAnalysis.scala` | `results/spark_analysis.txt` |
| 4.3.2 Top 3 per course with DENSE_RANK, ties explained | `SparkAnalysis.scala`; Ties above | `results/spark_analysis.txt` (94 rows) |
| 4.3.3 Pairs with same first and last name, both IDs and dates | `SparkAnalysis.scala`; 4.3.3 above | `results/spark_analysis.txt` (7 pairs) |
| 4.3.4 GPA above the average of every course taken | `SparkAnalysis.scala`; 4.3.4 above | `results/spark_analysis.txt` (49, student 48 excluded) |
| 4.4 Save course summary with the connector, read back | `SparkAnalysis.scala`; 4.4 above | `results/spark_analysis.txt` (`differing_rows` 0) |

## MongoDB vs Spark

- MongoDB is suited to fast lookups by key or field. With the `courseIds_1` and `gpa_1`
  indexes, queries read only matching documents instead of scanning the collection.
- Spark is suited to work over the whole dataset: exploding arrays, joins (including
  self-joins), group-by aggregations and window functions such as DENSE_RANK. It does not use
  indexes; it scans the data in parallel.
- Both returned the same results for the shared queries (36 and 49). A common pattern is to
  serve lookups from MongoDB and run analysis in Spark, writing summaries back to MongoDB as
  in 4.4.
