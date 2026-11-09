import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, ZoneOffset}
import java.util.Date

import com.mongodb.client.model.Indexes
import org.bson.Document

import scala.jdk.CollectionConverters._

/** Part 1: load students.csv into MongoDB (university.students) and write students.json. */
object ImportStudents {
  final case class Student(id: Int, firstName: String, lastName: String,
                           dateOfBirth: LocalDate, courseIds: Seq[Int], gpa: Double)

  private val IsoUtc = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'").withZone(ZoneOffset.UTC)

  // CourseIDs is unquoted and contains commas, e.g. 1,Alice,Johnson,1998-03-15,[101,102,103],3.8
  // so take the first 4 fields and the last one; everything in between is the course list.
  def parseLine(line: String): Student = {
    val parts = line.trim.split(",", -1)
    val courseText = parts.slice(4, parts.length - 1).mkString(",")
    val courseIds = courseText.stripPrefix("[").stripSuffix("]").split(",").map(_.trim.toInt).toSeq
    Student(parts(0).toInt, parts(1), parts(2), LocalDate.parse(parts(3)), courseIds, parts.last.toDouble)
  }

  def toDocument(s: Student): Document =
    new Document("_id", Int.box(s.id))
      .append("firstName", s.firstName)
      .append("lastName", s.lastName)
      .append("dateOfBirth", Date.from(s.dateOfBirth.atStartOfDay(ZoneOffset.UTC).toInstant))
      .append("courseIds", s.courseIds.map(Int.box).asJava)
      .append("gpa", Double.box(s.gpa))

  private def jsonString(s: String): String = {
    val escaped = s.flatMap {
      case '"'  => "\\\""
      case '\\' => "\\\\"
      case c if c < ' ' || c > '~' => f"\\u${c.toInt}%04x"
      case c => c.toString
    }
    "\"" + escaped + "\""
  }

  // One JSON document per line; dateOfBirth as an ISO string
  def toJsonLine(s: Student): String = {
    val dob = IsoUtc.format(s.dateOfBirth.atStartOfDay(ZoneOffset.UTC).toInstant)
    s"""{"_id": ${s.id}, "firstName": ${jsonString(s.firstName)}, "lastName": ${jsonString(s.lastName)}, """ +
      s""""dateOfBirth": "$dob", "courseIds": [${s.courseIds.mkString(", ")}], "gpa": ${s.gpa}}"""
  }

  def main(args: Array[String]): Unit = {
    val lines = Files.readAllLines(Paths.get("data", "students.csv"), StandardCharsets.UTF_8).asScala.toSeq
    val students = lines.drop(1).filter(_.trim.nonEmpty).map(parseLine) // drop header
    val docs = students.map(toDocument)

    println(s"Parsed: ${docs.size}")
    println(docs.head.toJson)

    Files.write(Paths.get("data", "students.json"),
      students.map(toJsonLine(_) + "\n").mkString.getBytes(StandardCharsets.UTF_8))
    println("Saved students.json")

    val client = Mongo.client()
    try {
      val db = client.getDatabase("university")
      db.getCollection("students").drop() // makes the program re-runnable
      val col = Mongo.students(client)

      val result = col.insertMany(docs.asJava)
      println(s"Inserted: ${result.getInsertedIds.size}")

      col.createIndex(Indexes.ascending("courseIds")) // multikey (array field)
      col.createIndex(Indexes.ascending("gpa"))

      println("Indexes:")
      col.listIndexes().asScala.foreach(idx => println("   " + idx.toJson))
    } finally client.close()
  }
}
