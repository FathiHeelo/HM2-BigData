import org.bson.Document

import scala.jdk.CollectionConverters._

/** Part 3: find students with GPA strictly greater than a value, optionally limited to one course. */
object QueryByGpa {
  private val Program = "QueryByGpa"
  private val Usage = "QueryByGpa [--gpa GPA] [--course COURSE]"

  private def askCourseId(): Option[Int] = {
    val text = Cli.prompt("Course ID (Enter to skip): ").trim
    if (text.isEmpty) None
    else Cli.parseCourseId(text) match {
      case some @ Some(_) => some
      case None =>
        println("Course ID must be an integer.")
        askCourseId()
    }
  }

  private def askMinGpa(): Option[Double] = {
    val text = Cli.prompt("GPA greater than (Enter to skip): ").trim
    if (text.isEmpty) None
    else Cli.parseGpa(text) match {
      case Right(v) => Some(v)
      case Left(message) =>
        println(message)
        askMinGpa()
    }
  }

  def main(args: Array[String]): Unit = {
    val opts = Cli.parseArgs(Program, Usage, args, Set("--gpa", "--course"))

    // Any argument given -> non-interactive; otherwise prompt
    val (courseId, minGpa) =
      if (opts.nonEmpty) {
        val course = opts.get("--course").map(t => t.toIntOption.getOrElse(
          Cli.argError(Program, Usage, s"argument --course: invalid int value: '$t'")))
        val gpa = opts.get("--gpa").map(t => Cli.parseGpa(t).fold(
          msg => Cli.argError(Program, Usage, s"argument --gpa: $msg"), identity))
        (course, gpa)
      } else {
        val course = askCourseId()
        (course, askMinGpa())
      }

    if (courseId.isEmpty && minGpa.isEmpty) {
      println("Please enter at least one condition.")
      sys.exit(0)
    }

    val query = new Document()
    courseId.foreach(id => query.append("courseIds", Int.box(id)))
    minGpa.foreach(g => query.append("gpa", new Document("$gt", Double.box(g))))
    println(s"Query: ${query.toJson}")

    val client = Mongo.client()
    try {
      val sortOrder = new Document("gpa", -1).append("lastName", 1)
      val results = Mongo.students(client).find(query).sort(sortOrder)
        .into(new java.util.ArrayList[Document]()).asScala
      results.foreach { s =>
        println(s"${s.get("_id")} ${s.getString("firstName")} ${s.getString("lastName")} ${s.getDouble("gpa")}")
      }
      println(s"Matches: ${results.size}")
    } finally client.close()
  }
}
