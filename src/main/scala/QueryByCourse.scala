import org.bson.Document

import scala.jdk.CollectionConverters._

/** Part 2: find students enrolled in a course, sorted by last name, and check the query plan. */
object QueryByCourse {
  private val Program = "QueryByCourse"
  private val Usage = "QueryByCourse [--course COURSE]"

  private def askCourseId(): Int =
    Cli.parseCourseId(Cli.prompt("Enter course ID: ")) match {
      case Some(id) => id
      case None =>
        println("Course ID must be an integer.")
        askCourseId()
    }

  /** Collect every stage with the given name from a (nested) explain plan. */
  def findStages(plan: Any, stageName: String): Seq[Document] = plan match {
    case d: Document =>
      val own = if (d.get("stage") == stageName) Seq(d) else Seq.empty
      own ++ d.values().asScala.toSeq.flatMap(findStages(_, stageName))
    case l: java.util.List[_] => l.asScala.toSeq.flatMap(findStages(_, stageName))
    case _ => Seq.empty
  }

  def main(args: Array[String]): Unit = {
    val opts = Cli.parseArgs(Program, Usage, args, Set("--course"))
    val courseId = opts.get("--course") match {
      case Some(text) => text.toIntOption.getOrElse(
        Cli.argError(Program, Usage, s"argument --course: invalid int value: '$text'"))
      case None => askCourseId()
    }

    val client = Mongo.client()
    try {
      val students = Mongo.students(client)
      val query = new Document("courseIds", Int.box(courseId))
      val sortOrder = new Document("lastName", 1).append("firstName", 1)
      println(s"Query: ${query.toJson} sort: ${sortOrder.toJson}")

      val results = students.find(query).sort(sortOrder).into(new java.util.ArrayList[Document]()).asScala
      results.foreach { s =>
        println(s"${s.get("_id")} ${s.getString("firstName")} ${s.getString("lastName")} ${s.getDouble("gpa")}")
      }
      println(s"Matches: ${results.size}")

      val plan = students.find(query).sort(sortOrder).explain()
      val winningPlan = plan.get("queryPlanner", classOf[Document]).get("winningPlan", classOf[Document])
      println(s"Winning plan: ${winningPlan.toJson}")

      val ixscans = findStages(winningPlan, "IXSCAN")
      val indexNames = ixscans.map(_.getString("indexName"))
      println(s"IXSCAN used? ${Cli.bool(ixscans.nonEmpty)} indexes: ${indexNames.mkString("[", ", ", "]")}")
      println(s"IXSCAN on courseIds_1? ${Cli.bool(indexNames.contains("courseIds_1"))}")
    } finally client.close()
  }
}
