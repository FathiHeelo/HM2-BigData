import java.util.concurrent.TimeUnit

import com.mongodb.{ConnectionString, MongoClientSettings}
import com.mongodb.client.{MongoClient, MongoClients, MongoCollection}
import org.bson.Document

import scala.io.StdIn

object Mongo {
  val Uri: String = sys.env.getOrElse("MONGO_URI", "mongodb://localhost:27017")

  def client(serverSelectionSeconds: Long = 30): MongoClient =
    MongoClients.create(
      MongoClientSettings.builder()
        .applyConnectionString(new ConnectionString(Uri))
        .applyToClusterSettings { b => b.serverSelectionTimeout(serverSelectionSeconds, TimeUnit.SECONDS); () }
        .build())

  def students(client: MongoClient): MongoCollection[Document] =
    client.getDatabase("university").getCollection("students")
}

object Cli {
  /** Booleans printed as True/False, matching the reference results. */
  def bool(b: Boolean): String = if (b) "True" else "False"

  /** Print a prompt and read a line; exit if stdin is closed. */
  def prompt(text: String): String = {
    print(text)
    Console.flush()
    val line = StdIn.readLine()
    if (line == null) {
      println("\nNo input.")
      sys.exit(1)
    }
    line
  }

  def parseCourseId(text: String): Option[Int] = text.trim.toIntOption

  private val Number = """[+-]?(\d+(\.\d*)?|\.\d+)([eE][+-]?\d+)?""".r

  /** A GPA in [0.0, 4.0], or an error message. NaN and Infinity are rejected. */
  def parseGpa(text: String): Either[String, Double] =
    text.trim match {
      case s @ Number(_*) =>
        val v = s.toDouble
        if (v >= 0.0 && v <= 4.0) Right(v) else Left("GPA must be between 0.0 and 4.0.")
      case _ => Left("GPA must be a number.")
    }

  /** Fail like a command-line parser: message on stderr, exit code 2. */
  def argError(program: String, usage: String, message: String): Nothing = {
    System.err.println(s"usage: $usage")
    System.err.println(s"$program: error: $message")
    sys.exit(2)
  }

  /** Parse --name value / --name=value pairs for the given option names. */
  def parseArgs(program: String, usage: String, args: Array[String], names: Set[String]): Map[String, String] = {
    def loop(rest: List[String], acc: Map[String, String]): Map[String, String] = rest match {
      case Nil => acc
      case opt :: tail if opt.startsWith("--") && opt.contains("=") =>
        val (name, value) = opt.splitAt(opt.indexOf('='))
        if (!names(name)) argError(program, usage, s"unrecognized arguments: $opt")
        loop(tail, acc + (name -> value.drop(1)))
      case opt :: value :: tail if names(opt) => loop(tail, acc + (opt -> value))
      case opt :: Nil if names(opt) => argError(program, usage, s"argument $opt: expected one argument")
      case other :: _ => argError(program, usage, s"unrecognized arguments: $other")
    }
    loop(args.toList, Map.empty)
  }
}
