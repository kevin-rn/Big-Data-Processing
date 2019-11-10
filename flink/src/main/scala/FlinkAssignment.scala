import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.cep.functions.PatternProcessFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import util.Protocol.{Commit, CommitGeo, CommitSummary, File}
import util.{CommitGeoParser, CommitParser}
import java.util

import org.apache.flink.cep.PatternSelectFunction

/** Do NOT rename this class, otherwise autograding will fail. **/
object FlinkAssignment {

  val env = StreamExecutionEnvironment.getExecutionEnvironment

  def main(args: Array[String]): Unit = {

    /**
     * Setups the streaming environment including loading and parsing of the datasets.
     *
     * DO NOT TOUCH!
     */
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // Read and parses commit stream.
    val commitStream =
      env
        .readTextFile("data/flink_commits.json")
        .map(new CommitParser)

    // Read and parses commit geo stream.
    val commitGeoStream =
      env
        .readTextFile("data/flink_commits_geo.json")
        .map(new CommitGeoParser)

    /** Use the space below to print and test your questions. */
    //    dummy_question(commitStream).print()
    //    question_one(commitStream).print()
    //    question_two(commitStream).print()
    //    question_three(commitStream).print()
    //    question_four(commitStream).print()
    //    question_five(commitStream).print()
    //    question_six(commitStream).print()
//    question_seven(commitStream).print()
    //    question_eight(commitStream, commitGeoStream).print()
        question_nine(commitStream).print()

    /** Start the streaming environment. **/
    env.execute()
  }

  /** Dummy question which maps each commits to its SHA. */
  def dummy_question(input: DataStream[Commit]): DataStream[String] = {
    input.map(_.sha)
  }

  /**
   * Write a Flink application which outputs the sha of commits with at least 20 additions.
   * Output format: sha
   */
  def question_one(input: DataStream[Commit]): DataStream[String] = {
    input.filter(c => c.stats.get.additions > 19).map(c => c.sha)
  }

  /**
   * Write a Flink application which outputs the names of the files with more than 30 deletions.
   * Output format:  fileName
   */
  def question_two(input: DataStream[Commit]): DataStream[String] = {
    input.flatMap(c => {
      val files = c.files.filter(c => c.deletions > 30)
      files.map(f => f.filename.get)
    })
  }

  /**
   * Count the occurrences of Java and Scala files. I.e. files ending with either .scala or .java.
   * Output format: (fileName, #occurences)
   */
  def question_three(input: DataStream[Commit]): DataStream[(String, Int)] = {
    input.flatMap(c => {
      val files = c.files.filter(f => f.filename.get.endsWith(".java") || f.filename.get.endsWith(".scala"))

      files.map(f => {
        val filename = if (f.filename.get.endsWith(".java")) "java" else "scala"
        (filename, 1)
      })
    }).keyBy(0).reduce((a, b) => (a._1, a._2 + b._2))
  }

  /**
   * Count the total amount of changes for each file status (e.g. modified, removed or added) for the following extensions: .js and .py.
   * Output format: (extension, status, count)
   */
  def question_four(
                     input: DataStream[Commit]): DataStream[(String, String, Int)] = {
    input.flatMap(c => {
      val files = c.files.filter(f => f.filename.get.endsWith(".js") || f.filename.get.endsWith(".py"))
      files.map(f => {
        val filename = if (f.filename.get.endsWith(".js")) "js" else "py"
        ((filename, f.status.get), f.changes)
      })

    }).keyBy(0).reduce((a, b) => ((a._1._1, a._1._2), a._2 + b._2)).map(c => (c._1._1, c._1._2, c._2))
  }

  /**
   * For every day output the amount of commits. Include the timestamp in the following format dd-MM-yyyy; e.g. (26-06-2019, 4) meaning on the 26th of June 2019 there were 4 commits.
   * Make use of a non-keyed window.
   * Output format: (date, count)
   */
  def question_five(input: DataStream[Commit]): DataStream[(String, Int)] = {
    input.assignAscendingTimestamps(_.commit.committer.date.getTime)
      .map(c => {
        (new SimpleDateFormat("dd-MM-yyyy").format(c.commit.committer.date), 1)
      }).timeWindowAll(Time.days(1)).reduce((a, b) => (a._1, a._2 + b._2))
  }

  /**
   * Consider two types of commits; small commits and large commits whereas small: 0 <= x <= 20 and large: x > 20 where x = total amount of changes.
   * Compute every 12 hours the amount of small and large commits in the last 48 hours.
   * Output format: (type, count)
   */
  def question_six(input: DataStream[Commit]): DataStream[(String, Int)] = {
    input.assignAscendingTimestamps(_.commit.committer.date.getTime).map(c => {
      (if (c.stats.get.total > 20) "large" else "small", 1)
    }).keyBy(c => c._1).timeWindow(Time.hours(48), Time.hours(12)).reduce((a, b) => (a._1, a._2 + b._2))
  }


  /**
   * For each repository compute a daily commit summary and output the summaries with more than 20 commits and at most 2 unique committers. The CommitSummary case class is already defined.
   *
   * The fields of this case class:
   *
   * repo: name of the repo.
   * date: use the start of the window in format "dd-MM-yyyy".
   * amountOfCommits: the number of commits on that day for that repository.
   * amountOfCommitters: the amount of unique committers contributing to the repository.
   * totalChanges: the sum of total changes in all commits.
   * topCommitter: the top committer of that day i.e. with the most commits. Note: if there are multiple top committers; create a comma separated string sorted alphabetically e.g. `georgios,jeroen,wouter`
   *
   * Hint: Write your own ProcessWindowFunction.
   * Output format: CommitSummary
   */
  def question_seven(
                      commitStream: DataStream[Commit]): DataStream[CommitSummary] = {
    val res = commitStream.assignAscendingTimestamps(_.commit.committer.date.getTime).map(c => {
      val date = new SimpleDateFormat("dd-MM-yyyy").format(c.commit.committer.date)
      (c.url.split('/')(4) + "/" + c.url.split('/')(5), date, 1, c.commit.committer.name, c.stats.get.total)
    }).keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.days(1)))
      .process(new SummaryWindows())
    res.filter(c => c.amountOfCommits > 20 && c.amountOfCommitters <= 2)
  }

  /**
   * For this exercise there is another dataset containing CommitGeo events. A CommitGeo event stores the sha of a commit, a date and the continent it was produced in.
   * You can assume that for every commit there is a CommitGeo event arriving within a timeframe of 1 hour before and 30 minutes after the commit.
   * Get the weekly amount of changes for the java files (.java extension) per continent.
   *
   * Hint: Find the correct join to use!
   * Output format: (continent, amount)
   */
  def question_eight(
                      commitStream: DataStream[Commit],
                      geoStream: DataStream[CommitGeo]): DataStream[(String, Int)] = {

    commitStream.assignAscendingTimestamps(_.commit.committer.date.getTime).map(c => {
      val javaChanges = c.files.filter(f => f.filename.get.endsWith(".java")).foldLeft(0)((a, b)
      => a + b.changes)
      (c.sha, javaChanges)
    }).filter(_._2 > 0)
      .keyBy(_._1)
      .intervalJoin(geoStream.assignAscendingTimestamps(_.createdAt.getTime).keyBy(_.sha))
      .between(Time.minutes(-60), Time.minutes(30))
      .process(new ProcessJoinFunction[(String, Int), CommitGeo, (String, Int)] {
        override def processElement(in1: (String, Int), in2: CommitGeo,
                                    context: ProcessJoinFunction[(String, Int), CommitGeo,
                                      (String, Int)]#Context,
                                    collector: Collector[(String, Int)]): Unit = {
          val out = (in2.continent, in1._2)
          collector.collect(out)
        }
      }).keyBy(_._1).timeWindow(Time.days(7))
      .reduce((a, b) => (a._1, a._2 + b._2))
  }

  /**
   * Find all files that were added and removed within one day. Output as (repository, filename).
   *
   * Hint: Use the Complex Event Processing library (CEP).
   * Output format: (repository, filename)
   */
  def question_nine(inputStream: DataStream[Commit]): DataStream[(String, String)] = {

    val files = inputStream.assignAscendingTimestamps(_.commit.committer.date.getTime)
      .flatMap(c => {
        c.files.map(f => (c.url.split('/')(4) + "/" + c.url.split('/')(5), f.filename, f.status))
      }).filter(f => f._2.isDefined && f._3.isDefined)
      .map(f => (f._1, f._2.get, f._3.get)).keyBy(f => (f._1, f._2))

    val pattern = Pattern.begin[(String, String, String)]("add").where(_._3 == "added")
      .followedBy("remove").where(f => {
      f._3 == "removed"
    }).within(Time.days(1))

    val patternStream = CEP.pattern(files, pattern)

    patternStream.select(
      new PatternSelectFunction[(String, String, String), (String, String)]() {

        override def select(map: util.Map[String, util.List[(String, String, String)]]): (String, String) = {
          (map.get("add").get(0)._1, map.get("add").get(0)._2)
        }
      }s
    )
  }
}