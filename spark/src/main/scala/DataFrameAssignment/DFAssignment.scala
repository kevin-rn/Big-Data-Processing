package DataFrameAssignment

import java.sql.Timestamp

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.functions._

/**
  * Note read the comments carefully, as they describe the expected result and may contain hints in how
  * to tackle the exercises. Note that the data that is given in the examples in the comments does
  * reflect the format of the data, but not the result the graders expect (unless stated otherwise).
  */
object DFAssignment {

  /**
    * In this exercise we want to know all the commit SHA's from a list of commit committers. We require these to be
    * in order according to timestamp.
    *
    * | committer      | sha                                      | timestamp            |
    * |----------------|------------------------------------------|----------------------|
    * | Harbar-Inbound | 1d8e15a834a2157fe7af04421c42a893e8a1f23a | 2019-03-10T15:24:16Z |
    * | ...            | ...                                      | ...                  |
    *
    * Hint: try to work out the individual stages of the exercises, which makes it easier to track bugs, and figure out
    * how Spark Dataframes and their operations work. You can also use the `printSchema()` function and `show()`
    * function to take a look at the structure and contents of the Dataframes.
    *
    * @param commits Commit Dataframe, created from the data_raw.json file.
    * @param authors Sequence of String representing the authors from which we want to know their respective commit
    *                SHA's.
    * @return DataFrame of commits from the requested authors, including the commit SHA and the according timestamp.
    */
  def assignment_1(commits: DataFrame, authors: Seq[String]): DataFrame = {
    val c = commits.select(commits("commit.committer.name").as("committer"), commits("sha"),
      commits("commit.committer.date").as("timestamp"))
    c.filter(c("committer").isInCollection(authors))
      .orderBy(c("timestamp"))
  }

  /**
    * In order to generate weekly dashboards for all projects, we need the data to be partitioned by weeks. As projects
    * can span multiple years in the data set, care must be taken to partition by not only weeks but also by years.
    * The returned DataFrame that is expected is in the following format:
    *
    * | repository | week             | year | count   |
    * |------------|------------------|------|---------|
    * | Maven      | 41               | 2019 | 21      |
    * | .....      | ..               | .... | ..      |
    *
    * @param commits Commit Dataframe, created from the data_raw.json file.
    * @return Dataframe containing 4 columns, Repository name, week number, year and the number fo commits for that
    *         week.
    */
  def assignment_2(commits: DataFrame): DataFrame = {
    val repo = functions.split(functions.split(commits("url"), "/repos/").apply(1),
      "/").apply(1).name("repository")
    val year = functions.year(commits("commit.committer.date"))
    val week = functions.weekofyear(commits("commit.committer.date"))
    commits.withColumn("repository", repo).withColumn("week", week)
      .withColumn("year", year).groupBy("repository", "week", "year").count()
  }

  /**
    * A developer is interested in the age of commits in seconds, although this is something that can always be
    * calculated during runtime, this would require us to pass a Timestamp along with the computation. Therefore we
    * require you to append the inputted DataFrame with an age column of each commit in `seconds`.
    *
    * Hint: Look into SQL functions in for Spark SQL.
    *
    * Expected Dataframe (column) example that is expected:
    *
    * | age    |
    * |--------|
    * | 1231   |
    * | 20     |
    * | ...    |
    *
    * @param commits Commit Dataframe, created from the data_raw.json file.
    * @return the inputted DataFrame appended with an age column.
    */
  def assignment_3(commits: DataFrame, snapShotTimestamp: Timestamp): DataFrame = {
    val age = -functions.unix_timestamp(functions.date_format(commits("commit.committer.date"),"yyyy-MM-dd HH:mm:ss")) + snapShotTimestamp.getTime/1000
    commits.withColumn("age", age)
  }


  /**
    * To perform analysis on commit behavior the intermediate time of commits is needed. We require that the DataFrame
    * that is put in is appended with an extra column that expresses the number of days there are between the current
    * commit and the previous commit of the user, independent of the branch or repository.
    * If no commit exists before a commit regard the time difference in days should be zero. Make sure to return the
    * commits in chronological order.
    *
    * Hint: Look into Spark sql's Window to have more expressive power in custom aggregations
    *
    * Expected Dataframe example:
    *
    * | $oid                     	| name   	| date                     	| time_diff 	|
    * |--------------------------	|--------	|--------------------------	|-----------	|
    * | 5ce6929e6480fd0d91d3106a 	| GitHub 	| 2019-01-27T07:09:13.000Z 	| 0         	|
    * | 5ce693156480fd0d5edbd708 	| GitHub 	| 2019-03-04T15:21:52.000Z 	| 36        	|
    * | 5ce691b06480fd0fe0972350 	| GitHub 	| 2019-03-06T13:55:25.000Z 	| 2         	|
    * | ...                      	| ...    	| ...                      	| ...       	|
    *
    * @param commits Commit DataFrame, see commit.json and data_raw.json for the structure of the file, or run
    *                `println(commits.schema)`.
    * @return DataFrame with column expressing seconds since last commit.
    */
  def assignment_4(commits: DataFrame, authorName: String): DataFrame = {
    val c = commits.filter(commits("commit.author.name").equalTo(authorName))
      .orderBy(commits("commit.author.date"))
    val timeSec = c("commit.author.date")
    val diff = Window.orderBy(c("commit.author.name"))
    val prev = functions.lag(timeSec, 1).over(diff)
    c.withColumn("time_diff", functions.when(functions
      .isnull(functions.datediff(timeSec, prev)), 0)
      .otherwise(functions.datediff(timeSec, prev)))
  }


  /**
    * To get a bit of insight in the spark SQL, and its aggregation functions, you will have to implement a function
    * that returns a DataFrame containing a column `day` (int) and a column `commits_per_day`, based on the commits
    * commit date. Monday would be 0, Tuesday 1, etc.
    *
    * Expected Dataframe example:
    *
    * | day | commits_per_day|
    * |-----|----------------|
    * | 0   | 32             |
    * | ... | ...            |
    *
    * @param commits Commit DataFrame, see commit.json and data_raw.json for the structure of the file, or run
    *                `println(commits.schema)`.
    * @return DataFrame containing a `day` column and a `commits_per_day` representing a count of the total number of
    *         commits that that were ever made on that week day.
    */
  def assignment_5(commits: DataFrame): DataFrame = {
    val day = functions.dayofweek(commits("commit.committer.date"))
    val days = commits.withColumn("day", day).groupBy("day").count()
    days.select(days("day"), days("count").as("commits_per_day")).orderBy("day")
  }


  /**
    * Commits can be uploaded on different days, we want to get insight in difference in commit time of the author and
    * the committer. Append the given dataframe with a column expressing the number of seconds in difference between
    * the two events in the commit data.
    *
    * Expected Dataframe (column) example:
    *
    * | commit_time_diff |
    * |------------------|
    * | 1022             |
    * | 0                |
    * | ...              |
    *
    * @param commits Commit DataFrame, see commit.json and data_raw.json for the structure of the file, or run
    *                `println(commits.schema)`.
    * @return original Dataframe appended with a column `commit_time_diff` containing the number of seconds time
    *         difference between authorizing and committing.
    */
  def assignment_6(commits: DataFrame): DataFrame = {
    val diff = functions.unix_timestamp(
      functions.date_format(commits("commit.committer.date"),
        "yyyy-MM-dd HH:mm:ss")) - functions.unix_timestamp(
      functions.date_format(commits("commit.author.date"),
        "yyyy-MM-dd HH:mm:ss"))
    commits.withColumn("commit_time_diff", diff)
  }

  /**
    * Using dataframes find all the commit SHA's from which a branch was created, including the number of
    * branches that were made. Only take the SHA's into account if they are also contained in the RDD.
    * Note that the returned Dataframe should not contain any sha's of which no new branches were made.
    *
    * Expected Dataframe example:
    *
    * | sha                                      | times_parent |
    * |------------------------------------------|--------------|
    * | 3438abd8e0222f37934ba62b2130c3933b067678 | 2            |
    * | ...                                      | ...          |
    *
    * @param commits Commit DataFrame, see commit.json and data_raw.json for the structure of the file, or run
    *                `println(commits.schema)`.
    * @return DataFrame containing the SHAs of which a new branch was made.
    */
  def assignment_7(commits: DataFrame): DataFrame = {
    val parents = commits
      .withColumn("parent_sha", commits("sha"))

    val branch = commits.withColumn("parents", explode
    (commits("parents.sha")))

    val joined = parents.join(branch, parents("parent_sha").equalTo(branch("parents")), "inner")
      .select("parent_sha")

    val result = joined.groupBy("parent_sha").count()
    result.filter(result("count") > 1).select(result("parent_sha").as("sha"), result("count").as
    ("times_parent"))
  }




  /**
    * Find of commits from which a fork was created in the given commit DataFrame. We are interested in the name of
    * repositories, the parent and the subsequent fork, including the name of the repository owner. The SHA from which
    * the fork was created (parent_sha) as well as the first SHA that occurs in the forked branch (child_sha).
    *
    * Expected Dataframe example:
    *
    * | repo_name            | child_repo_name     | parent_sha           | child_sha            |
    * |----------------------|---------------------|----------------------|----------------------|
    * | ElucidataInc/ElMaven | saifulbkhan/ElMaven | 37d38cb21ab342b17... | 6a3dbead35c10add6... |
    * | hyho942/hecoco       | Sub2n/hecoco        | ebd077a028bd2169d... | b47db8a9df414e28b... |
    * | ...                  | ...                 | ...                  | ...                  |
    *
    * Note that this example is based on _real_ data, so you can verify the functionality of your solution, which might
    * help during debugging your solution.
    *
    * @param commits Commit DataFrame, see commit.json and data_raw.json for the structure of the file, or run
    *                `println(commits.schema)`.
    * @return DataFrame containing the SHAs of which a new fork was created.
    */
  def assignment_8(commits: DataFrame): DataFrame = {
    val parent = commits.withColumn("parent_sha", commits("sha"))
      .withColumn("repo_name", concat(split(commits("url"), "/").apply(4), lit("/"), split(commits("url"), "/").apply(5)))

    val forked = commits.withColumn("child_sha", commits( "sha")).withColumn("parents", explode(commits("parents")))
      .withColumn("date", commits( "commit.committer.date"))
      .withColumn("child_repo_name", concat(split(commits("url"), "/").apply(4), lit( literal= "/"), split(commits("url"), "/").apply(5) ))

    val result = parent.join(forked, parent("parent_sha").equalTo(forked("parents.sha")), "inner")
      .select("repo_name","child_repo_name", "parent_sha", "child_sha", "date")
      .orderBy(asc("date"), asc("repo_name"))

    result.filter(result("repo_name").notEqual(result("child_repo_name"))).select("repo_name", "child_repo_name", "parent_sha", "child_sha")
  }

}
