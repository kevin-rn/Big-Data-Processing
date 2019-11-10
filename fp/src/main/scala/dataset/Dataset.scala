package dataset

import java.text.SimpleDateFormat
import java.util.SimpleTimeZone

import dataset.util.Commit.Commit

/**
  * Use your knowledge of functional programming to complete the following functions.
  * You are recommended to use library functions when possible.
  *
  * The data is provided as a list of `Commit`s. This case class can be found in util/Commit.scala.
  * When asked for dates, use the `commit.commit.committer.date` field.
  *
  * This part is worth 40 points.
  */
object Dataset {

    /** Q16 (5p)
      * For the commits that are accompanied with stats data, compute the average of their additions.
      * You can assume a positive amount of usable commits is present in the data.
      * @param input the list of commits to process.
      * @return the average amount of additions in the commits that have stats data.
      */
    def avgAdditions(input: List[Commit]): Int = {
        val additions = input.filter(c => {
            if (c.stats.isEmpty) false
            else true
        }).map(c => {
            c.stats.get.additions
        })
          additions.sum/additions.size
    }


    /** Q17 (8p)
      * Find the hour of day (in 24h notation, UTC time) during which the most javascript (.js) files are changed in commits.
      * The hour 00:00-00:59 is hour 0, 14:00-14:59 is hour 14, etc.
      * Hint: for the time, use `SimpleDateFormat` and `SimpleTimeZone`.
      * @param input list of commits to process.
      * @return the hour and the amount of files changed during this hour.
      */
    def jsTime(input: List[Commit]): (Int, Int) = {
        val times = input.map(c => {
            val files = c.files.count(c => c.filename.get.endsWith(".js"))
            val date = c.commit.committer.date
            val dateFormat = new SimpleDateFormat("k")
            dateFormat.setTimeZone(new SimpleTimeZone(SimpleTimeZone.UTC_TIME, "UTC"))
            val time = dateFormat.format(date)
            (time.toInt, files)
        })
        times.groupBy(_._1).mapValues(_.map(x => x._2).sum).maxBy(_._2)
    }

    /** Q18 (9p)
      * For a given repository, output the name and amount of commits for the person
      * with the most commits to this repository.
      * @param input the list of commits to process.
      * @param repo the repository name to consider.
      * @return the name and amount of commits for the top committer.
      */
    def topCommitter(input: List[Commit], repo: String): (String, Int) = {
        input.filter((c:Commit) => {c.url.contains(repo)}).map(c => {c.commit.author.name}).groupBy(identity).mapValues(_.size).maxBy(_._2)
    }

    /** Q19 (9p)
      * For each repository, output the name and the amount of commits that were made to this repository in 2019 only.
      * Leave out all repositories that had no activity this year.
      * @param input the list of commits to process.
      * @return a map that maps the repo name to the amount of commits.
      *
      * Example output:
      * Map("KosDP1987/students" -> 1, "giahh263/HQWord" -> 2)
      */
    def commitsPerRepo(input: List[Commit]): Map[String, Int] = {
        input.filter((c:Commit) => {
            val date = c.commit.committer.date
            val format = new SimpleDateFormat("y")
            format.setTimeZone(new SimpleTimeZone(SimpleTimeZone.UTC_TIME, "UTC"))
            val time = format.format(date)
            time.equals(date)
        }).map(c => c.url.split("/repos/").apply(1).split("/commits/").apply(0)).groupBy(identity).mapValues(_.size)


    }

    /** Q20 (9p)
      * Derive the 5 file types that appear most frequent in the commit logs.
      * @param input the list of commits to process.
      * @return 5 tuples containing the file extension and frequency of the most frequently appeared file types, ordered descendingly.
      */
    def topFileFormats(input: List[Commit]): List[(String, Int)] = {
        input.flatMap(c => {
            c.files.filter(f => f.filename.nonEmpty).map(f => {
                val fileArray = f.filename.get.split('.')
                fileArray.apply(fileArray.length - 1)
            })
        }).groupBy(identity).mapValues(_.size).toSeq.sortBy(-_._2).slice(0, 5).toList
    }
}
