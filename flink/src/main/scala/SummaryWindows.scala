import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util
import _root_.util.Protocol.CommitSummary

class SummaryWindows extends ProcessWindowFunction[(String, String, Int, String, Int),
  CommitSummary, String, TimeWindow]  {

  override def process(key: String,
                       context: Context,
                       elements: Iterable[(String, String, Int, String, Int)],
                       out: util.Collector[CommitSummary]): Unit = {

    val amounts = elements.map(c => (c._1, c._2, c._3, Set(c._4), c._5, List(c._4)))
      .reduce((a,b) => (a._1, a._2, a._3+b._3, a._4++b._4, (a._5 + b._5).toInt, a._6++b._6))

    val topCommitter = amounts._6.sortWith(_<_).groupBy(identity).mapValues(_.size).reduce((a,b) =>
      if(a._2 < b._2) b
      else if(a._2 == b._2) (a._1 + "," + b._1, a._2)
      else a
    )

    val output = CommitSummary.apply(amounts._1, amounts._2, amounts._3, amounts._4.size, amounts._5, topCommitter._1)
    out.collect(output)
  }

}