import java.lang
import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object TrafficAnalysis {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val textDstream: DataStream[String] = env.readTextFile("D:\\workspace_idea\\UserBehaviorAnalysis\\HotIteamsAnalysis\\src\\main\\resources\\apachetest.log")
    textDstream.map(line => {
      val lineArray: Array[String] = line.split(" ")
      //定义时间转换模板，将时间转换为时间戳
      val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
      val timeStamp: Long = simpleDateFormat.parse(lineArray(3)).getTime
      ApacheLogEvent(lineArray(0), lineArray(1), timeStamp, lineArray(5), lineArray(6))
    }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(10)) {
      override def extractTimestamp(element: ApacheLogEvent): Long = {
        element.eventTime
      }
    }).filter(_.method == "GET")
      .keyBy(_.url)
      .timeWindow(Time.minutes(1), Time.seconds(5))
      //.window(SlidingEventTimeWindows.of(Time.minutes(10),Time.seconds(5)))
      .aggregate(new CountAgg(), new WindowResultFunction())
      .keyBy(_.windowEnd)
      .process(new TopNHotUrls(5))
      .print()

    env.execute("Network Traffic Analysis Job")
  }
}

case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)

case class UrlViewCount(url: String, windowEnd: Long, count: Long)

class CountAgg extends AggregateFunction[ApacheLogEvent, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

class WindowResultFunction extends WindowFunction[Long, UrlViewCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
    val url: String = key
    val count: Long = input.iterator.next()
    out.collect(UrlViewCount(url, window.getEnd, count))
  }
}

class TopNHotUrls(topSize: Int) extends KeyedProcessFunction[Long, UrlViewCount, String] {

  lazy val listState: ListState[UrlViewCount] = getRuntimeContext.getListState(new ListStateDescriptor("topN", classOf[UrlViewCount]))

  override def processElement(input: UrlViewCount, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context, out: Collector[String]): Unit = {
    listState.add(input)
    //设定定时器
    val l: Long = ctx.timerService().currentWatermark()
    ctx.timerService().registerEventTimeTimer(l)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    val listBuffer: ListBuffer[UrlViewCount] = ListBuffer()
    import scala.collection.JavaConversions._
    for (item <- listState.get) {
      listBuffer += item
    }
    listState.clear()
    val resultList: List[UrlViewCount] = listBuffer.toList.sortBy(_.count)(Ordering.Long.reverse).take(topSize)
    val result = new StringBuilder
    result.append("====================================\n")
    result.append("时间").append(new Timestamp(timestamp-10*1000)).append("\n")
    for (i <- resultList.indices) {
      val current: UrlViewCount = resultList(i)
      result.append("No").append(i + 1).append(":")
        .append("  URL=").append(current.url)
        .append("  流量=").append(current.count).append("\n")
    }
    result.append("====================================\n\n")
    Thread.sleep(500)
    out.collect(result.toString())

  }

}