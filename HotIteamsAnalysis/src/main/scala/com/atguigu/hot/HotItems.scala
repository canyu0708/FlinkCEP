
import java.lang
import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.{AscendingTimestampExtractor, BoundedOutOfOrdernessTimestampExtractor}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object HotItems {
  def main(args: Array[String]): Unit = {

    //创建一个flink环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //设定Time类型为EventTime
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //设置全局并发为1
    env.setParallelism(1)
    val textDstream: DataStream[String] = env.readTextFile("D:\\workspace_idea\\UserBehaviorAnalysis\\HotIteamsAnalysis\\src\\main\\resources\\UserBehavior.csv")
    //    textDstream.print()

    //TODO 将属性封装进一个样例类中，并且按照timestamp为时间戳，水位为1s,filter过滤behavior为pv的日志，按照商品分类，设置开窗
    textDstream.map(line => {
      val linearray: Array[String] = line.split(",")
      UserBehavior(linearray(0).toLong, linearray(1).toLong, linearray(2).toInt, linearray(3), linearray(4).toLong)
    }). /*assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[UserBehavior](Time.seconds(1))  {
      override def extractTimestamp(element: UserBehavior): Long = return element.timestamp*1000
    }) */ assignAscendingTimestamps(_.timestamp * 1000)
      .filter(_.behavior == "pv")
      .keyBy("itemId")
      .timeWindow(Time.minutes(60), Time.minutes(5))
      .aggregate(new CountAgg(), new WindowResultFunction())
      //TODO 按照windowEnd分组
      .keyBy("windowEnd")
      .process(new TopNHotItems(3))
      .print()
  }
}

case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

//TODO 自定义的窗口内的预处理函数
class CountAgg extends AggregateFunction[UserBehavior, Long, Long] {

  //创建累加器
  override def createAccumulator(): Long = 0L

  //从累加器中获取值
  override def add(in: UserBehavior, acc: Long): Long = acc + 1

  //获取累加器中的结果
  override def getResult(acc: Long): Long = acc

  //合并累加器的值
  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

//TODO 自定义实现window Function,输入为Long类型，输出为ItemViewCount类型,Tuple为定义的key，类型必须为元组，TimeWindow
class WindowResultFunction extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    //TODO key为单个iteamid，将KEY定义为Tuple1
    val iteamId: Long = key.asInstanceOf[Tuple1[Long]].f0
    val count: Long = input.iterator.next()
    out.collect(ItemViewCount(iteamId, window.getEnd, count))
  }
}

//TODO 自定义实现process实现
class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String] {

  //TODO 定义状态liststate（有多种state，list较好用）
  private var itemState: ListState[ItemViewCount] =  getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("iteamState", classOf[ItemViewCount]))
  //  private var state: ValueState[ItemViewCount] = _

//  override def open(parameters: Configuration): Unit = {
//    super.open(parameters)
    // 命名状态变量的名字和类型
//    val itemStateDesc = new ListStateDescriptor[ItemViewCount]("iteamState", classOf[ItemViewCount])
//    itemState = getRuntimeContext.getListState(itemStateDesc)
    //   getRuntimeContext.getState(new ValueStateDescriptor("iteamState", classOf[ItemViewCount]))



  //TODO 对每一个进来的数据就行操作
  override def processElement(input: ItemViewCount, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
    //TODO 将进来的对象放入状态对象中
    itemState.add(input)
    //TODO 注册定时器，触发时间设定为 windowEnd+1，触发时说明window已经收集完当前窗口的所有数据
    ctx.timerService.registerEventTimeTimer(input.windowEnd + 1)
    //水位
    //    ctx.timerService().currentWatermark()
  }

  //TODO 重写一个定时器的触发操作，从state中取出数据，排序
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {

    //TODO 获取所有的商品点击属性，从itemState取出
    val allItems: ListBuffer[ItemViewCount] = ListBuffer()

    //TODO 用到scala的for的语法格式，引入转换包
    import scala.collection.JavaConversions._
    for (item <- itemState.get) {
      allItems += item
    }
    //TODO 清除状态中的数据，释放空间
    itemState.clear()

    //TODO 按照点击量进行排序,并按照从大到小排序，取出前top个
    val resultList: List[ItemViewCount] = allItems.toList.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

    //TODO 将排名数据格式化
    val result: StringBuilder = new StringBuilder
    result.append("====================================\n")
    result.append("时间：").append(new Timestamp(timestamp - 1)).append("\n")

    for (i <- resultList.indices) {
      val currentItem: ItemViewCount = resultList(i)
      //TODO 输出打印的格式 e.g.  No1：  商品ID=12224  浏览量=2413
      result.append("No").append(i + 1).append(":")
        .append("商品ID=").append(currentItem.itemId)
        .append("浏览量=").append(currentItem.count).append("\n")
    }
    result.append("====================================\n\n")
    Thread.sleep(1000)
    out.collect(result.toString())
  }

}
