import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api._
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer
// 定义输入的登录事件流
case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)

object LoginFail {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val loginStream = env.fromCollection(List(
      LoginEvent(1, "192.168.0.1", "fail", 1558430842),
      LoginEvent(1, "192.168.0.2", "fail", 1558430843),
      LoginEvent(1, "192.168.0.3", "fail", 1558430844),
      LoginEvent(2, "192.168.10.10", "success", 1558430845)
    )).assignAscendingTimestamps(_.eventTime * 1000)
      .filter(_.eventType == "fail")
      .keyBy(_.userId)
      .process(new MatchFuntion())
      .print()
    env.execute()
  }
}

class MatchFuntion extends KeyedProcessFunction[Long, LoginEvent, LoginEvent] {
  lazy val listState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("loginStatte", classOf[LoginEvent]))

  override def processElement(input: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, LoginEvent]#Context, out: Collector[LoginEvent]): Unit = {
    listState.add(input)
    //注册定时器
    ctx.timerService().registerEventTimeTimer(input.eventTime*1000 + 2 * 1000)

  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, LoginEvent]#OnTimerContext, out: Collector[LoginEvent]): Unit = {
    val allLogins :ListBuffer[LoginEvent] =ListBuffer()
    for( login <- listState.get() ){
      allLogins += login
    }
    listState.clear()

    if(allLogins.length > 1){
      out.collect(allLogins.head)
    }
  }
}