
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.Map

object LoginFailWithCep {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val loginStream = env.fromCollection(List(
      LoginEvent(1, "192.168.0.1", "fail", 1558430842),
      LoginEvent(1, "192.168.0.2", "fail", 1558430843),
      LoginEvent(1, "192.168.0.3", "fail", 1558430844),
      LoginEvent(2, "192.168.0.3", "fail", 1558430845),
      LoginEvent(2, "192.168.10.10", "success", 1558430845)
    ))
      .assignAscendingTimestamps(_.eventTime)
      .keyBy(_.userId)
    //定义匹配模式
    val loginFailPattern: Pattern[LoginEvent, LoginEvent] = Pattern.begin[LoginEvent]("begin").where(_.eventType == "fail")
      .next("next").where(_.eventType == "fail")
      .within(Time.seconds(2))

    val patternStream: PatternStream[LoginEvent] = CEP.pattern(loginStream, loginFailPattern)
    import scala.collection.Map
    patternStream.select(
      (pattern: Map[String, Iterable[LoginEvent]]) => {
        val begin: LoginEvent = pattern.getOrElse("begin", null).iterator.next()
        val next: LoginEvent = pattern.getOrElse("next", null).iterator.next()
        (next.userId, begin.ip, next.ip, next.eventType)
      }
    ).print()

    env.execute("Login Fail Detect Job")


  }
}

case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)