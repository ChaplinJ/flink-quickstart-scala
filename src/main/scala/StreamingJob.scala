import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @Description
  * @Author chaplinj 
  * @Date 2019/11/4 18:23
  * @EMAIL john_cc_zongheng@126.
  */
object StreamingJob {


    def main(args: Array[String]): Unit = {
        //获取执行的环境
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        val text: DataStream[String] = env.socketTextStream("localhost",9999,'\n')

        val windowCounts: DataStream[WordWithCount] = text.flatMap(w => w.split("\\s"))
            .map(w => WordWithCount(w, 1))
            .keyBy("word")
            .timeWindow(Time.seconds(5))
            .sum("count")
        windowCounts.print().setParallelism(1)

        env.execute("socket windows worldCount")

    }

    case class WordWithCount(word:String, count:Long)
}
