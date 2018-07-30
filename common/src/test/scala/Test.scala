import java.util.Date

import com.ijunhai.common.TimeUtil

object Test {




  def main(args: Array[String]): Unit = {
    val todaySplit = TimeUtil.time2DateString("yyyy-MM-dd", new Date().getTime, TimeUtil.MILLISECOND).split("-")
    val today = todaySplit(1) + todaySplit(2)

    println(today)
  }

}
