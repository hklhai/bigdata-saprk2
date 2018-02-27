package com.hxqh.bigdata

import org.apache.spark.sql.SparkSession

/**
  * Created by Ocean lin on 2018/2/26.
  *
  * @author Ocean lin
  */
object UserActiveDegreeAnalyze {

  case class UserActionLog(logId: Long, userId: Long, actionTime: String, actionType: Long, purchaseMoney: Double)

  case class UserActionLogVO(logId: Long, userId: Long, actionValue: Long)

  case class UserActionLogWithPurchaseMoneyVO(logId: Long, userId: Long, purchaseMoney: Double)


  def main(args: Array[String]): Unit = {
    val startDate = "2016-09-01"
    val endDate = "2016-11-01"


    val spark = SparkSession.builder().appName("UserActiveDegreeAnalyze").master("local")
      .config("spark.sql.warehouse.dir", "D:\\spark")
      .getOrCreate()

    // 导入spark的隐式转换
    import spark.implicits._
    // 导入spark sql的functions
    import org.apache.spark.sql.functions._

    // 获取两份数据集
    val userBaseInfo = spark.read.json("D:\\spark\\user_base_info.json")
    val userActionLog = spark.read.json("D:\\spark\\user_action_log.json")

    //    userActionLog
    //      // 第一步：过滤数据，找到指定时间范围内的数据
    //      .filter("actionTime >= '" + startDate + "' and actionTime <= '" + endDate + "' and actionType = 0")
    //      // 第二步：关联对应的用户基本信息数据
    //      .join(userBaseInfo, userActionLog("userId") === userBaseInfo("userId"))
    //      // 第三部：进行分组，按照userid和username
    //      .groupBy(userBaseInfo("userId"), userBaseInfo("username"))
    //      // 第四步：进行聚合
    //      .agg(count(userActionLog("logId")).alias("actionCount"))
    //      // 第五步：进行排序
    //      .sort($"actionCount".desc)
    //      // 第六步：抽取指定的条数
    //      .limit(10)
    //      // 第七步：展示结果，因为监简化了，所以说不会再写入mysql
    //      .show()

    // 第二个功能：获取指定时间范围内购买金额最多的10个用户
    // 对金额进行处理的函数讲解
    // feature，技术点的讲解：嵌套函数的使用
    userActionLog
      .filter("actionTime >= '" + startDate + "' and actionTime <= '" + endDate + "' and actionType = 1")
      .join(userBaseInfo, userActionLog("userId") === userBaseInfo("userId"))
      .groupBy(userBaseInfo("userId"), userBaseInfo("username"))
      .agg(round(sum(userActionLog("purchaseMoney")), 2).alias("totalPurchaseMoney"))
      .sort($"totalPurchaseMoney" desc)
      .limit(10)
      .show()

    // 第三个功能：统计最近一个周期相对上一个周期访问次数增长最多的10个用户

    // 设定一个周期是1个月
    // 有1个用户，叫张三，那么张三在9月份这个周期内总共访问了100次，张三在10月份这个周期内总共访问了200次
    // 张三这个用户在最近一个周期相比上一个周期，访问次数增长了100次
    // 每个用户都可以计算出这样一个值
    // 获取在最近两个周期内，访问次数增长最多的10个用户

    // 周期，是可以由用户在web界面上填写的，java web系统会写入mysql，可以去获取本次执行的周期
    // 假定1个月，2016-10-01~2016-10-31，上一个周期就是2016-09-01~2016-09-30
    val userActionLogInFirstPeriod = userActionLog.as[UserActionLog]
      .filter("actionTime >= '2016-10-01' and actionTime <= '2016-10-31' and actionType = 0")
      .map { userActionLogEntry => UserActionLogVO(userActionLogEntry.logId, userActionLogEntry.userId, 1) }

    val userActionLogInSecondPeriod = userActionLog.as[UserActionLog]
      .filter("actionTime >= '2016-01-01' and actionTime <= '2016-09-30' and actionType = 0")
      .map { userActionLogEntry => UserActionLogVO(userActionLogEntry.logId, userActionLogEntry.userId, -1) }

    val userActionLogDS = userActionLogInFirstPeriod.union(userActionLogInSecondPeriod)

    userActionLogDS
      .join(userBaseInfo, userActionLogDS("userId") === userBaseInfo("userId"))
      .groupBy(userBaseInfo("userId"), userBaseInfo("username"))
      .agg(sum(userActionLogDS("actionValue")).alias("actionIncr"))
      .sort($"actionIncr".desc)
      .limit(10)
      .show()


    // 统计最近一个周期相比上一个周期购买金额增长最多的10个用户
    val userActionLogWithPurchaseMoneyInFirstPeriod = userActionLog.as[UserActionLog]
      .filter("actionTime >= '2016-10-01' and actionTime <= '2016-10-31' and actionType = 1")
      .map { userActionLogEntry => UserActionLogWithPurchaseMoneyVO(userActionLogEntry.logId, userActionLogEntry.userId, userActionLogEntry.purchaseMoney) }

    val userActionLogWithPurchaseMoneyInSecondPeriod = userActionLog.as[UserActionLog]
      .filter("actionTime >= '2016-09-01' and actionTime <= '2016-09-30' and actionType = 1")
      .map { userActionLogEntry => UserActionLogWithPurchaseMoneyVO(userActionLogEntry.logId, userActionLogEntry.userId, -userActionLogEntry.purchaseMoney) }

    val userActionLogWithPurchaseMoneyDS = userActionLogWithPurchaseMoneyInFirstPeriod.union(userActionLogWithPurchaseMoneyInSecondPeriod)

    userActionLogWithPurchaseMoneyDS
      .join(userBaseInfo, userActionLogWithPurchaseMoneyDS("userId") === userBaseInfo("userId"))
      .groupBy(userBaseInfo("userId"), userBaseInfo("username"))
      .agg(round(sum(userActionLogWithPurchaseMoneyDS("purchaseMoney")), 2).alias("purchaseMoneyIncr"))
      .sort($"purchaseMoneyIncr".desc)
      .limit(10)
      .show()

    // 统计指定注册时间范围内头7天访问次数最高的10个用户
    // 举例，用户通过web界面指定的注册范围是2016-10-01~2016-10-31
    userActionLog
      .join(userBaseInfo, userActionLog("userId") === userBaseInfo("userId"))
      .filter(userBaseInfo("registTime") >= "2016-10-01"
        && userBaseInfo("registTime") <= "2016-10-31"
        && userActionLog("actionTime") >= userBaseInfo("registTime")
        && userActionLog("actionTime") <= date_add(userBaseInfo("registTime"), 7) // 注册头7天
        && userActionLog("actionType") === 0)
      .groupBy(userBaseInfo("userId"), userBaseInfo("username"))
      .agg(count(userActionLog("logId")).alias("actionCount"))
      .sort($"actionCount".desc)
      .limit(10)
      .show()


    // 指定注册时间范围内头7天购买金额最高的10个用户
    userActionLog
      .join(userBaseInfo, userActionLog("userId") === userBaseInfo("userId"))
      .filter(userBaseInfo("registTime") >= "2016-10-01"
        && userBaseInfo("registTime") <= "2016-10-31"
        && userActionLog("actionTime") >= userBaseInfo("registTime")
        && userActionLog("actionTime") <= date_add(userBaseInfo("registTime"), 7)
        && userActionLog("actionType") === 1)
      .groupBy(userBaseInfo("userId"), userBaseInfo("username"))
      .agg(round(sum(userActionLog("purchaseMoney")),2).alias("purchaseMoneyTotal"))
      .sort($"purchaseMoneyTotal".desc)
      .limit(10)
      .show()


  }

}
