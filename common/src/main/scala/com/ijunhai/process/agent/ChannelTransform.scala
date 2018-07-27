package com.ijunhai.process.agent

/**
  * 渠道数据去关联字段，并作汇率转换操作
  */
object ChannelTransform {
  val error = "error"
  val FORMAT_STR = "yyyy-MM-dd HH:mm:ss"
  val HWGameMap = Map("hw_csgj" -> "100000000", "hw_hwdzz" -> "100000001", "hw_djcz" -> "100000002",
    "hw_csgjhwb" -> "100000003", "hw_lzdl" -> "100000004", "hw_csgjhwbfoyo" -> "100000005",
    "hw_lzdlhwb" -> "100000006", "hw_tw_lzdl" -> "100000007", "hw_tw_lzdl3" -> "100000008",
    "hw_tw_lzdl4" -> "100000009", "hw_jt_wyfx" -> "100000010", "hw_web_pay" -> "100000011",
    "hw_tw_lzdl5" -> "100000012", "hw_tw_android_yjqy" -> "100000013", "hw_tw_ios_yjqy" -> "100000014",
    "hw_sgqj" -> "100000015", "hw_tw_ios_sgqj" -> "100000016", "hw_wzsy" -> "100000017",
    "hw_hw_android_HWwzsy" -> "100000018", "hw_hw_tw_android_HW_jyqy" -> "100000019", "hw_csssj" -> "100000020",
    "hw_XJZZ" -> "100000021", "hw_xjzz_standalone" -> "100000022", "hw_hw_tw_google_xjzz" -> "100000023",
    "hw_hw_tw_standalone_xjzz" -> "100000024", "hw_hw_tw_standalone_sgqj_test" -> "100000025",
    "hw_hw_tw_google" -> "100000026", "hw_hw_tw_google_sgqj" -> "100000027", "hw_hw_tw_standalone_sgqj" -> "100000028",
    "hw_hw_tw_ios_xjzz" -> "100000029")


}
