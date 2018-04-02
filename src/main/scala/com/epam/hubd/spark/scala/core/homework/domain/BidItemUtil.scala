package com.epam.hubd.spark.scala.core.homework.domain

/**
  * Util domain class for bid records, allows price to be a non convertible to Double value
  * to ease the process of filtering such records
  */
case class BidItemUtil(motelId: String, bidDate: String, loSa: String, price: String) {

  override def toString: String = s"$motelId,$bidDate,$loSa,$price"
}
