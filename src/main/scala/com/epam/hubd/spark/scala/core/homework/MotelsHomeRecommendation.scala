package com.epam.hubd.spark.scala.core.homework

import com.epam.hubd.spark.scala.core.homework.domain.{BidError, BidItem, BidItemUtil, EnrichedItem}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

import scala.collection.Map
import scala.util.Try

object MotelsHomeRecommendation {

  val ERRONEOUS_DIR: String = "erroneous"
  val AGGREGATED_DIR: String = "aggregated"
  val ERROR_TAG: String = "ERROR_"

  def main(args: Array[String]): Unit = {
    require(args.length == 4, "Provide parameters in this order: bidsPath, motelsPath, exchangeRatesPath, outputBasePath")

    val bidsPath = args(0)
    val motelsPath = args(1)
    val exchangeRatesPath = args(2)
    val outputBasePath = args(3)

    val sc = new SparkContext(new SparkConf().setAppName("motels-home-recommendation"))
    //    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("motels-home-recommendation"))

    processData(sc, bidsPath, motelsPath, exchangeRatesPath, outputBasePath)

    sc.stop()
  }

  def processData(sc: SparkContext, bidsPath: String, motelsPath: String, exchangeRatesPath: String, outputBasePath: String) = {

    /**
      * Task 1:
      * Read the bid data from the provided file.
      */
    val rawBids: RDD[List[String]] = getRawBids(sc, bidsPath)

    /**
      * Task 1:
      * Collect the errors and save the result.
      * Hint: Use the BideError case class
      */
    val erroneousRecords: RDD[String] = getErroneousRecords(rawBids)
    erroneousRecords.saveAsTextFile(s"$outputBasePath/$ERRONEOUS_DIR")

    /**
      * Task 2:
      * Read the exchange rate information.
      * Hint: You will need a mapping between a date/time and rate
      */
    val exchangeRates: Map[String, Double] = getExchangeRates(sc, exchangeRatesPath)

    /**
      * Task 3:
      * Transform the rawBids and use the BidItem case class.
      * - Convert USD to EUR. The result should be rounded to 3 decimal precision.
      * - Convert dates to proper format - use formats in Constants util class
      * - Get rid of records where there is no price for a Losa or the price is not a proper decimal number
      */
    val bids: RDD[BidItem] = getBids(rawBids, exchangeRates)

    /**
      * Task 4:
      * Load motels data.
      * Hint: You will need the motels name for enrichment and you will use the id for join
      */
    val motels: RDD[(String, String)] = getMotels(sc, motelsPath)

    /**
      * Task5:
      * Join the bids with motel names and utilize EnrichedItem case class.
      * Hint: When determining the maximum if the same price appears twice then keep the first entity you found
      * with the given price.
      */
    val enriched: RDD[EnrichedItem] = getEnriched(bids, motels)
    enriched.saveAsTextFile(s"$outputBasePath/$AGGREGATED_DIR")
  }

  def getRawBids(sc: SparkContext, bidsPath: String): RDD[List[String]] = {
    sc.textFile(bidsPath)
      .map(line => line.split(Constants.DELIMITER).toList)
  }

  def getErroneousRecords(rawBids: RDD[List[String]]): RDD[String] = {
    rawBids
      .filter(rawBid => rawBid(2).startsWith(ERROR_TAG))
      .map(bid => (BidError(bid(1), bid(2)), 1L))
      .reduceByKey(_ + _)
      .map(bidErrorAndCount => bidErrorAndCount._1.toString() + Constants.DELIMITER + bidErrorAndCount._2)
  }

  def getExchangeRates(sc: SparkContext, exchangeRatesPath: String): Map[String, Double] = {
    sc.textFile(exchangeRatesPath)
      .map(line => line.split(Constants.DELIMITER))
      .map(exchangeRateRecord => (exchangeRateRecord(0), exchangeRateRecord(3).toDouble))
      .collectAsMap()
  }

  def getBids(rawBids: RDD[List[String]], exchangeRates: Map[String, Double]): RDD[BidItem] = {
    val targetLoSaIndexes: Seq[Int] =
      Constants.TARGET_LOSAS
        .map(targetLoSa => Constants.BIDS_HEADER.indexOf(targetLoSa))

    rawBids
      .filter(rawBid => !rawBid(2).startsWith(ERROR_TAG))
      .flatMap(bid => {
        targetLoSaIndexes
          .map(
            index => BidItemUtil(
              bid.head,
              bid(1),
              Constants.BIDS_HEADER(index),
              bid(index)
            )
          )
      })
      .filter(utilBid => isDouble(utilBid.price))
      .map(
        utilBid => BidItem(
          utilBid.motelId,
          DateTime.parse(utilBid.bidDate, Constants.INPUT_DATE_FORMAT).toString(Constants.OUTPUT_DATE_FORMAT),
          utilBid.loSa,
          "%.3f".format(utilBid.price.toDouble * exchangeRates(utilBid.bidDate)).toDouble
        )
      )
  }

  def getMotels(sc: SparkContext, motelsPath: String): RDD[(String, String)] = {
    sc.textFile(motelsPath)
      .map(line => line.split(Constants.DELIMITER))
      .map(motelRecord => (motelRecord(0), motelRecord(1)))
  }

  def getEnriched(bids: RDD[BidItem], motels: RDD[(String, String)]): RDD[EnrichedItem] = {
    val mappedBids = bids.keyBy(_.motelId)
    val mappedMotels = motels.keyBy(_._1)

    mappedBids
      .join(mappedMotels)
      .map(
        // mapping join result of type
        // (motelId: String, (bidItem: BidItem, motel: (String, String)))
        // to key (motelId, bidDate) and value EnrichedItem for further reducing
        joinedBidItem =>
          (
            (joinedBidItem._2._1.motelId, joinedBidItem._2._1.bidDate),
            EnrichedItem(
              joinedBidItem._2._1.motelId,
              joinedBidItem._2._2._2,
              joinedBidItem._2._1.bidDate,
              joinedBidItem._2._1.loSa,
              joinedBidItem._2._1.price
            )
          )
      )
      .reduceByKey((x, y) => if (x.price >= y.price) x else y)
      .map(enrichedItemWithKey => enrichedItemWithKey._2)
  }

  def isDouble(s: String): Boolean = Try(s.toDouble).isSuccess
}
