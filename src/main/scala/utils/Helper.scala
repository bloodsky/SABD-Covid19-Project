package utils

import org.apache.log4j.{Level, Logger}

import scala.math.pow

object Helper {

  val DECEMBER: Int = 12
  val splitRegex = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"
  val daysInAMonth: Array[Int] = Array(31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)

  val log: Logger = Logger.getLogger("VaccinazioniCovid19")
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val q1: String = "QUERY#1:"
  val q2: String = "QUERY#2:"

  def hubCsvParser(line: String): Hub = {
    val hub = new Hub()
    val features = line.split(splitRegex)
    hub.setAreaName(features(6))
    hub
  }

  def dosesCsvParser(line: String): DaySummaryDoses = {
    val daySummaryDoses = new DaySummaryDoses()
    val features = line.split(splitRegex)
    daySummaryDoses.setDate(features(0))
    daySummaryDoses.setTotVaccini(features(2).toInt)
    daySummaryDoses.setAreaName(features(10))
    daySummaryDoses
  }

  def dosesLatestCsvParser(line: String): DayLatestDoses = {
    val dayLatestDoses = new DayLatestDoses()
    val features = line.split(splitRegex)
    dayLatestDoses.setDate(features(0))
    dayLatestDoses.setFascia(features(3))
    dayLatestDoses.setNumDonne(features(5).toInt)
    dayLatestDoses.setAreaName(features(11))
    dayLatestDoses
  }

  def getDayOfMonth(month: Int): Int = {
    daysInAMonth(month)
  }

  def RegressionModel(days: Int, yMap: Map[Int, Int]): Double = {
    val x = (1 to days).toArray
    val y = Array.fill(days)(0)

    for (i <- y.indices) {
      if (yMap.contains(i + 1))
        y(i) = yMap(i + 1)
    }

    // Fast mean
    val x_mean = x.foldLeft((0.0, 1))((acc, i) => ((acc._1 + (i - acc._1) / acc._2), acc._2 + 1))._1
    val y_mean = y.foldLeft((0.0, 1))((acc, i) => ((acc._1 + (i - acc._1) / acc._2), acc._2 + 1))._1

    var num = 0.0
    var den = 0.0
    for (i <- x.indices) {
      num = num + (x(i) - x_mean) * (y(i) - y_mean)
      den = den + pow((x(i) - x_mean), 2)
    }

    val bOne = num / den
    val bZero = y_mean - (bOne * x_mean)
    BigDecimal(bZero + bOne).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
  }
}
