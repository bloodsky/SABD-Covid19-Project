import org.apache.spark.{SparkConf, SparkContext}
import utils.Helper

import java.time.Month

object VaccinazioniCovid19 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("VaccinazioniCovid19")

    // Entry point
    val sc = new SparkContext(conf)
    jobQueryOne(sc)
    jobQueryTwo(sc)
    sc.stop()
  }

  def jobQueryOne(sc: SparkContext): Unit = {

    Helper.log.info(Helper.q1+" started!")

    val file1 = "s3n://file1-query1/2021/06/04/10/stream-query1-file1-1-2021-06-04-10-47-13-d3f9066e-bf21-4b8d-9817-dc67a50b0cd1"
    val file2 = "s3n://file1-query2/2021/06/04/10/stream-query1-file2-1-2021-06-04-10-47-13-f12fc210-6a76-4bd7-82ba-764f698fc54a"
    // HUBS
    val rddFromFileOne = sc.textFile(file1)
    // DOSES
    val rddFromFileTwo = sc.textFile(file2)

    // Jump header
    val RDDOne = rddFromFileOne.mapPartitionsWithIndex{(idx,iter) => if (idx == 0) iter.drop(1) else iter}
    val RDDTwo = rddFromFileTwo.mapPartitionsWithIndex{(idx,iter) => if (idx == 0) iter.drop(1) else iter}

    // (areaName, #hubs)
    val rddFileOne = RDDOne.map{line =>
      val hub = Helper.hubCsvParser(line)
      (hub.getAreaName,1)
    }.reduceByKey(_+_)
    Helper.log.info(Helper.q1+" shuffle -> new stage created!")

    // ((AreaName, Mese), TotVacciniPerMese)
    val rddFileTwo = RDDTwo
      .map{line =>
        val daySummaryDoses = Helper.dosesCsvParser(line)
        ((daySummaryDoses.getAreaName,daySummaryDoses.getDateMonth),daySummaryDoses.getTotVaccini)
      }
      .filter(q => q._1._2 != Helper.DECEMBER)
      .reduceByKey(_+_)
    Helper.log.info(Helper.q1+" shuffle -> new stage created!")

    Helper.log.info(Helper.q1+" joining RDD!")
    rddFileTwo
      //(Regione,(  Mese , TotVacciniPerMese / GiorniMeseSolare)
      .map(k => (k._1._1,(k._1._2,(k._2/Helper.getDayOfMonth(k._1._2-1)).toDouble)))
      .join(rddFileOne)
      //        ((       Mese      ,regione),MediaVaccini/#hub)
      .map(t => ((Month.of(t._2._1._1),t._1),(t._2._1._2/t._2._2)))
      .sortBy(x => x._1)
      .saveAsTextFile("s3n://file1-query1/"+java.time.LocalDate.now.toString+"_Q1")

    Helper.log.info(Helper.q1+" ended!")
  }

  def jobQueryTwo(sc: SparkContext): Unit = {

    Helper.log.info(Helper.q2+" started")

    val file = "s3n://file2-query1/somministrazioni-vaccini-latest.csv"
    val pFile = sc.textFile(file)

    // Jump header
    val rddFromFile = pFile.mapPartitionsWithIndex{(idx,iter) => if (idx == 0) iter.drop(1) else iter}

    val rddOne = rddFromFile.map{line =>
      val dayLatestDoses = Helper.dosesLatestCsvParser(line)
      ((dayLatestDoses.getDateMonth,dayLatestDoses.getFascia,dayLatestDoses.getAreaName)
        ,(dayLatestDoses.getNumDonne,dayLatestDoses.getDay))
    }.filter(k => k._1._1 != Helper.DECEMBER).groupBy(t => t._1).sortBy(x => x._1).map { t =>
      //(Vaccini, Giorno).groupBy(Giorno).((k,sum))
      val dayDoses = t._2.map(_._2).groupBy(_._2).mapValues(_.map(_._1).sum)
      //(Mese, Fascia, Regione, (Vaccini, Giorno))
      (t._1._1,t._1._2,t._1._3,dayDoses)
    }
    Helper.log.info(Helper.q2+" shuffle -> new stage created!")

    Helper.log.info(Helper.q2+" regression -> fitting ...")
    // Regression & result
    rddOne.map { line =>
      val days = Helper.getDayOfMonth(line._1)
      val doses = line._4
      ((line._1+1,line._2),line._3,Helper.RegressionModel(days,doses))
    }.groupBy(t => t._1).map{k =>
      k._2.toList.sortBy(_._3)(Ordering[Double].reverse).take(5)
    }.flatMap(line => line).sortBy(_._1).map(line => ("1° "+Month.of(line._1._1),line._1._2,line._2,line._3))
      .saveAsTextFile("s3n://file2-query1/"+java.time.LocalDate.now.toString+"_Q2")

    Helper.log.info(Helper.q2+" ended!")
  }
}