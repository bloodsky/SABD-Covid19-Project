package utils

import java.text.SimpleDateFormat
import java.util.{Calendar, GregorianCalendar}

class DayLatestDoses {
  private val dFormat = new SimpleDateFormat("yyyy-MM-dd")
  private val myCal: Calendar = new GregorianCalendar
  private var meseSomministrazione: Int = 0
  private var fullData: String = " "
  private var numeroDonne: Int = 0
  private var fascia: String = " "
  private var areaName: String = " "

  def setFascia(x : String): Unit = {
    fascia = x
  }

  def getFascia: String = {
    fascia
  }

  def setDate(x : String): Unit = {
    fullData = x
    myCal.setTime(dFormat.parse(x))
    meseSomministrazione = myCal.get(Calendar.MONTH)+1
  }

  def getDateMonth: Int = {
    meseSomministrazione
  }

  def getDay: Int = {
    fullData.split("-")(2).toInt
  }

  def setNumDonne(x : Int): Unit = {
    numeroDonne = x
  }

  def getNumDonne: Int = {
    numeroDonne
  }

  def setAreaName(x : String): Unit = {
    areaName = x
  }

  def getAreaName: String = {
    areaName
  }

}
