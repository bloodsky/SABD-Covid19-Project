package utils

import java.text.SimpleDateFormat
import java.util.{Calendar, GregorianCalendar}

class DaySummaryDoses {
  private val dFormat = new SimpleDateFormat("yyyy-MM-dd")
  private val myCal: Calendar = new GregorianCalendar
  private var meseSomministrazione: Int = 0
  private var TotVaccini: Int = 0
  private var areaName: String = " "

  def setDate(x : String): Unit = {
    myCal.setTime(dFormat.parse(x))
    meseSomministrazione = myCal.get(Calendar.MONTH)+1
  }

  def getDateMonth: Int = {
    meseSomministrazione
  }

  def setTotVaccini(x : Int): Unit = {
    TotVaccini = x
  }

  def getTotVaccini: Int = {
    TotVaccini
  }

  def setAreaName(x : String): Unit = {
    areaName = x
  }

  def getAreaName: String = {
    areaName
  }

}
