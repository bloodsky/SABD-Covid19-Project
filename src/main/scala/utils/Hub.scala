package utils

class Hub {
  private var areaName: String = ""

  def setAreaName(x : String): Unit = {
    areaName = x
  }

  def getAreaName: String = {
    areaName
  }
}
