package ru.bigdata

trait getVariables {
  // Интерфейс обработки входных значений

  def getValue(nameIn: String): String = {

    if (nameIn.equalsIgnoreCase("loadPart")) "loadPart"
    else if (nameIn.equalsIgnoreCase("srcSchema")) "srcSchema"
    else if (nameIn.equalsIgnoreCase("srcTable")) "srcTable"
    else if (nameIn.equalsIgnoreCase("stgSchema")) "stgSchema"
    else if (nameIn.equalsIgnoreCase("stgTable")) "stgTable"
    else if (nameIn.equalsIgnoreCase("tgtSchema")) "tgtSchema"
    else if (nameIn.equalsIgnoreCase("tgtTable")) "tgtTable"
    else if (nameIn.equalsIgnoreCase("stgDir")) "stgDir"
    else if (nameIn.equalsIgnoreCase("tgtDir")) "tgtDir"
    else if (nameIn.equalsIgnoreCase("partColumnSrc")) "partColumnSrc"
    else if(nameIn.equalsIgnoreCase("loadingId")) "loadingId"
    else null
  }

}
