package com.omnipresent.support

import com.omnipresent.system.Master.CreateQueue

import net.liftweb.json._

object ViewSupport {

  implicit val formats = DefaultFormats

  def parseData(queueData: Set[String]): String = {
    var queueHTMLString = ""
    var count = 0
    for (queue <- queueData) {
      if (count == 0)
        queueHTMLString += """<div class="row"><div class="docModuleGrid">""".stripMargin

      val queueJson = parse(queue);
      queueHTMLString += parseToHTMLSegment(queueJson.extract[CreateQueue])

      if (count == 2) {
        count = -1
        queueHTMLString += "</div></div>"
      }
      count += 1
    }
    queueHTMLString
  }

  def parseToHTMLSegment(details: CreateQueue): String = {
    """<div class="box single-column">
              		<h1>Cola: """ + details.name + """</h1>
              		<span class="underLine"></span>
                     	<h3>Productores:</h3> """ + details.producers + """
                     	<h3>Consumidores:</h3> """ + details.workers + """
                     	<h3>Intervalo:</h3> """ + details.jobInterval + """
                     	<h3>Tipo Cola:</h3> """ + details.spreadType + """
       	  </div>""".stripMargin
  }

}
