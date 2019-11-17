package com.omnipresent.support

import java.util.UUID

object IdGenerator {

  def getRandomID(prefix: String = ""): String = s"${prefix}_${UUID.randomUUID().getMostSignificantBits}"

}
