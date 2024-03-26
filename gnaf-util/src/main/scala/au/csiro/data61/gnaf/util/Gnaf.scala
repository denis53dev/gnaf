package au.csiro.data61.gnaf.util

import spray.json.DefaultJsonProtocol

object Gnaf {
  
  def join(s: Seq[Option[String]], delim: String): Option[String] = {
    val r = s.flatten.filter(_.nonEmpty).mkString(delim)
    if (r.nonEmpty) Some(r) else None
  }
  def d61Num(n: Option[Int]) = n.map(_.toString)
  
  case class PreNumSuf(prefix: Option[String], number: Option[Int], suffix: Option[String]) {
    def toOptStr = join(Seq(prefix, d61Num(number), suffix), "")
  }
  
  case class Street(name: String, typeCode: Option[String], typeName: Option[String], suffixCode: Option[String], suffixName: Option[String])
  case class LocalityVariant(localityName: String)
  case class Location(lat: BigDecimal, lon: BigDecimal)
  case class Address(addressDetailPid: String, addressSiteName: Option[String], buildingName: Option[String],
                     flatTypeCode: Option[String], flatTypeName: Option[String], flat: PreNumSuf,
                     levelTypeCode: Option[String], levelTypeName: Option[String], level: PreNumSuf,
                     numberFirst: PreNumSuf, numberLast: PreNumSuf,
                     street: Option[Street], localityName: String, primaryPostcode: Option[String], stateAbbreviation: String, stateName: String, postcode: Option[String],
                     aliasPrincipal: Option[Char], primarySecondary: Option[Char],
                     location: Option[Location], streetVariant: Seq[Street], localityVariant: Seq[LocalityVariant]) {
        
    def toD61Address = {
      val streetNum = numberFirst.toOptStr.map(f => f + numberLast.toOptStr.map("-" + _).getOrElse(""))
      val seqNoAlias = Seq(
        Seq( addressSiteName, buildingName ), // each inner Seq optionally produces a String in the final Seq
        Seq( flatTypeName, flat.toOptStr ), 
        Seq( levelTypeName, level.toOptStr ),
        Seq( streetNum, street.map(_.name), street.flatMap(_.typeCode), street.flatMap(_.suffixName) ),
        Seq( Some(localityName), Some(stateAbbreviation), postcode )
      )
      val seqWithAlias = seqNoAlias ++
        streetVariant.map(v => Seq( streetNum, Some(v.name), v.typeCode, v.suffixName )) ++
        localityVariant.map(v => Seq( Some(v.localityName), Some(stateAbbreviation), postcode ))
      val d61Address = seqWithAlias.map(inner => join(inner, " ")).flatten
      val seqNoAlias2 = seqNoAlias.map(inner => join(inner, " "))
      val noneCount = (streetNum +: seqNoAlias2).count(_.isEmpty) // count each empty streetNum and inner seq: site/building, flat, level 
      val d61AddressNoAlias = join(seqNoAlias.map(inner => join(inner, " ")), " ").getOrElse("")
      (d61Address, noneCount, d61AddressNoAlias)
    }
  }

  object JsonProtocol extends DefaultJsonProtocol {
    implicit val preNumSufFormat = jsonFormat3(PreNumSuf)
    implicit val streetFormat = jsonFormat5(Street)
    implicit val locVarFormat = jsonFormat1(LocalityVariant)
    implicit val locationFormat = jsonFormat2(Location)
    implicit val addressFormat = jsonFormat22(Address)
  }
  
}