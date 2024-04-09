package au.csiro.data61.gnaf.util

import spray.json._

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
                     aliasPrincipal: Option[Char], primarySecondary: Option[Char], geocodeTypeCode: Option[String],
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
    implicit object AddressJsonFormat extends RootJsonFormat[Address] {
      def write(a: Address): JsValue = JsObject(
        "addressDetailPid" -> JsString(a.addressDetailPid),
        "addressSiteName" -> a.addressSiteName.toJson,
        "buildingName" -> a.buildingName.toJson,
        "flatTypeCode" -> a.flatTypeCode.toJson,
        "flatTypeName" -> a.flatTypeName.toJson,
        "flat" -> a.flat.toJson,
        "levelTypeCode" -> a.levelTypeCode.toJson,
        "levelTypeName" -> a.levelTypeName.toJson,
        "level" -> a.level.toJson,
        "numberFirst" -> a.numberFirst.toJson,
        "numberLast" -> a.numberLast.toJson,
        "street" -> a.street.toJson,
        "localityName" -> JsString(a.localityName),
        "primaryPostcode" -> a.primaryPostcode.toJson,
        "stateAbbreviation" -> JsString(a.stateAbbreviation),
        "stateName" -> JsString(a.stateName),
        "postcode" -> a.postcode.toJson,
        "aliasPrincipal" -> a.aliasPrincipal.toJson,
        "primarySecondary" -> a.primarySecondary.toJson,
        "geocodeTypeCode" -> a.geocodeTypeCode.toJson,
        "location" -> a.location.toJson,
        "streetVariant" -> a.streetVariant.toJson,
        "localityVariant" -> a.localityVariant.toJson
      )

      def read(value: JsValue): Address = {
        val obj = value.asJsObject

        Address(
          addressDetailPid = obj.fields("addressDetailPid").convertTo[String],
          addressSiteName = obj.fields.get("addressSiteName").flatMap(_.convertTo[Option[String]]),
          buildingName = obj.fields.get("buildingName").flatMap(_.convertTo[Option[String]]),
          flatTypeCode = obj.fields.get("flatTypeCode").flatMap(_.convertTo[Option[String]]),
          flatTypeName = obj.fields.get("flatTypeName").flatMap(_.convertTo[Option[String]]),
          flat = obj.fields("flat").convertTo[PreNumSuf],
          levelTypeCode = obj.fields.get("levelTypeCode").flatMap(_.convertTo[Option[String]]),
          levelTypeName = obj.fields.get("levelTypeName").flatMap(_.convertTo[Option[String]]),
          level = obj.fields("level").convertTo[PreNumSuf],
          numberFirst = obj.fields("numberFirst").convertTo[PreNumSuf],
          numberLast = obj.fields("numberLast").convertTo[PreNumSuf],
          street = obj.fields.get("street").flatMap(_.convertTo[Option[Street]]),
          localityName = obj.fields("localityName").convertTo[String],
          primaryPostcode = obj.fields.get("primaryPostcode").flatMap(_.convertTo[Option[String]]),
          stateAbbreviation = obj.fields("stateAbbreviation").convertTo[String],
          stateName = obj.fields("stateName").convertTo[String],
          postcode = obj.fields.get("postcode").flatMap(_.convertTo[Option[String]]),
          aliasPrincipal = obj.fields.get("aliasPrincipal").flatMap(_.convertTo[Option[Char]]),
          primarySecondary = obj.fields.get("primarySecondary").flatMap(_.convertTo[Option[Char]]),
          geocodeTypeCode = obj.fields.get("geocodeTypeCode").flatMap(_.convertTo[Option[String]]),
          location = obj.fields.get("location").flatMap(_.convertTo[Option[Location]]),
          streetVariant = obj.fields.get("streetVariant") match {
            case Some(jsValue) => jsValue.convertTo[Seq[Street]]
            case None => Seq.empty[Street] // Provide an empty sequence as default
          },
          localityVariant = obj.fields.get("localityVariant") match {
            case Some(jsValue) => jsValue.convertTo[Seq[LocalityVariant]]
            case None => Seq.empty[LocalityVariant] // Provide an empty sequence as default
          }
        )
      }
    }
  }
  
}