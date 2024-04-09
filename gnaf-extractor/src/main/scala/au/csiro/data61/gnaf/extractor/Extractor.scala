package au.csiro.data61.gnaf.extractor

import scala.concurrent.{ Await, ExecutionContext, Future }, ExecutionContext.Implicits.global
import scala.concurrent.duration.DurationInt
import scala.language.implicitConversions
import scala.util.{ Failure, Success }

import com.typesafe.config.{ ConfigFactory, ConfigValueFactory }

import au.csiro.data61.gnaf.db.GnafTables
import au.csiro.data61.gnaf.util.Gnaf._
import au.csiro.data61.gnaf.util.Gnaf.JsonProtocol._
import au.csiro.data61.gnaf.util.Util
import resource.managed
import slick.collection.heterogeneous.HNil
import slick.collection.heterogeneous.syntax.::
import spray.json.pimpAny
import scala.sys.SystemProperties

// Organize Imports deletes this, so make it easy to restore ...
// import slick.collection.heterogeneous.syntax.::

object Extractor {
  val log = Util.getLogger(getClass)
  
  val config = ConfigFactory.load
  
  object MyGnafTables extends {
    val profile = Util.getObject[slick.driver.JdbcProfile](config.getString("gnafDb.slickDriver")) // e.g. slick.driver.{H2Driver,PostgresDriver}
  } with GnafTables
  import MyGnafTables._
  import MyGnafTables.profile.api._

  /** result of command line option processing */
  case class CliOption(dburl: String, localityTimeout: Int, allTimeout: Int)
  val defaultCliOption = CliOption(config.getString("gnafDb.url"), config.getInt("gnafDb.localityTimeout"), config.getInt("gnafDb.allTimeout"))

  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[CliOption]("gnaf-extractor") {
      head("gnaf-extractor", "0.x")
      note("Creates JSON from gnaf database to load into a search engine.")
      opt[String]('u', "dburl") action { (x, c) =>
        c.copy(dburl = x)
      } text (s"database URL, default ${defaultCliOption.dburl}")
      opt[Int]('l', "localityTimeout") action { (x, c) =>
        c.copy(localityTimeout = x)
      } text (s"timeout in minutes for all queries for a locality, default ${defaultCliOption.localityTimeout}")
      opt[Int]('a', "allTimeout") action { (x, c) =>
        c.copy(allTimeout = x)
      } text (s"timeout in minutes for all queries, default ${defaultCliOption.allTimeout}")
      help("help") text ("prints this usage text")
    }
    parser.parse(args, defaultCliOption) foreach run
    log.info("complete")
  }

  def run(c: CliOption) = {
    // configure global thread pool
    (new SystemProperties()) ++= Seq(
      ("scala.concurrent.context.minThreads", "4"), 
      ("scala.concurrent.context.numThreads", "4"), 
      ("scala.concurrent.context.maxThreads", "4")
    )
  
    val conf = config.withValue("gnafDb.url", ConfigValueFactory.fromAnyRef(c.dburl)) // CliOption.dburl overrides gnafDb.url
    for (db <- managed(Database.forConfig("gnafDb", conf))) {
      doAll(c)(db)
    }
  }

  val qAddressDetail = {
    def q(localityPid: Rep[String]) = for {
      ((((ad, lta), as), sl), adg) <- AddressDetail joinLeft
        LevelTypeAut on (_.levelTypeCode === _.code) joinLeft  // only 15 rows so keep in memory
        AddressSite on (_._1.addressSitePid === _.addressSitePid) joinLeft // ADDRESS_DETAIL.ADDRESS_SITE_PID is NON NULL, so no need for LEFT JOIN
        StreetLocality on (_._1._1.streetLocalityPid === _.streetLocalityPid) joinLeft
        AddressDefaultGeocode on (_._1._1._1.addressDetailPid === _.addressDetailPid)
      if (ad.localityPid === localityPid && ad.confidence > -1)
    } yield (
      ad,
      lta.map(_.name),
      as.map(_.addressSiteName),
      sl.map(sl => (sl.streetName, sl.streetTypeCode, sl.streetSuffixCode)),
      adg.map(adg => (adg.geocodeTypeCode, adg.latitude, adg.longitude)))
    Compiled(q _)
  }

  val qLocalityAliasName = {
    def q(localityPid: Rep[String]) = for (la <- LocalityAlias if la.localityPid === localityPid) yield la.name
    Compiled(q _)
  }
  def localityVariant(localityPid: String)(implicit db: Database): Future[Seq[LocalityVariant]] =
    db.run(qLocalityAliasName(localityPid).result).map(_.map(name => LocalityVariant(name)))

  val qStreetLocalityAlias = {
    def q(streetLocalityPid: Rep[String]) = for (sla <- StreetLocalityAlias if sla.streetLocalityPid === streetLocalityPid) yield (sla.streetName, sla.streetTypeCode, sla.streetSuffixCode)
    Compiled(q _)
  }

  def streetLocalityAlias(streetLocalityPid: Option[String])(implicit db: Database): Future[Seq[(String, Option[String], Option[String])]] = {
    streetLocalityPid.map { pid =>
      db.run(qStreetLocalityAlias(pid).result)
    }.getOrElse(Future(Seq.empty))
  }

  type FutStrMap = Future[Map[String, String]]

  def doAll(c: CliOption)(implicit db: Database) = {
    // These code -> name mappings are all small enough to keep in memory
    val stateMap: Future[Map[String, (String, String)]] = db.run((for (s <- State) yield s.statePid -> (s.stateAbbreviation, s.stateName)).result).map(_.toMap)
    val flatTypeMap: FutStrMap = db.run((for (f <- FlatTypeAut) yield f.code -> f.name).result).map(_.toMap)
    val streetTypeMap: FutStrMap = db.run((for (s <- StreetTypeAut) yield s.code -> s.name).result).map(_.toMap)
    val streetSuffixMap: FutStrMap = db.run((for (s <- StreetSuffixAut) yield s.code -> s.name).result).map(_.toMap)

    val localities: Future[Seq[(String, String, String, Option[String])]] = db.run((for (loc <- Locality if loc.localityClassCode === 'G') yield (loc.localityPid, loc.localityName, loc.statePid, loc.primaryPostcode)).result)
    val done: Future[Unit] = localities.flatMap { seq =>
      log.info("got all localities")
      val seqFut: Seq[Future[Unit]] = seq.map {
        case (localityPid, localityName, statePid, primaryPostcode) =>
          val locDone = doLocality(localityPid, localityName, statePid, primaryPostcode, stateMap, flatTypeMap, streetTypeMap, streetSuffixMap)
          Await.result(locDone, c.localityTimeout.minute) // without this it runs out of memory before outputting anything!
          locDone
      }
      Future.fold(seqFut)(())((_, _) => ())
    }
    Await.result(done, c.allTimeout.minute)
    log info "all done"
  }

  /*
  When I try to stream all AddressDetail rows, I don't get any rows in a reasonable time (seems to hang but CPU is busy).
  
  http://stackoverflow.com/questions/24787119/how-to-set-h2-to-stream-resultset
  H2 currently does not support server side cursors. However, it buffers large result sets to disk (as a separate file, or as a temporary table). The disadvantage is speed, but it should not be a memory usage problems.
  
  You can set the size of the when H2 will buffer to disk using set max_memory_rows. You can append that to the database URL: jdbc:h2:~/test;max_memory_rows=200000.
  
  A workaround is usually to use "keyset paging" as described in the presentation "Pagination Done the Right Way". That would mean running multiple queries instead of one.
  
  http://www.h2database.com/html/advanced.html
  Before the result is returned to the application, all rows are read by the database. Server side cursors are not supported currently.
  
  http://www.h2database.com/javadoc/org/h2/engine/SysProperties.html?highlight=max_memory_rows&search=max_memory_rows#h2.maxMemoryRows
  System property h2.maxMemoryRows (default: 40000 per GB of available RAM).
  
  So if we set -Xmx3G  and partition by LOCALITY_PID we should be OK:
  There are 16398 LOCALITY rows and max ADDRESS_DETAILs for a LOCALITY is 95004.
  SELECT LOCALITY_PID , count(*) cnt FROM ADDRESS_DETAIL group by LOCALITY_PID order by cnt desc limit 3;
  
  LOCALITY_PID    CNT Feb 2016    CNT Nov 2017  
  VIC1634         95004           105960
  NSW3749         44656            45502
  QLD2772         34712            39162
  
  http://slick.typesafe.com/doc/3.1.1/dbio.html
  Slick's Database.stream produces a `Reactive Stream` that can be consumed with a foreach that takes a callback for each row.
  Since H2 is providing all the rows at once (see above):
  - the callback is called for multiple rows at once
  - concurrency is limited only by the number of threads
  - all the other callbacks are queued on the thread pool, preventing anything else from running on this pool.
  It's better to use Database.run to get all all the rows at once, allow H2 to release any resources, and to have some control over the
  concurrency of processing the rows.
 */

  def doLocality(
    localityPid: String, localityName: String, statePid: String, primaryPostcode: Option[String],
    stateMap: Future[Map[String, (String, String)]], flatTypeMap: FutStrMap, streetTypeMap: FutStrMap, streetSuffixMap: FutStrMap
  )(
    implicit db: Database
  ): Future[Unit] = {
    val state = stateMap.map(_.apply(statePid))
    val locVariant = localityVariant(localityPid)

    log.info(s"starting locality $localityName")
    db.run(qAddressDetail(localityPid).result).flatMap { seq =>
      log.info(s"got all addresses for locality $localityName")

      val seqFut: Seq[Future[Address]] = seq.map {
        case (
          // copied from AddressDetail.*
          addressDetailPid :: dateCreated :: dateLastModified :: dateRetired :: buildingName :: lotNumberPrefix :: lotNumber :: lotNumberSuffix ::
            flatTypeCode :: flatNumberPrefix :: flatNumber :: flatNumberSuffix ::
            levelTypeCode :: levelNumberPrefix :: levelNumber :: levelNumberSuffix ::
            numberFirstPrefix :: numberFirst :: numberFirstSuffix ::
            numberLastPrefix :: numberLast :: numberLastSuffix ::
            streetLocalityPid :: locationDescription :: localityPid :: aliasPrincipal :: postcode :: privateStreet :: legalParcelId :: confidence ::
            addressSitePid :: levelGeocodedCode :: propertyPid :: gnafPropertyPid :: primarySecondary :: HNil,
          levelTypeName,
          addressSiteName,
          street,
          location
          ) =>

          val addr: Future[Address] = for {
            (stateAbbreviation, stateName) <- state
            ftm <- flatTypeMap
            stm <- streetTypeMap
            ssm <- streetSuffixMap
            locVar <- locVariant
            sla <- streetLocalityAlias(streetLocalityPid)
          } yield {
            val geocodeTypeCode = location.map(_._1)
            Address(
            addressDetailPid, addressSiteName.flatten, buildingName,
            flatTypeCode, flatTypeCode.map(ftm), PreNumSufString(lotNumberPrefix, lotNumber, lotNumberSuffix), PreNumSuf(flatNumberPrefix, flatNumber, flatNumberSuffix),
            levelTypeCode, levelTypeName, PreNumSuf(levelNumberPrefix, levelNumber, levelNumberSuffix),
            PreNumSuf(numberFirstPrefix, numberFirst, numberFirstSuffix),
            PreNumSuf(numberLastPrefix, numberLast, numberLastSuffix),
            street.map(s => Street(s._1, s._2, s._2.map(stm), s._3, s._3.map(ssm))),
            localityName, primaryPostcode, stateAbbreviation, stateName, postcode,
            aliasPrincipal, primarySecondary, geocodeTypeCode,
            location.flatMap {
              case (_, Some(lat), Some(lon)) => Some(Location(lat, lon))
              case _                      => None
            },
            sla.map(s => Street(s._1, s._2, s._2.map(stm), s._3, s._3.map(ssm))),
            locVar)
          }

          addr.onComplete {
            case Success(a) => println(a.toJson.compactPrint) // println appears to be synchronized
            case Failure(e) => log.error(s"future address for $addressDetailPid failed", e)
          }

          /*
           * Trying to use small bounded thread pools I got:
           * 12:50:59.843 [Pool-2-thread-2] ERROR au.com.data61.gnaf.indexer.Main. - future address for GAACT715082885 failed
           * java.util.concurrent.RejectedExecutionException: Task slick.backend.DatabaseComponent$DatabaseDef$$anon$2@1dbaddc0 rejected from
           * java.util.concurrent.ThreadPoolExecutor@2bc930eb[Running, pool size = 3, active threads = 3, queued tasks = 987, completed tasks = 10]
           *         
           * The only pool with a queue size of 987 and 3 threads is the slick pool configured in application.conf.
           * I tried explicit flatMaps instead of for, with an explicit ExecutionContext, but it still used the slick pool!
           */
          addr
      }

      val locDone = Future.fold(seqFut)(())((_, _) => ())
      locDone.onComplete {
        case Success(_) => log.info(s"completed locality $localityName")
        case Failure(e) => log.error(s"future locality $localityName failed", e)
      }
      locDone
    }
  }

}
