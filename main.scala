import java.nio.file.{Path, Paths}
import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.{FlowShape, Graph, IOResult, OverflowStrategy}
import akka.stream.scaladsl.{Broadcast, FileIO, Flow, GraphDSL, Merge, RunnableGraph, Source,Keep}
import akka.stream.alpakka.csv.scaladsl.{CsvParsing, CsvToMap}
import akka.util.ByteString
import java.nio.file.StandardOpenOption.*
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.concurrent.duration.*


// turn the raw CSV lines into objects
case class RideQueueRecord(ride_id: Int,
                           ride_name: String,
                           is_open: Boolean,
                           wait_time: Int,
                           date: String,
                           timestamp: String)


object RideQueue extends App:

  implicit val actorSystem: ActorSystem = ActorSystem("RideQueue")
  implicit val dispatcher: ExecutionContextExecutor = actorSystem.dispatcher

  // Path to the CSV file
  val resourcesFolder: String = "src/main/resources"
  val pathCSVFile: Path = Paths.get(s"$resourcesFolder/queuetimes.csv")

  // Read the CSV file as a stream of bytes
  val source: Source[ByteString, Future[IOResult]] = FileIO.fromPath(pathCSVFile)

  // Split the file into CSV lines
  val csvParsing: Flow[ByteString, List[ByteString], NotUsed] = CsvParsing.lineScanner()

  // Convert each CSV line into a map with header as keys
  val mappingHeader: Flow[List[ByteString], Map[String, ByteString], NotUsed] = CsvToMap.toMap()

  // Convert the map into RideQueueRecord object
  val flowRide: Flow[Map[String, ByteString], RideQueueRecord, NotUsed] = Flow[Map[String, ByteString]]
    .map(tempMap => {
      tempMap.map(element => {
        (element._1, element._2.utf8String) // Change ByteString into normal Strings
      })
    }).map(record => {
      RideQueueRecord(ride_id = record("ride_id").toInt,
                      ride_name = record("ride_name"),
                      is_open = record("is_open").toBoolean,
                      wait_time = record("wait_time").toInt,
                      date = record("date"),
                      timestamp = record("timestamp"))
    })


  // count how many rides are in the dataset
  val countNumberRidesFlow = Flow[RideQueueRecord]
    .fold(List.empty[Int]) { (uniqueIds, record) => //list of unique ride_ids
      if (uniqueIds.contains(record.ride_id)) { // already saw this id, don't change the list
        uniqueIds
      } else {
        record.ride_id :: uniqueIds // otherwise add this new id to the list
      }
    }
    // Count the number of unique id's
    .map { uniqueRideIds =>
      uniqueRideIds.size
    }.map { n => // convert the number into a byteString line for the output file
      ByteString(s"Answer question 1: $n different rides \n \n")
    }


  // top 5 rides by the mean wait time?
  val best5MeanWaitTimesFlow = Flow[RideQueueRecord]
    // construct a map: (ride_id, ride name) => (total WaitTime, number of records)
    .fold(Map.empty[(Int, String), (Int, Int)]) {(idMap, record) =>
       // get current total sum and count for the (rideID, ride name) default (0, 0) if not found
       val (sum,count) = idMap.getOrElse((record.ride_id,record.ride_name), (0, 0))
       idMap.updated((record.ride_id, record.ride_name), (sum + record.wait_time, count + 1)) // update the map
       }
     .map { idMap =>
       idMap.map { element =>
         val rideId = element._1._1
         val rideName = element._1._2
         val sum = element._2._1
         val count = element._2._2
         (rideId.toString,rideName ,sum.toDouble / count) // new map with (id,name ,mean)
         }
       }
     .map { meanMap =>
       meanMap.toList
       .sortBy(_._3)(Ordering[Double].reverse) // sort descending
       .take(5) // top 5
     }.map { list =>   // convert the list into a byteString line for the output file
      val header = "Answer to Question 2: Top 5 Rides by Mean Wait Time\n"
      val text =
        list.map { e =>
          val id   = e._1
          val name = e._2
          val avg  = e._3
          s"$name (ID: $id) => Average Wait: $avg \n"
        }.mkString
      val tot =  header + text + "\n \n"
      ByteString(tot)
    }


// percentage of recorded intervals each rides was open
  val openIntervalFlow = Flow[RideQueueRecord]
    // construct a map: (ride_id, ride name) => (open count, closed count)
    .fold(Map.empty[(Int, String), (Int, Int)]) {(idMap, record) =>
      val rideId = record.ride_id
      val rideName = record.ride_name
      val rideOpen = record.is_open

      // total open and closed count default to (0, 0) if (ride_id, ride name) not found
      val (openCount, closedCount) = idMap.getOrElse((rideId,rideName), (0, 0))

      // update open or closed count
      if (rideOpen){
        idMap.updated((rideId,rideName), (openCount + 1, closedCount))
      } else{
        idMap.updated((rideId,rideName), (openCount, closedCount + 1))
      }
    }
    .map {idMap => // convert counts to percentage open
      idMap.map { element =>
        val rideId = element._1._1
        val rideName = element._1._2
        val sumTrue = element._2._1
        val sumFalse = element._2._2

        // percentage = open / (open + closed) * 100
        (rideId.toString,rideName ,sumTrue.toDouble / (sumTrue + sumFalse) * 100)
        }.toList
      }.map { list =>   // convert the list into a byteString line for the output file
      val header = "Answer to Question 3: For each attraction, what percentage of recorded intervals was it open? \n"
      val text =
        list.map { e =>
          val id   = e._1
          val name = e._2
          val per  = e._3
          s"$name (ID: $id) => $per %\n"
        }.mkString
      val tot =  header + text + "\n \n"
      ByteString(tot)
    }


  // ride with the largest variation in queue times
  val largestVariationQueueTimesFlow = Flow[RideQueueRecord]

    // construct a map: (ride_id, ride name) => (minWait, maxWait)
    .fold(Map.empty[(Int, String), (Int, Int)]) { (acc, record) =>
      val rideId = record.ride_id
      val rideName = record.ride_name
      val rideWaitTime = record.wait_time

      // Get current min and maw wait time or default (0,0)
      val (minWait, maxWait) = acc.getOrElse((rideId,rideName), (0, 0))

      // Update min and max
      if (rideWaitTime < minWait) {
        acc.updated((rideId,rideName), (rideWaitTime, maxWait))  // found a new minimum
      }
      else if (rideWaitTime > maxWait) {
        acc.updated((rideId,rideName), (minWait, rideWaitTime))  // found a new maximum
      }
      else {
        acc.updated((rideId,rideName), (minWait, maxWait)) // no change
      }
    }// variation = max - min for each ride
    .map { idMap =>
      idMap.map { element =>
        val rideId = element._1._1
        val rideName = element._1._2
        val minWait = element._2._1
        val maxWait = element._2._2

        (rideId.toString,rideName, maxWait - minWait.toDouble)
      }.toList
    }
    // Keep only the ride with the largest variation
    .map { percentageMap =>
      percentageMap
        .sortBy(_._3)(Ordering[Double].reverse)
        .take(1)
    }.map { list =>   // convert the list into a byteString line for the output file
      val header = "Answer to Question 4: Which attraction experiences the largest variation in queue times throughout the day?  \n"
      val text =
        list.map { e =>
          val id   = e._1
          val name = e._2
          val variation  = e._3
          s"$name (ID: $id) => $variation \n"
        }.mkString
      val tot =  header + text + "\n \n"
      ByteString(tot)
    }


  // overall park-wide average wait time per hour
  val hourAverageWaitTime = Flow[RideQueueRecord]
      // construct a map: hour => (total waitTime, number of Records)
      .fold(Map.empty[String, (Int, Int)]) { (hourMap, record) =>
        val hour = record.timestamp.substring(0, 2) // get only the hour without minutes
        val rideWaitTime = record.wait_time

        // Get(sum, count) for the current hour default (0,0)
        val (totalSum,count) = hourMap.getOrElse(hour, (0, 0))

        // Update sum and count
        hourMap.updated(hour, (totalSum + rideWaitTime, count + 1))
      }
      // calculate the average
      .map { hourMap =>
        hourMap.map { element =>
          val hour = element._1
          val sum = element._2._1 // total sum
          val total = element._2._2 // total count
          (hour, sum / total.toDouble) // compute average
        }.toList
      }.map { list =>   // convert the list into a byteString line for the output file
      val header = "Answer to Question 5: What is the overall park-wide average wait time per hour? \n"
      val text =
        list.map { e =>
          val hour  = e._1
          val avg = e._2
          s"at $hour H => $avg on average \n"
        }.mkString
      val tot =  header + text + "\n \n"
      ByteString(tot)
    }


  // dynamic skip-the-line price per hour for each ride
  val pricePerHour = Flow[RideQueueRecord]
    // Only keep records where the ride is open
    .filter(record => record.is_open)

    // construct a map: (date, hour, rideId,rideName) =>(totalWait, count, List[(wait, price, time,average wait)])
    .fold(Map.empty[(String, String, Int, String),  (Int, Int, List[(Int, Double, String,Double)])]) { (hourDateMap, record) =>
      val hour = record.timestamp.substring(0, 2)
      val date = record.date
      val rideName = record.ride_name
      val rideId = record.ride_id
      val rideWaitTime = record.wait_time

      // Get current data for this (date, hour, rideId) key, or default empty values
      val (totalWait, count, elementen) = hourDateMap.getOrElse((date, hour, rideId, rideName), (0, 0, List.empty))

      // Compute new average wait time with new entry in the same hour
      val avg = (totalWait + rideWaitTime.toDouble) / (count + 1)

      // Compute price for this record
      val price = computePrice(rideWaitTime, avg)

      // update values back into the map with new price for new record in same hour
      hourDateMap.updated((date, hour, rideId,rideName),(totalWait + rideWaitTime, count + 1, (rideWaitTime, price, record.timestamp, avg):: elementen))
    }
    .map { hourMap =>
      hourMap.map { element => // Convert one map element into a map where each wait time is paired with its corresponding price
        val date = element._1._1
        val hour = element._1._2
        val rideId = element._1._3
        val rideName = element._1._4
        val entryList = element._2._3

        (date, hour, rideId,rideName, entryList)
      }.toList
      // convert the list into a byteString line for the output file
    }.map{ list =>
      val header = "Answer to Question 6: What would be a fair, dynamic skip-the-line price per hour for each ride, given its average and current wait time?\n"
      val header2 = "date, hour, ride id, ride name, avg wait, current wait, price (EUR)\n"
      val text = list.flatMap { entry =>
        val date = entry._1
        val rideId = entry._3
        val rideName = entry._4
        val list = entry._5
        list.map { p =>
          val wait = p._1
          val price = p._2
          val realHour = p._3
          val avg = p._4
          s"$date, $realHour, $rideId,$rideName, $avg,$wait, $price \n"
        }
      }.mkString
      val tot =  header + header2 +text + "\n \n"
      ByteString(tot)
    }


  private def computePrice(currentWait: Int, avgWait: Double): Double =
    val ratio = currentWait.toDouble / avgWait
    val raw = 5 * math.pow(ratio, 1.25)

    // clamp via if
    val clamped =
      if raw < 2 then 2
      else if raw > 25 then 25
      else raw

    // afronden op 0.50
    math.round(clamped / 0.5) * 0.5

  val graph: Graph[FlowShape[RideQueueRecord, ByteString], NotUsed] =
    Flow.fromGraph(
      GraphDSL.create() {
        implicit builder =>
        import GraphDSL.Implicits._

        //buffer with a capacity of 10 elements, backpressure for upstream producers must wait until downstream consumers are ready
        val buffer = builder.add(Flow[RideQueueRecord].buffer(10, OverflowStrategy.backpressure))
        //throttle: max 5000 elements per seconds
        val throttle = Flow[RideQueueRecord].throttle(5000, 1.second)

        //copy each element to 6 outputs
        val broadcast = builder.add(Broadcast[RideQueueRecord](6))
        //merge six outputs back into one stream
        val merge = builder.add(Merge[ByteString](6))



        // the 6 questions flows
        val q1 = builder.add(countNumberRidesFlow)
        val q2 = builder.add(best5MeanWaitTimesFlow)
        val q3 = builder.add(openIntervalFlow)
        val q4 = builder.add(largestVariationQueueTimesFlow)
        val q5 = builder.add(hourAverageWaitTime)
        val q6 = builder.add(pricePerHour)

        buffer ~> throttle ~> broadcast.in
        broadcast.out(0) ~> q1 ~> merge.in(0)
        broadcast.out(1) ~> q2 ~> merge.in(1)
        broadcast.out(2) ~> q3 ~> merge.in(2)
        broadcast.out(3) ~> q4 ~> merge.in(3)
        broadcast.out(4) ~> q5 ~> merge.in(4)
        broadcast.out(5) ~> q6 ~> merge.in(5)

        FlowShape(buffer.in, merge.out)
      }
    )

  // write the final output to a text file
  val sink = FileIO.toPath(Paths.get(s"$resourcesFolder/streamAssingnment1Output.txt"), Set(CREATE, WRITE, APPEND))

  // full stream pipeline and connect it to the sink
  val runnableGraph: RunnableGraph[Future[IOResult]] =
      source
        .via(csvParsing)
        .via(mappingHeader)
        .via(flowRide)
        .via(graph)
        .toMat(sink)(Keep.right)

  // run the stream and stop when finish
  runnableGraph.run().onComplete { result =>
    actorSystem.terminate()
  }
