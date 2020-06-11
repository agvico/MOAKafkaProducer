import java.io.{File, PrintWriter}
import java.lang
import java.util.Properties

import com.yahoo.labs.samoa.instances.{Instance, InstancesHeader}
import moa.MOAObject
import moa.core.{Example, InstanceExample}
import moa.options.ClassOption
import moa.streams.generators.RandomTreeGenerator
import moa.streams.{ArffFileStream, ConceptDriftStream, ExampleStream, InstanceStream}
import picocli.CommandLine
import picocli.CommandLine.{Option, Parameters}
import org.apache.kafka.clients.producer.{KafkaProducer, Producer}
import org.apache.kafka.common.serialization.LongSerializer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.clients.producer.ProducerRecord

class Main extends Runnable {

  @Option(names = Array("-f"), paramLabel = "PATH", description = Array("Read stream from the specified file"))
  var FILE: String = ""

  @Option(names = Array("-c"), paramLabel = "NUMBER", description = Array("The index of the class column for file streams. The default, -1, means the last column as the class value"))
  var CLASS_INDEX: Int = -1

  @Option(names = Array("-s"), paramLabel = "METHOD", description = Array("The stream generator"))
  var STREAM: String = "ConceptDriftStream"

  @Option(names = Array("-g"), split = ",", paramLabel = "GEN1,Gen2", description = Array("A comma-separated list with the MOA CLI lines corresponding to the generator(s) to be employed"))
  var GENERATORS = Array("generators.RandomTreeGenerator", "generators.RandomTreeGenerator")

  @Option(names = Array("-n"), paramLabel = "NUMBER", description = Array("The maximum number of instances to be generated. Default: 10.000.000"))
  var MAX_INSTANCES: Int = 1E7.toInt

  @Option(names = Array("-p"), paramLabel = "NUMBER", description = Array("Position where the drift occur, i.e., every X instances, a drift happen. Default: 250.000 instances"))
  var DRIFT_CENTER: Int = 250000

  @Option(names = Array("-w"), paramLabel = "NUMBER", description = Array("For gradual drifts, the amount of instances in the transition between generators. Default 10.000 instances"))
  var DRIFT_WIDTH: Int = 10000

  @Option(names = Array("-v"), description = Array("Be verbose."))
  var VERBOSE: Boolean = false

  @Option(names = Array("--abrupt"), description = Array("The drift is abrupt instead of gradual"))
  var abrupt: Boolean = false

  @Option(names = Array("--topic"), description = Array("The kafka topic to send data to"))
  var TOPIC: String = "test"

  @Option(names = Array("-r"), description = Array("The instance sending rate, i.e., the amount of instances sent in each time interval. Default: 100.000"))
  var INSTANCE_RATE: Int = 100000

  @Option(names = Array("-t"), description = Array("The time interval for sending the amount of instances set in -r in milliseconds. Default: 1000"))
  var TIME_INTERVAL: Long = 1000

  @Option(names = Array("--header"), description = Array("Generate the header in a file"))
  var HEADER_FILE = ""


  override def run(): Unit = {
    val CLIENT_ID: String = "KafkaProducer01"
    val NUM_PARTITIONS: Int = 1

    /* Configuración del productor *//* Configuración del productor */
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", classOf[LongSerializer].getName)
    props.put("value.serializer", classOf[StringSerializer].getName)
    props.put("client.id", CLIENT_ID)
    //props.put("num.partitions", NUM_PARTITIONS)

    // Create the producer with the given properties
    val producer: Producer[Long, String] = new KafkaProducer[Long, String](props)
    var instancesSent = 0

    // Create the stream
    val stream: InstanceStream = if (FILE equals "") {

      // Generate a Concept Drift stream (By now, only concept drift stream is supported
      if (STREAM equals "ConceptDriftStream") {
        val s = new ConceptDriftStream
        s.driftstreamOption.setValueViaCLIString(GENERATORS(0)) // Stream 1
        s.streamOption.setValueViaCLIString(GENERATORS(1)) // Stream 2
        s.positionOption.setValue(DRIFT_CENTER) // Position where the drift happen (every x instances)
        if (abrupt) {
          s.widthOption.setValue(1) // The width of the change window (for abrupt drift, set to 1)
        } else {
          s.widthOption.setValue(DRIFT_WIDTH)
        }

        s.prepareForUse()
        s
      } else {
        // Generate another Moa-Supported Stream
        val s: InstanceStream = STREAM match {
          case "RandomTree" => {
            new RandomTreeGenerator()
          }
          case _ => {
            throw new Exception
          }
        }
        s
      }

    } else {
      // If -f option is available, read the stream from a file.
      val s = new ArffFileStream(FILE, CLASS_INDEX)
      s.prepareForUse()
      s
    }

    var t_ini: Long = 0

    // First of all, generate the header
    if (!(HEADER_FILE equals "")) {
      val header = stream.getHeader.toString
      val pw = new PrintWriter(new File(HEADER_FILE))
      var instance = stream.nextInstance().getData.toString
      instance = instance.substring(0, instance.length - 1)
      pw.write(header)
      pw.write(instance + "\n")
      pw.close
      return

    }

    while (stream.hasMoreInstances && instancesSent < MAX_INSTANCES) {
      if (instancesSent % INSTANCE_RATE == 0) {
        t_ini = System.currentTimeMillis()
      }

      val instance: String = stream match {
        case s: ArffFileStream => {
          val inst = stream.nextInstance().getData.toString
          inst.substring(0, inst.length - 1)  // remove last comma
        }
        case _ => {stream.nextInstance().getData.toString}
      }

      producer.send(new ProducerRecord[Long, String](TOPIC, System.currentTimeMillis(), instance))
      instancesSent += 1

      val t_end = System.currentTimeMillis()
      if (instancesSent % INSTANCE_RATE == 0 && (t_end - t_ini) < TIME_INTERVAL) {
        val sleepTime = (t_ini + TIME_INTERVAL) - t_end
        if(VERBOSE) println("Instances sent: " + instancesSent + ". Sleeping for " + sleepTime + " ms")
        Thread.sleep(sleepTime)
      }
    }


  }


}


object Main {
  def main(args: Array[String]): Unit = {
    CommandLine.run(new Main(), System.err, args: _*)
  }
}
