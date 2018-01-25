package example

import java.lang

import akka.Done
import akka.actor.ActorSystem
import akka.kafka.{AutoSubscription, ConsumerSettings, ProducerSettings, Subscriptions}
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.stream._
import akka.stream.scaladsl.{Flow, Sink, Source}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization._

import scala.concurrent.Future
import scala.concurrent.duration.DurationLong

object Producer1 extends App {

  private val system = ActorSystem("test")
  private val actorMaterializerSettings = ActorMaterializerSettings(system).withInputBuffer(1, 1)

  implicit val materializer: ActorMaterializer = ActorMaterializer()(system)

  val producerSettings: ProducerSettings[Array[Byte], lang.Long] = ProducerSettings(system, new ByteArraySerializer, new LongSerializer)
    .withBootstrapServers("localhost:9092")

  //  val done = Source
  //    .repeat(())
  //    .delay(10.milli)
  //    .map(x => System.currentTimeMillis())
  //    .map { elem =>
  //      new ProducerRecord[Array[Byte], lang.Long]("topic2", elem)
  //    }
  //    //    .buffer(10000, OverflowStrategy.dropNew)
  //    .runWith(Producer.plainSink(producerSettings))

  //  val done = Source
  //        .tick(1.second, 1.millis, ())
  ////    .repeat(())
  //    .map { x =>
  //      new ProducerRecord[Array[Byte], lang.Long]("topic2", System.currentTimeMillis())
  //    }
  //    .runWith(Producer.plainSink(producerSettings))


  Source(1L to 500000)
    //        .tick(1.second, 1.millis, ())
    //    .repeat(())
    .map { x ⇒
    println(x)
    new ProducerRecord[Array[Byte], lang.Long]("topic2", x)

  }
    .throttle(10000, 1.second, 10000, ThrottleMode.shaping)
    .runWith(Producer.plainSink(producerSettings))
}


