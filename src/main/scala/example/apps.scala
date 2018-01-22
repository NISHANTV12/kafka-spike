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
  implicit val materializer: ActorMaterializer = ActorMaterializer()(system)

  val producerSettings: ProducerSettings[Array[Byte], lang.Long] = ProducerSettings(system, new ByteArraySerializer, new LongSerializer)
    .withBootstrapServers("localhost:9092")

  val done = Source
    .repeat(())
    .delay(10.milli)
    .map(x => System.currentTimeMillis())
    .map { elem =>
      new ProducerRecord[Array[Byte], lang.Long]("topic2", elem)
    }
    //    .buffer(10000, OverflowStrategy.dropNew)
    .runWith(Producer.plainSink(producerSettings))
}

object Consumer1 extends App {

  private val system = ActorSystem("test")
  implicit val materializer: ActorMaterializer = ActorMaterializer()(system)

  val consumerSettings = ConsumerSettings(system, new ByteArrayDeserializer, new LongDeserializer)
    .withBootstrapServers("localhost:9092")
    .withGroupId("group1")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")

  private val subscription: AutoSubscription = Subscriptions.topics("topic2")

  val done2 =
    Consumer.plainSource(consumerSettings, subscription)
      .map(x => System.currentTimeMillis() - x.value())
      .runForeach { x =>
        //        Thread.sleep(100)
        //        println(x)
      }


  val sink: Sink[lang.Long, Future[Done]] = Sink.foreach[lang.Long] { x =>
    val dd = System.currentTimeMillis() - x
    println(s"throttled: $dd")
  }//.addAttributes(Attributes.inputBuffer(initial = 1, max = 1))

  val done3 =
    Consumer.plainSource(consumerSettings.withGroupId("group2"), subscription)
      .delay(1.second, DelayOverflowStrategy.dropBuffer)
      .addAttributes(Attributes.inputBuffer(initial = 1, max = 1))
      .map(_.value())
      .runWith(sink)
}
