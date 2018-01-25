package example

import java.lang

import akka.actor.ActorSystem
import akka.kafka.scaladsl.Consumer
import akka.kafka.{AutoSubscription, ConsumerSettings, Subscriptions}
import akka.stream.scaladsl.Sink
import akka.stream.{ActorMaterializer, OverflowStrategy}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, LongDeserializer}

import scala.collection.immutable
import scala.concurrent.duration.DurationDouble
import scala.concurrent.{Await, Future}

object Consumer1 extends App {

  private val system = ActorSystem("test")
  implicit val materializer: ActorMaterializer = ActorMaterializer()(system)

  val consumerSettings = ConsumerSettings(system, new ByteArrayDeserializer, new LongDeserializer)
    .withBootstrapServers("localhost:9092")
    .withGroupId("group1")
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")

  private val subscription: AutoSubscription = Subscriptions.topics("topic2")

//  val done =
//    Consumer.plainSource(consumerSettings, subscription)
//      .map(x => System.currentTimeMillis() - x.value())
//      .runForeach { x =>
//        //        Thread.sleep(100)
//        //        println(x)
//      }


//  val sink: Sink[lang.Long, Future[Done]] = Sink.foreach[lang.Long] { x =>
//    val dd = System.currentTimeMillis() - x
//    println(s"throttled: $dd")
//  }.addAttributes(Attributes.inputBuffer(initial = 1, max = 1))

//  val done2 =
//    Consumer.plainSource(consumerSettings.withGroupId("group2"), subscription)
//      .delay(1.second, DelayOverflowStrategy.dropBuffer)
//      .addAttributes(Attributes.inputBuffer(initial = 1, max = 1))
//      .map(_.value())
//      .runWith(sink)

  //  val done3 =
  //    Consumer.plainSource(consumerSettings, subscription).buffer(1, OverflowStrategy.dropBuffer).async
  //      .runForeach { x =>
  //        println(System.currentTimeMillis() - x.value())
  //        Thread.sleep(1000)
  //      }

  //  val done4 =
  //    Consumer.plainSource(consumerSettings, subscription).buffer(1, OverflowStrategy.dropBuffer).async
  //      .delay(1.second, DelayOverflowStrategy.dropBuffer)
  //      .runForeach { x =>
  //        println(System.currentTimeMillis() - x.value())
  //      }

  val done5: Future[immutable.Seq[lang.Long]] =
    Consumer.plainSource(consumerSettings, subscription)
      .buffer(1, OverflowStrategy.dropHead).async
      .take(50000)
      .map(_.value())
      .runWith(Sink.seq)

  //      .delay(1.second, DelayOverflowStrategy.dropBuffer)
  //      .addAttributes(Attributes.inputBuffer(initial = 1, max = 1))
  //      .buffer(1, OverflowStrategy.dropBuffer)
  //conflate((a, b) ⇒ b)

  val consumer = Await.result(done5, 30.seconds)

  println(consumer == consumer.sorted)

  println(consumer)
  println(consumer.sorted)
}
