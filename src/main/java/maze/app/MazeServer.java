package maze.app;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.OverflowStrategy;
import akka.stream.javadsl.AsPublisher;
import akka.stream.javadsl.BroadcastHub;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.Framing;
import akka.stream.javadsl.FramingTruncation;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.Tcp;
import akka.util.ByteString;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import maze.model.MazePayload;
import maze.model.MazePayload.PayloadType;
import maze.util.StringUtil;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MazeServer {

  private static final Logger LOGGER = LoggerFactory.getLogger("GENERAL");
  private static final String INTERFACE = "127.0.0.1";
  private static final int CLIENT_PORT = 9099;
  private static final int SERVER_PORT = 9090;

  private static final String NEW_CONNECTION_FROM = "New connection from ";
  private static final int MAXIMUM_FRAME_LENGTH = 256;
  private static final int BUFFER_SIZE = 10000;
  private static final String NUMBER_OF_EVENTS_RECEIVED = "Number of events received : ";
  private static final String PRESS_ENTER_TO_STOP = "Press ENTER to stop.";
  private static final String EVENT_PUBLISHER_COMPLETED = "Event publisher completed ";
  private static final String SENDING = "Sending  : ";
  private static final String TO_USER = " to user ";
  private static final String CLIENT_COMPLETED = "Client completed ";
  private static final String MESSAGE = "Message ";
  private static final String USER = "User ";
  private static final String IS_FOLLOWING = " is following : ";
  private static final String COLON = " : ";
  private static final String SPACE = " ";
  private static final String PREDICATE_FLOW_FAILED = "Predicate flow failed";
  private static final String UNFOLLOWED = " unfollowed : ";

  public static void main(String[] args) throws Exception {
    ActorSystem system = ActorSystem.create();
    try {
      final Materializer materializer = ActorMaterializer.create(system);

      //Actor messages to publisher
      Pair<ActorRef, org.reactivestreams.Publisher<Object>> eventSourceActorAndPublisher = Source
          .actorRef(BUFFER_SIZE, OverflowStrategy.fail())
          .toMat(Sink.asPublisher(AsPublisher.WITH_FANOUT), Keep.both()).run(materializer);

      //Infinite source backed by publisher
      Source<ByteString, NotUsed> source = Source
          .fromPublisher(eventSourceActorAndPublisher.second())
          .map(object -> (ByteString) object)
          .toMat(BroadcastHub.of(ByteString.class, 2048), Keep.right()).run(materializer);

      //Set up event publisher connection handling
      setUpEventPublisher(system, materializer, eventSourceActorAndPublisher);

      //Set up client connection handling
      setUpClient(system, materializer, source);

      System.out.println(PRESS_ENTER_TO_STOP);
      new BufferedReader(new InputStreamReader(System.in)).readLine();
    } finally {
      system.terminate();
    }
  }

  private static void setUpEventPublisher(ActorSystem system, Materializer materializer,
      Pair<ActorRef, Publisher<Object>> eventSourceActorAndPublisher) {
    //Initialize connection handling
    Tcp.get(system).bind(INTERFACE, SERVER_PORT).runForeach(connection -> {

      LOGGER.info(NEW_CONNECTION_FROM + SERVER_PORT + COLON + connection.remoteAddress());
      AtomicLong counter = new AtomicLong(0);

      //Publisher sink
      final Sink<ByteString, CompletionStage<Done>> dataPublisherSink = Sink.foreach(message -> {
            eventSourceActorAndPublisher.first().tell(message, ActorRef.noSender());
            LOGGER.info(MESSAGE + CLIENT_PORT + COLON + message.utf8String());
            LOGGER.info(NUMBER_OF_EVENTS_RECEIVED + counter.incrementAndGet());
          }
      );

      //Take line by line flow
      final Flow<ByteString, ByteString, NotUsed> delimiter = Framing
          .delimiter(ByteString.fromString("\n"), MAXIMUM_FRAME_LENGTH,
              FramingTruncation.DISALLOW);

      final Sink<ByteString, NotUsed> completionSink = Sink.onComplete(
          completion -> LOGGER.info(EVENT_PUBLISHER_COMPLETED + completion));

      //Event publisher flow
      final Flow<ByteString, ByteString, NotUsed> eventPublisherFlow =
          Flow.of(ByteString.class)
              .via(delimiter)
              .alsoTo(dataPublisherSink)
              .alsoTo(completionSink);

      connection.handleWith(eventPublisherFlow, materializer);
    }, materializer);
  }

  private static void setUpClient(ActorSystem system, Materializer materializer,
      Source<ByteString, NotUsed> source) {

    Tcp.get(system).bind(INTERFACE, CLIENT_PORT).runForeach(connection -> {
      AtomicReference<Long> userId = new AtomicReference<>(null);

      //User id update sink
      final Sink<ByteString, CompletionStage<Done>> updateUserIdSink = Sink.foreach(message -> {
            final Optional<Long> userIdOption = Optional.ofNullable(message).map(ByteString::utf8String)
                .map(StringUtil::removeControlCharacters).map(Long::valueOf);
            LOGGER.info(USER + userIdOption.orElse(null));
            userIdOption.ifPresent(userId::set);
          }

      );

      LOGGER.info(NEW_CONNECTION_FROM + CLIENT_PORT +
          COLON + connection.remoteAddress());

      final Sink<ByteString, CompletionStage<Done>> loggerSink = Sink.foreach(event -> LOGGER.info(
          SENDING + event
              .utf8String() + TO_USER + userId.get()));

      final Sink<ByteString, NotUsed> completionSink = Sink
          .onComplete(
              completion -> LOGGER.info(CLIENT_COMPLETED + userId.get() + SPACE + completion));

      Set<Long> following = Collections.synchronizedSet(new HashSet<>());
      final Sink<ByteString, CompletionStage<Done>> followerSink = resolveFollowers(userId,
          following);


      //FIXME Should be piped sequentially
      source.to(followerSink).run(materializer);

      //Pipe dynamic source content to clients
      source.filter(value ->
          shouldReceiveMessage(userId, value, following)
      ).alsoTo(loggerSink)
          .via(connection.flow())
          .alsoTo(completionSink)
          .to(updateUserIdSink)
          .run(materializer);

    }, materializer);

  }


  //Filter messages based on event rules
  private static boolean shouldReceiveMessage(
      AtomicReference<Long> userId, ByteString value, Set<Long> following) {
    try {
      final MazePayload mazePayload = MazePayload.fromPayload(value.utf8String());

      if (mazePayload.getPayloadType() == PayloadType.UNFOLLOW) {
        return false;
      }
      if (mazePayload.getPayloadType() == PayloadType.BROADCAST) {
        return true;
      }

      if (mazePayload.getPayloadType() == PayloadType.STATUS_UPDATE
      ) {
        return mazePayload.getFromUserId().map(following::contains).orElse(false);
      }
      if (mazePayload.getPayloadType() == PayloadType.PRIVATE_MSG
          || mazePayload.getPayloadType() == PayloadType.FOLLOW) {

        return Optional.ofNullable(userId.get())
            .map(uId -> uId.equals(mazePayload.getToUserId().orElse(null))).orElse(false);
      }
      return false;
    } catch (Exception e) {
      LOGGER.error(PREDICATE_FLOW_FAILED, e);
      return false;
    }
  }

  private static Sink<ByteString, CompletionStage<Done>> resolveFollowers(
      AtomicReference<Long> userId, Set<Long> following) {
    //Handle follow unfollow
    return Sink.foreach(message -> {
          final MazePayload mazePayload = MazePayload.fromPayload(message.utf8String());

          if (mazePayload.getPayloadType() == PayloadType.FOLLOW
              && mazePayload.getToUserId().isPresent() && Optional.ofNullable(userId.get())
              .map(v -> v.equals(mazePayload.getToUserId().orElse(null))).orElse(false)
          ) {
            LOGGER.info(USER + userId.get() + IS_FOLLOWING + mazePayload.getFromUserId()
                .orElse(null));
            mazePayload.getFromUserId().ifPresent(following::add);
          }

          if (mazePayload.getPayloadType() == PayloadType.UNFOLLOW && mazePayload.getFromUserId()
              .isPresent() && Optional.ofNullable(userId.get())
              .map(v -> v.equals(mazePayload.getToUserId().orElse(null))).orElse(false)
          ) {
            LOGGER.info(
                USER + userId.get() + UNFOLLOWED + mazePayload.getFromUserId().orElse(null));

            mazePayload.getFromUserId().ifPresent(following::remove);
          }
        }
    );
  }

}
