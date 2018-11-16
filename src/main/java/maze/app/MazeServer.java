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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MazeServer {

  private static final Logger LOGGER = LoggerFactory.getLogger("GENERAL");
  private static final Logger TRANSPORT_LOGGER = LoggerFactory.getLogger("TRANSPORT");

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
      Source<MazePayload, NotUsed> source = Source
          .fromPublisher(eventSourceActorAndPublisher.second())
          .map(object -> (MazePayload) object)
          .toMat(BroadcastHub.of(MazePayload.class, 2048), Keep.right()).run(materializer);

      //Set up event publisher connection handling
      setUpEventPublisher(system, materializer, eventSourceActorAndPublisher.first());

      //Set up client connection handling
      setUpClient(system, materializer, source);

      System.out.println(PRESS_ENTER_TO_STOP);
      new BufferedReader(new InputStreamReader(System.in)).readLine();
    } finally {
      system.terminate();
    }
  }


  private static void setUpEventPublisher(ActorSystem system, Materializer materializer,
      ActorRef actorPublisher) {

    //Initialize connection handling
    Tcp.get(system).bind(INTERFACE, SERVER_PORT).runForeach(connection -> {

      LOGGER.debug(NEW_CONNECTION_FROM + SERVER_PORT + COLON + connection.remoteAddress());
      AtomicLong counter = new AtomicLong(0);

      //Publisher sink
      final Sink<ByteString, CompletionStage<Done>> dataPublisherSink = Sink.foreach(message -> {
            actorPublisher
                .tell(MazePayload.fromPayload(StringUtil.removeControlCharacters(message.utf8String())),
                    ActorRef.noSender());
            LOGGER.debug(MESSAGE + SERVER_PORT + COLON + message.utf8String());
            LOGGER.debug(NUMBER_OF_EVENTS_RECEIVED + counter.incrementAndGet());
          }
      );

      //Take line by line flow
      final Flow<ByteString, ByteString, NotUsed> delimiter = Framing
          .delimiter(ByteString.fromString("\n"), MAXIMUM_FRAME_LENGTH,
              FramingTruncation.DISALLOW);

      final Sink<ByteString, NotUsed> completionSink = Sink.onComplete(
          completion -> LOGGER.debug(EVENT_PUBLISHER_COMPLETED + completion + counter.get()));

      //TODO runtime source sorting
      //Event publisher
      connection
          .flow().via(delimiter).alsoTo(dataPublisherSink).alsoTo(completionSink)
          .runWith(Source.maybe(), Sink.ignore(), materializer);

    }, materializer);
  }

  private static void setUpClient(ActorSystem system, Materializer materializer,
      Source<MazePayload, NotUsed> source) {

    Tcp.get(system).bind(INTERFACE, CLIENT_PORT).runForeach(connection -> {
      AtomicReference<Long> userId = new AtomicReference<>(null);

      //User id update sink
      final Sink<ByteString, CompletionStage<Done>> updateUserIdSink = Sink.foreach(message -> {
            final Optional<Long> userIdOption = Optional.ofNullable(message).map(ByteString::utf8String)
                .map(StringUtil::removeControlCharacters).map(Long::valueOf);
            LOGGER.debug(USER + userIdOption.orElse(null));
            userIdOption.ifPresent(userId::set);
          }

      );
      LOGGER.debug(NEW_CONNECTION_FROM + CLIENT_PORT +
          COLON + connection.remoteAddress());

      final Sink<ByteString, CompletionStage<Done>> loggerSink = Sink.foreach(event -> TRANSPORT_LOGGER.debug(
          SENDING + StringUtil.removeControlCharacters(event
              .utf8String()) + TO_USER + userId.get()));

      final Sink<ByteString, NotUsed> completionSink = Sink
          .onComplete(
              completion -> LOGGER.debug(CLIENT_COMPLETED + userId.get() + SPACE + completion));

      Set<Long> following = Collections.synchronizedSet(new HashSet<>());

      final Sink<MazePayload, CompletionStage<Done>> followerSink = resolveFollowersSink(userId,
          following);

      //Pipe dynamic source content to clients
      connection.flow().to(updateUserIdSink)
          .runWith(
              source
                  .alsoTo(Flow.of(MazePayload.class).to(followerSink))
                  .filter(value ->
                      shouldReceiveMessage(userId, value, following))
                  .map(MazePayload::toPayload)
                  .map(ByteString::fromString)
                  .alsoTo(Flow.of(ByteString.class).to(loggerSink)).alsoTo(completionSink),
              materializer);

    }, materializer);

  }


  //Filter messages based on event rules
  private static boolean shouldReceiveMessage(
      AtomicReference<Long> userId, MazePayload mazePayload, Set<Long> following) {
    try {

      if (mazePayload.getPayloadType() == PayloadType.UNFOLLOW) {
        return false;
      }
      if (mazePayload.getPayloadType() == PayloadType.BROADCAST) {
        return true;
      }

      if (mazePayload.getPayloadType() == PayloadType.STATUS_UPDATE
      ) {
        return  mazePayload
            .getFromUserId().map(following::contains).orElse(false);
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

  private static Sink<MazePayload, CompletionStage<Done>> resolveFollowersSink(
      AtomicReference<Long> userId, Set<Long> following) {
    return Sink.foreach(mazePayload -> resolveFollowers(userId, following, mazePayload));
  }

  //Handle follow unfollow
  private static void resolveFollowers(AtomicReference<Long> userId, Set<Long> following,
      MazePayload mazePayload) {

    if (mazePayload.getPayloadType() == PayloadType.FOLLOW
        && mazePayload.getToUserId().isPresent() && Optional.ofNullable(userId.get())
        .map(v -> v.equals(mazePayload.getFromUserId().orElse(null)))
        .orElse(false)
    ) {
      LOGGER.debug(USER + userId.get() + IS_FOLLOWING + mazePayload
          .getToUserId()
          .orElse(null));

      mazePayload.getToUserId().ifPresent(following::add);
    }

    if (mazePayload.getPayloadType() == PayloadType.UNFOLLOW && mazePayload.getFromUserId()
        .isPresent() && Optional.ofNullable(userId.get())
        .map(v -> v.equals(mazePayload.getFromUserId().orElse(null)))
        .orElse(false)
    ) {
      LOGGER.debug(
          USER + userId.get() + UNFOLLOWED + mazePayload
              .getToUserId().orElse(null));

      mazePayload.getToUserId().ifPresent(following::remove);
    }
  }

}
