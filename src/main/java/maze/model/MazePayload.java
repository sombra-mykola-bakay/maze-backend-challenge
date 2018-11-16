package maze.model;

import java.util.Arrays;
import java.util.Optional;
import maze.util.StringUtil;


public class MazePayload {

  private final Long sequence;
  private final PayloadType payloadType;
  private final Long fromUserId;
  private final Long toUserId;

  private MazePayload(
      Long sequence,
      PayloadType payloadType,
      Long fromUserId,
      Long toUserId) {
    this.sequence = sequence;
    this.payloadType = payloadType;
    this.fromUserId = fromUserId;
    this.toUserId = toUserId;
  }

  public Long getSequence() {
    return sequence;
  }

  public PayloadType getPayloadType() {
    return payloadType;
  }

  public Optional<Long> getFromUserId() {
    return Optional.ofNullable(fromUserId);
  }

  public Optional<Long> getToUserId() {
    return Optional.ofNullable(toUserId);
  }

  public static MazePayload fromPayload(String payload) {
    if (payload == null || payload.isEmpty()) {
      throw new RuntimeException("Invalid payload : " + payload);
    }
    String payloadInternal = StringUtil.removeControlCharacters(payload);
    String[] parts = payloadInternal.split("\\|");
    if (parts.length != 2 && parts.length != 3 && parts.length != 4) {
      throw new RuntimeException("Invalid payload : " + payload);
    }
    if (parts.length == 2) {
      return new MazePayload(Long.valueOf(parts[0]), PayloadType.fromAlias(parts[1]), null,
          null);
    }
    if (parts.length == 3) {
      return new MazePayload(Long.valueOf(parts[0]), PayloadType.fromAlias(parts[1]),
          Long.valueOf(parts[2]),
          null);
    }
    return new MazePayload(Long.valueOf(parts[0]), PayloadType.fromAlias(parts[1]),
        Long.valueOf(parts[2]),
        Long.valueOf(parts[3]));
  }

  public String toPayload() {

    String payloadInternal = sequence + "|" + payloadType.alias;
    if (fromUserId != null) {
      payloadInternal = payloadInternal + "|" + fromUserId;
    }
    if (toUserId != null) {
      payloadInternal = payloadInternal + "|" + toUserId;
    }
    return payloadInternal+ "\n";
  }

  public enum PayloadType {
    FOLLOW("F"),
    UNFOLLOW("U"),
    BROADCAST("B"),
    PRIVATE_MSG("P"),
    STATUS_UPDATE("S");

    private String alias;

    PayloadType(String alias) {
      this.alias = alias;
    }

    public static PayloadType fromAlias(String alias) {
      return Arrays.stream(values()).filter(value -> value.alias.equalsIgnoreCase(alias))
          .findFirst()
          .orElseThrow(() -> new RuntimeException("Payload type " + alias + "is not resolvable"));
    }

  }
}
