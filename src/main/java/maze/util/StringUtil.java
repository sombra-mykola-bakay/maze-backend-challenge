package maze.util;

public final class StringUtil {

  private static final String CONTROL_CHARACTER_REGEX = "(?U)\\p{Cntrl}";
  private static final String EMPTY = "";

  private StringUtil() {
    throw new AssertionError();
  }

  public static String removeControlCharacters(String input) {
    return input.replaceAll(CONTROL_CHARACTER_REGEX, EMPTY);
  }
}
