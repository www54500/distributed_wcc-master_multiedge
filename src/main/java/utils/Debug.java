package utils;

public class Debug {
  public Debug() {}
  
  public static void print(Class<?> classType, String content) {
    String[] nameArr = classType.getName().split("\\.");
    String className = nameArr[nameArr.length - 1];
    System.out.print("[" + className + "]"  + content);
  }
  
  public static void println(Class<?> classType, String content) {
    String[] nameArr = classType.getName().split("\\.");
    String className = nameArr[nameArr.length - 1];
    System.out.println("[" + className + "]"  + content);
  }
  
  public static void print(String className, String content) {
    System.out.print("[" + className + "]"  + content);
  }
  
  public static void println(String className, String content) {
    System.out.println("[" + className + "]"  + content);
  }
  
  public static void print(String content) {
    System.out.print(content);
  }
  
  public static void println(String content) {
    System.out.println(content);
  }
}