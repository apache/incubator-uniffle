package org.apache.uniffle.server;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.KryoDataInput;
import com.esotericsoftware.kryo.io.KryoDataOutput;
import com.esotericsoftware.kryo.io.Output;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.roaringbitmap.longlong.Roaring64NavigableMap;
import sun.misc.Signal;
import sun.misc.SignalHandler;

/**
 * @author zhangjunfan
 * @date 2022/11/7
 */
public class KryoTest {

  public class TaskBufferManagerSerializer extends Serializer<TaskBufferManager> {

    public void write(Kryo kryo, Output output, TaskBufferManager object) {
      output.writeString(object.getBufferName());
      try {
        object.getMap().serialize(new KryoDataOutput(output));
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    public TaskBufferManager read(Kryo kryo, Input input, Class<TaskBufferManager> type) {
      TaskBufferManager bufferManager = new TaskBufferManager();
      bufferManager.setBufferName(input.readString());
      Roaring64NavigableMap bitmap = new Roaring64NavigableMap();
      try {
        bitmap.deserialize(new KryoDataInput(input));
      } catch (IOException e) {
        e.printStackTrace();
      }
      bufferManager.setMap(bitmap);
      return bufferManager;
    }
  }

  static class TaskBufferManager {
    private String bufferName;
    private int bufferAge = 100;
    private Roaring64NavigableMap map;

    public String getBufferName() {
      return bufferName;
    }

    public void setBufferName(String bufferName) {
      this.bufferName = bufferName;
    }

    public int getBufferAge() {
      return bufferAge;
    }

    public void setBufferAge(int bufferAge) {
      this.bufferAge = bufferAge;
    }

    public Roaring64NavigableMap getMap() {
      return map;
    }

    public void setMap(Roaring64NavigableMap map) {
      this.map = map;
    }
  }

  static class TaskManager {
    private String user;
    private String address;
    private int age;

    private TaskBufferManager taskBufferManager;

    public String getUser() {
      return user;
    }

    public void setUser(String user) {
      this.user = user;
    }

    public String getAddress() {
      return address;
    }

    public void setAddress(String address) {
      this.address = address;
    }

    public int getAge() {
      return age;
    }

    public void setAge(int age) {
      this.age = age;
    }

    public TaskBufferManager getTaskBufferManager() {
      return taskBufferManager;
    }

    public void setTaskBufferManager(TaskBufferManager taskBufferManager) {
      this.taskBufferManager = taskBufferManager;
    }
  }

  public static Input input;
  public static Kryo kryo;
  public static Output output;

  @BeforeAll
  public static void init() throws FileNotFoundException {
    kryo = new Kryo();
    output = new Output(new FileOutputStream("/tmp/file1.bin"));
    input = new Input(new FileInputStream("/tmp/file1.bin"));
  }

  @Test
  public void t1() {
    TaskManager taskManager = new TaskManager();
    taskManager.address = "shanghai";
    taskManager.user = "zuston";
    taskManager.age = 10;

    TaskBufferManager bufferManager = new TaskBufferManager();
    bufferManager.setBufferName("hello-world");
    bufferManager.setBufferAge(200);

    Roaring64NavigableMap map = Roaring64NavigableMap.bitmapOf(100, 101, 1003, 111);
    bufferManager.setMap(map);

    taskManager.setTaskBufferManager(bufferManager);

    kryo.writeObject(output, taskManager);
    output.close();

    TaskManager tm = kryo.readObject(input, TaskManager.class);
    input.close();
    System.out.println(tm.getTaskBufferManager().getBufferAge());
    System.out.println(tm.getTaskBufferManager().getBufferName());
    System.out.println(tm.getTaskBufferManager().getMap().contains(1003));
    System.out.println(tm.getTaskBufferManager().getMap().contains(12));
    System.out.println(tm.getTaskBufferManager().getMap().contains(111));
  }

  @Test
  public void t2() throws InterruptedException {
    new ShutdownSignal();
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      @Override
      public void run() {
        System.out.println("Shutdown hook.");
      }
    }));

    Thread.sleep(1000 * 1000);
  }

  public class ShutdownSignal implements SignalHandler {

    public ShutdownSignal() {
      Signal signal = new Signal("TERM");
      Signal.handle(signal, this);
    }

    @Override
    public void handle(Signal signal) {
      System.out.println("Trigger signal of " + signal.getName());
      if (signal.getName().equals("SIGTERM")) {
        System.out.println("accept SIGTERM signal.");
        System.exit(1);
      }
      System.out.println("Finished.");
      System.exit(1);
    }
  }
}
