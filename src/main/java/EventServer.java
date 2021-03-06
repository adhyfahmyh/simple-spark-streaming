import java.io.*;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;
import java.util.concurrent.*;

public class EventServer {
    private static final Executor SERVER_EXECUTOR = Executors.newSingleThreadExecutor();
    private static final Integer PORT = 9999;
    private static final String DELIMITER = ":";
    private static final long EVENT_PERIOD_SECONDS = 1;
    private static final Random random = new Random();

    public static void main(String[] args) throws IOException, InterruptedException {
        BlockingQueue<String> eventQueue = new ArrayBlockingQueue<>(100);
        SERVER_EXECUTOR.execute(new StreamingServer(eventQueue));
        while (true){
            eventQueue.put(generateEvent());
            Thread.sleep(TimeUnit.SECONDS.toMillis(EVENT_PERIOD_SECONDS));
        }
    }

    private static String generateEvent(){
        Integer userNumber = random.nextInt(10);
        String event = random.nextBoolean()?"login":"purchase";
        return String.format("user-%s",userNumber) + DELIMITER + event;
    }

    private static class StreamingServer implements Runnable{
        private final BlockingQueue<String> eventQueue;

        public StreamingServer(BlockingQueue<String> eventQueue) {
            this.eventQueue = eventQueue;
        }

        @Override
        public void run() {
            try (ServerSocket serverSocket = new ServerSocket(PORT);
                 Socket clientSocket = serverSocket.accept();
                 PrintWriter out = new PrintWriter(clientSocket.getOutputStream(),true)
            ){
                while (true){
                    String event = eventQueue.take();
                    System.out.println(String.format("Writing \"%s\" to the socket.", event));
                    out.println(event);
                }
            }catch (InterruptedException | IOException e) {
                throw new RuntimeException("Server error", e);
            }
        }
    }
}
