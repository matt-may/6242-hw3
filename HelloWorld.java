import java.util.*;
import java.io.IOException;

public class HelloWorld {

    public static void main(String[] args) {
        // Prints "Hello, World" to the terminal window.
        List<String> outNodes = new ArrayList<String>();

        outNodes.add("1:0");
        outNodes.add("2:1");
        outNodes.add("3:1");

        for (String outNode : outNodes) {
            String[] split = outNode.split(":");

            System.out.println(split[1]);
        }
    }

}