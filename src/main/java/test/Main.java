package test;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.Scanner;

public class Main {


    public static void main(String[] args) {
        ApplicationContext applicationContext =
                new ClassPathXmlApplicationContext(new String[]{"application-context.xml"});
        Engine engine = (Engine) applicationContext.getBean("engine");
        engine.put(1, new Data(1, "testValue"));
        System.out.println(engine.get(1));
        Scanner scanner = new Scanner(System.in);
        String operation = "";
        while (!"exit".equals(operation)) {

            System.out.println("Enter operation");
            operation = scanner.nextLine();
            if ("add".equals(operation)) {
                System.out.println("Enter key");
                Integer key = scanner.nextInt();
                scanner.nextLine();
                System.out.println("Enter value");

                String value = scanner.nextLine();
                Data data = new Data(key, value);
                engine.put(key, data);
            }
            if ("ins".equals(operation)) {
                System.out.println("Enter collection key");
                Integer collectionKey = scanner.nextInt();
                scanner.nextLine();
                System.out.println("Enter key");
                Integer key = scanner.nextInt();
                scanner.nextLine();
                System.out.println("Enter value");
                String value = scanner.nextLine();
                try {
                    engine.addOrReplaceCollectionValue(collectionKey, key, value);
                } catch (PutCacheException e) {
                    e.printStackTrace();
                }
            }
            if ("get".equals(operation)) {
                System.out.println("Enter key");
                Integer key = scanner.nextInt();
                scanner.nextLine();
                System.out.println(engine.get(key));
            }
            if ("localAny".equals(operation)) {
                engine.setLocalListenerAnyKey(new CacheUpdateListenerImpl());
                System.out.println("Added localAny");
            }
            if ("localCurr".equals(operation)) {
                System.out.println("Enter key");
                Integer key = scanner.nextInt();
                scanner.nextLine();
                engine.setLocalListenerCurrentKey(new CacheUpdateListenerImpl(), key);
            }
            if ("remoteAny".equals(operation)) {
                engine.setRemoteListenerAnyKey(new CacheUpdateListenerImpl());
                System.out.println("added remoteAny");
            }
            if ("remoteCurr".equals(operation)) {
                System.out.println("Enter key");
                Integer key = scanner.nextInt();
                scanner.nextLine();
                engine.setRemoteListenerCurrentKey(new CacheUpdateListenerImpl(), key);
            }
        }
    }
}
