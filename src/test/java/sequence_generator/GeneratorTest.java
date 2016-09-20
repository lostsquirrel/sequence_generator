package sequence_generator;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class GeneratorTest {
	
	public static void main(String[] args) {

		testSingle();
		testMultThread();
		
		testGSpeed();
	}
	
	private static void testSingle() {
		System.out.println(Thread.currentThread().getName());
//		System.out.println(SequenceGenerator.generate("test"));
		System.out.println(DBSequenceGenerator.generate());
		System.out.println("finished");
	}
	
	private static void testMultThread() {
		ThreadPoolExecutor executor  = (ThreadPoolExecutor) Executors.newCachedThreadPool();
//		System.out.println(String.format("%s:%S", Thread.currentThread().getName(), SequenceGenerator.generate("test")));
//		System.out.println(Thread.currentThread().getName());
//		System.out.println(SequenceGenerator.generate("test"));
//		System.out.println("123");
		for (int i = 0; i < 5; i++) {
			
			executor.execute(new Runnable(){
				public void run() {
//					System.out.println(String.format("%s:\t%S", Thread.currentThread().getName(), SequenceGenerator.generate("test")));
//					System.out.println(String.format("%s:\t%S", Thread.currentThread().getName(), SequenceGenerator.generate("test")));
//					System.out.println(String.format("%s:\t%S", Thread.currentThread().getName(), SequenceGenerator.generate("test")));
					System.out.println(String.format("%s:\t%S", Thread.currentThread().getName(), DBSequenceGenerator.generate()));
					System.out.println(String.format("%s:\t%S", Thread.currentThread().getName(), DBSequenceGenerator.generate()));
					System.out.println(String.format("%s:\t%S", Thread.currentThread().getName(), DBSequenceGenerator.generate()));
				}
			});
		}
		
		executor.shutdown();
	}
	
	private static void testGSpeed() {
		System.out.println("start at: " + DBSequenceGenerator.generate());
		long s = System.currentTimeMillis() + 1000;
		
		while(System.currentTimeMillis() < s) {
			DBSequenceGenerator.generate();
		}
		System.out.println("finish at: " + DBSequenceGenerator.generate());
	}
}
