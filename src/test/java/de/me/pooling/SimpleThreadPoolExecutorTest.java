package de.me.pooling;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SimpleThreadPoolExecutorTest {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private SimpleThreadPoolExecutor executor;

	@Before
	public void setup() throws Exception {
		executor = new SimpleThreadPoolExecutor();
	}

	@After
	public void destroy() throws Exception {
		List<Runnable> rem = executor.shutdownNow();
		log.debug("Shutdown now remaining: {}", rem);
	}


	@Test(expected=IllegalArgumentException.class)
	public void testSetMaxPoolSizeZero() throws Exception {
		executor.setMaxPoolSize(0);
	}

	@Test
	public void testSetMaxPoolSizeOne() throws Exception {
		executor.setMaxPoolSize(1);
	}

	@Test
	public void testSetCorePoolSizeZero() throws Exception {
		executor.setCorePoolSize(0);
	}

	@Test
	public void testTaskQueueLengthZero() throws Exception {
		executor.setMaxQueuedTasks(0);
	}


	@Test(timeout=60000)
	public void testLiveTaskExecution() throws Exception {
		executor.setCorePoolSize(0);
		executor.setMaxPoolSize(1);
		executor.setMaxQueuedTasks(1);
		executor.setThreadTimeoutMillis(100);

		log.debug("Exec task 1");
		executor.execute(new Runnable() {
			@Override
			public void run() {
				log.debug("Task 1: Started");
				try {
					Thread.sleep(500);
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					log.error("Task 1: Interrupted", e);
				}
				log.debug("Task 1: Finished");
			}
		});

		log.debug("Exec task 2");
		executor.execute(new Runnable() {
			@Override
			public void run() {
				log.debug("Task 2: Started");
				try {
					Thread.sleep(500);
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					log.error("Task 2: Interrupted", e);
				}
				log.debug("Task 2: Finished");
			}
		});

		log.debug("Exec task 3");
		executor.execute(new Runnable() {
			@Override
			public void run() {
				log.debug("Task 3: Started");
				try {
					Thread.sleep(500);
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					log.error("Task 3: Interrupted", e);
				}
				log.debug("Task 3: Finished");
			}
		});

		log.debug("Shutting down");
		executor.shutdown();

		log.debug("Wait for termination");
		boolean term = executor.awaitTermination(6000, TimeUnit.MILLISECONDS);
		log.debug("Test finished: {}", term);
	}

}
