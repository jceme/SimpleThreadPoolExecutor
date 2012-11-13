package de.me.pooling;

import static org.junit.Assert.assertEquals;

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
	public void testLiveTaskExecution1Thread() throws Exception {
		doTestLive(1);
	}

	@Test(timeout=60000)
	public void testLiveTaskExecution2Threads() throws Exception {
		doTestLive(2);
	}

	private void doTestLive(int max) throws Exception {
		executor.setCorePoolSize(0);
		executor.setMaxPoolSize(max);
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

		log.debug("Exec task 4, not to start");
		boolean done = executor.execute(100, TimeUnit.MILLISECONDS, new Runnable() {
			@Override
			public void run() {
				log.debug("Task 4: Started, but shouldn't");
			}
		});
		assertEquals(false, done);

		log.debug("Shutting down");
		executor.shutdown();

		log.debug("Wait for termination");
		boolean term = executor.awaitTermination(6000, TimeUnit.MILLISECONDS);
		log.debug("Test finished: {}", term);
	}

	@Test
	public void testLiveTaskExecutionTimeout() throws Exception {
		executor.setCorePoolSize(0);
		executor.setMaxPoolSize(2);
		executor.setMaxQueuedTasks(1);
		executor.setThreadTimeoutMillis(800);

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
					Thread.sleep(200);
				}
				catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					log.error("Task 2: Interrupted", e);
				}
				log.debug("Task 2: Finished");
			}
		});

		Thread.sleep(800);

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

		Thread.sleep(800);

		log.debug("Shutting down");
		executor.shutdown();

		log.debug("Wait for termination");
		boolean term = executor.awaitTermination(6000, TimeUnit.MILLISECONDS);
		log.debug("Test finished: {}", term);
	}

}
