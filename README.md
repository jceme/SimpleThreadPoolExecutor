SimpleThreadPoolExecutor
============

A simple thread pool executor using the strategy trying to execute commands first, else enqueue them, else blocking the calling thread.

This executor is trying to execute commands immediately using up to maxPoolSize threads.
If the pool cannot be further extended then the commands are queued up to maxQueuedTasks.
If even the queue is full then the execute methods will block the calling thread.

The SimpleThreadPoolExecutor is compatible with ExecutorService and hence compatible with Executor as well.

This class is intentionally compatible to Spring configuration.

```java
public class Example {

	// ...

	private SimpleExecutorService executor;
	
	
	/**
	 * Creates a new executor with some reasonable example settings.
	 */
	@PostConstruct
	public void init() {
		SimpleThreadPoolExecutor executor = new SimpleThreadPoolExecutor();
		
		executor.setMaxPoolSize(20);
		executor.setCorePoolSize(5);
		executor.setThreadTimeout(30L, TimeUnit.SECONDS);
		
		this.executor = executor;
	}
	
	/**
	 * Shutdown executor when done.
	 */
	@PreDestroy
	public void shutdown() {
		executor.shutdownNow();
	}
	
	
	public void runExample() {
		Executor executor = this.executor;
		
		executor.execute(new Runnable() {
			@Override
			public void run() {
				// Do some work here ...
			}
		});
	}
	
	public void runExampleWithWaitingTimeout() {
		boolean done = executor.execute(2000L, TimeUnit.MILLISECONDS, new Runnable() {
			@Override
			public void run() {
				// Do some work here ...
			}
		});
		
		if (!done) {
			log.info("Could not schedule job within two seconds");
			return;
		}
		// else job was scheduled for execution
	}
	
	
	// ...

}
```
