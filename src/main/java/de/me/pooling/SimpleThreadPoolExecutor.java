package de.me.pooling;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.me.pooling.exception.ExecutionAwaitInterruptedException;


public class SimpleThreadPoolExecutor extends AbstractExecutorService implements SimpleExecutorService {

	private static final int STATE_SHUTDOWN = 1;
	private static final int STATE_TERMINATED = 2;

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final Lock terminateLock = new ReentrantLock();
	private final Condition terminateCondition = terminateLock.newCondition();

	private final Lock taskQueueLock = new ReentrantLock(true);
	private final Condition taskQueueNotEmpty = taskQueueLock.newCondition();
	private final Condition taskQueueNotFull = taskQueueLock.newCondition();

	private final Deque<PoolThread> threadQueue = new LinkedList<PoolThread>();
	private final Queue<Runnable> taskQueue = new LinkedList<Runnable>();

	private final AtomicInteger state = new AtomicInteger(0);

	private volatile int totalThreads = 0;

	private volatile ThreadGroup threadGroup = new ThreadGroup(getClass().getSimpleName());

	public void setThreadGroup(ThreadGroup threadGroup) {
		if (threadGroup == null) throw new IllegalArgumentException("Thread group required");
		this.threadGroup = threadGroup;
	}

	public void setThreadGroupName(String name) {
		setThreadGroup(new ThreadGroup(name));
	}

	private volatile String threadName = PoolThread.class.getSimpleName();

	public void setThreadName(String threadName) {
		if (threadName == null) throw new IllegalArgumentException("Thread name required");
		this.threadName = threadName;
	}

	private volatile boolean threadDaemon = false;

	public void setThreadDaemon(boolean threadDaemon) {
		this.threadDaemon = threadDaemon;
	}

	private volatile int threadPriority = Thread.NORM_PRIORITY;

	public void setThreadPriority(int threadPriority) {
		this.threadPriority = threadPriority;
	}

	private volatile int corePoolSize = 0;

	public void setCorePoolSize(int corePoolSize) {
		if (corePoolSize < 0) throw new IllegalArgumentException("Invalid core pool size");
		checkNotShutdown();
		this.corePoolSize = corePoolSize;
	}

	private volatile int maxPoolSize = 10;

	public void setMaxPoolSize(int maxPoolSize) {
		if (maxPoolSize < 1) throw new IllegalArgumentException("Invalid maximum pool size");
		checkNotShutdown();
		this.maxPoolSize = maxPoolSize;
	}

	private volatile long threadTimeout = 60000L;
	private volatile TimeUnit threadTimeoutUnit = TimeUnit.MILLISECONDS;

	public void setThreadTimeout(long timeout, TimeUnit unit) {
		if (unit == null) throw new IllegalArgumentException("Timeout unit required");
		checkNotShutdown();
		threadTimeout = Math.max(timeout, 0L);
		threadTimeoutUnit = unit;
	}

	public void setThreadTimeoutMillis(long timeout) {
		setThreadTimeout(timeout, TimeUnit.MILLISECONDS);
	}

	private int maxQueuedTasks = Integer.MAX_VALUE;

	public void setMaxQueuedTasks(int maxQueuedTasks) {
		this.maxQueuedTasks = maxQueuedTasks;
	}

	private volatile Thread.UncaughtExceptionHandler uncaughtExceptionHandler = new UncaughtExceptionHandler() {
		@Override
		public void uncaughtException(Thread t, Throwable e) {
			log.error("Uncaught command exception", e);
		}
	};

	public void setUncaughtExceptionHandler(UncaughtExceptionHandler uncaughtExceptionHandler) {
		if (uncaughtExceptionHandler == null) throw new IllegalArgumentException("Handler required");
		this.uncaughtExceptionHandler = uncaughtExceptionHandler;
	}


	@Override
	public void shutdown() {
		taskQueueLock.lock();
		try {
			if (!setState(STATE_SHUTDOWN)) return;

			if (totalThreads > 0) {
				threadTimeout = 0L;
				threadTimeoutUnit = TimeUnit.NANOSECONDS;

				corePoolSize = 0;

				taskQueueNotEmpty.signalAll();
			}
			else {
				doTerminate();
			}
		}
		finally {
			taskQueueLock.unlock();
		}
	}

	@Override
	public List<Runnable> shutdownNow() {
		shutdown();

		List<Runnable> remainingTasks;

		taskQueueLock.lock();
		try {
			remainingTasks = new ArrayList<Runnable>(taskQueue);
			taskQueue.clear();

			threadGroup.interrupt();
		}
		finally {
			taskQueueLock.unlock();
		}

		return remainingTasks;
	}

	private void checkNotShutdown() {
		if (isShutdown()) throw new IllegalStateException("Executor already shut down");
	}

	private boolean isState(int state) {
		return((this.state.get() & state) != 0);
	}

	private boolean setState(int state) {
		for (;;) {
			int currentState = this.state.get();

			if ((currentState & state) != 0) return false;

			if (this.state.compareAndSet(currentState, currentState | state)) return true;
		}
	}

	@Override
	public boolean isShutdown() {
		return isState(STATE_SHUTDOWN);
	}

	@Override
	public boolean isTerminated() {
		return isState(STATE_TERMINATED);
	}

	private void doTerminate() {
		terminateLock.lock();
		try {
			if (!setState(STATE_TERMINATED)) return;

			terminateCondition.signalAll();
		}
		finally {
			terminateLock.unlock();
		}
	}

	@Override
	public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
		terminateLock.lockInterruptibly();
		try {
			if (isTerminated()) return true;
			if (timeout <= 0L) return false;

			return terminateCondition.await(timeout, unit);
		}
		finally {
			terminateLock.unlock();
		}
	}



	@Override
	public void execute(final Runnable command)
	throws ExecutionAwaitInterruptedException {
		try {
			execute(command, -1L, TimeUnit.MILLISECONDS);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new ExecutionAwaitInterruptedException(e);
		}
	}


	@Override
	public boolean execute(final Runnable command, long timeout, TimeUnit unit)
	throws InterruptedException {
		if (command == null) throw new IllegalArgumentException("Command required");
		if (unit == null) throw new IllegalArgumentException("Timeout unit required");

		checkNotShutdown();


		long remainingWaitTime = unit.toNanos(timeout);

		taskQueueLock.lockInterruptibly();
		try {
			do {
				// Try to use idle thread
				if (executeWithIdleThread(command)) {
					return true;
				}

				// Try to create new pool thread
				if (executeWithNewThread(command)) {
					return true;
				}

				// Put command into task queue
				remainingWaitTime = addToTaskQueue(command, timeout < 0 ? timeout : remainingWaitTime);
			}
			while (remainingWaitTime > 0L);

			return false;
		}
		finally {
			taskQueueLock.unlock();
		}
	}


	private boolean executeWithIdleThread(Runnable command) {
		PoolThread thread = threadQueue.poll();

		if (thread == null) return false;

		thread.setNextCommand(command);

		taskQueueNotEmpty.signalAll();
		return true;
	}

	private boolean executeWithNewThread(Runnable command) {
		if (totalThreads >= maxPoolSize) return false;

		PoolThread thread = createNewThread(command);
		totalThreads++;

		thread.start();
		return true;
	}

	protected PoolThread createNewThread(Runnable command) {
		PoolThread thread = new PoolThread(command, threadGroup, threadName);
		thread.setDaemon(threadDaemon);
		thread.setPriority(threadPriority);
		return thread;
	}

	private long addToTaskQueue(Runnable command, long timeout) throws InterruptedException {
		if (taskQueue.size() < maxQueuedTasks) {
			taskQueue.add(command);
			return 0L;
		}

		if (timeout == 0L) return 0L;

		if (timeout > 0L) {
			return taskQueueNotFull.awaitNanos(timeout);
		}

		taskQueueNotFull.await();
		return 1L;
	}



	protected class PoolThread extends Thread {

		private final Logger log = LoggerFactory.getLogger(getClass());

		private final AtomicReference<Runnable> command;


		public PoolThread(Runnable command, ThreadGroup group, String name) {
			super(group, name);
			this.command = new AtomicReference<Runnable>(command);
		}


		public void setNextCommand(Runnable command) {
			if (!this.command.compareAndSet(null, command)) {
				throw new IllegalStateException("Cannot set command when other command present");
			}
		}



		@Override
		public void run() {
			try {
				Runnable command = this.command.get();

				do {
					log.debug("Executing task");

					try {
						command.run();

						log.debug("Task finished successfully");
					}
					catch (Throwable e) {
						log.debug("Task execution failed", e);

						try {
							uncaughtExceptionHandler.uncaughtException(this, e);
						}
						catch (Throwable ee) {
							log.error("Uncaught exception handler failed", ee);
						}
					}


					taskQueueLock.lockInterruptibly();
					try {
						command = taskQueue.poll();
						if (command != null) {
							if (log.isDebugEnabled()) {
								log.debug("Got task from task queue, remaining {}", taskQueue.size());
							}

							taskQueueNotFull.signalAll();
							continue;
						}

						threadQueue.addFirst(this);
						if (log.isDebugEnabled()) {
							log.debug("Free threads now {}", threadQueue.size());
						}

						for (;;) {
							long waittime = threadTimeoutUnit.toNanos(threadTimeout);

							do {
								log.trace("Waiting for {} ns", waittime);

								waittime = taskQueueNotEmpty.awaitNanos(waittime);
							}
							while (waittime > 0L && (command = this.command.get()) == null && !isShutdown());


							if (command == null) {
								log.debug("Waited");

								if (isShutdown() || totalThreads > corePoolSize) {
									threadQueue.remove(this);
									totalThreads--;

									log.debug("Pool thread exit, remaining threads: {}", totalThreads);

									if (totalThreads == 0) {
										doTerminate();
									}
									break;
								}
							}
							else log.debug("Have new command");
						}
					}
					finally {
						taskQueueLock.unlock();
					}
				}
				while (command != null);
			}
			catch (InterruptedException e) {
				log.debug("Interrupted", e);
			}
		}

	}

}
