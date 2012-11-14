package de.me.pooling;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.Collections;
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


/**
 * {@link SimpleExecutorService} implementation using a thread pool.<br><br>
 *
 * This executor is trying to execute commands immediately using up to {@link #setMaxPoolSize(int) maxPoolSize} threads.<br>
 * If the pool cannot be further extended then the commands are queued up to {@link #setMaxQueuedTasks(int) maxQueuedTasks}.<br>
 * If even the queue is full then the execute methods will block the calling thread.<br><br>
 *
 * <b>Note</b> that there is no explicit start method since the executor is started with the first command to execute.<br>
 * <b>Nevertheless</b> once the executor was shut down, it cannot be used again.
 */
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

	/**
	 * Sets the thread group to use for pool threads.
	 *
	 * @param threadGroup the new thread group
	 * @throws IllegalStateException if pool threads exist currently
	 */
	public void setThreadGroup(ThreadGroup threadGroup) {
		if (threadGroup == null) throw new IllegalArgumentException("Thread group required");

		taskQueueLock.lock();
		try {
			if (totalThreads > 0) {
				throw new IllegalStateException("Cannot set thread group with active pool threads");
			}

			this.threadGroup = threadGroup;
		}
		finally {
			taskQueueLock.unlock();
		}
	}

	/**
	 * Sets the thread group name, defaults to this class name.<br>
	 * <b>Note</b> that this will create a new thread group with the given name.
	 *
	 * @param name the new thread group name
	 * @throws IllegalStateException if pool threads exist currently
	 */
	public void setThreadGroupName(String name) {
		setThreadGroup(new ThreadGroup(name));
	}

	private volatile String threadName = PoolThread.class.getSimpleName();

	/**
	 * Sets the pool thread base name, defaults to name of class {@link PoolThread}.<br>
	 * This name is extended with a dash and number for an actual pool thread.
	 *
	 * @param threadName the new thread base name
	 */
	public void setThreadName(String threadName) {
		if (threadName == null) throw new IllegalArgumentException("Thread name required");
		this.threadName = threadName;
	}

	private volatile boolean threadDaemon = false;

	/**
	 * Set to <code>true</code> if pool threads should be {@link Thread#setDaemon(boolean) daemon threads}, defaults to <code>false</code>.
	 */
	public void setThreadDaemon(boolean threadDaemon) {
		this.threadDaemon = threadDaemon;
	}

	private volatile int threadPriority = Thread.NORM_PRIORITY;

	/**
	 * Sets the {@link Thread#setPriority(int) thread priority}, defaults to {@link Thread#NORM_PRIORITY}.
	 *
	 * @param threadPriority the new thread priority
	 */
	public void setThreadPriority(int threadPriority) {
		this.threadPriority = threadPriority;
	}

	private volatile int corePoolSize = 0;

	/**
	 * Sets the core pool size, defaults to <code>0</code>.<br>
	 * Once pool threads reach this level, it will be retained (core threads do not {@link #setThreadTimeout(long, TimeUnit) time out}).
	 *
	 * @param corePoolSize the new core pool size
	 * @throws IllegalStateException if executor already shut down
	 */
	public void setCorePoolSize(int corePoolSize) {
		if (corePoolSize < 0) throw new IllegalArgumentException("Invalid core pool size");

		taskQueueLock.lock();
		try {
			checkNotShutdown();
			this.corePoolSize = corePoolSize;
		}
		finally {
			taskQueueLock.unlock();
		}
	}

	private volatile int maxPoolSize = 10;

	/**
	 * Sets the maximum pool size, defaults to <code>10</code>.<br>
	 * Pool threads can {@link #setThreadTimeout(long, TimeUnit) time out} until the {@link #setCorePoolSize(int) core level} is reached.
	 *
	 * @param maxPoolSize the new max pool size
	 * @throws IllegalStateException if executor already shut down
	 */
	public void setMaxPoolSize(int maxPoolSize) {
		if (maxPoolSize < 1) throw new IllegalArgumentException("Invalid maximum pool size");

		taskQueueLock.lock();
		try {
			checkNotShutdown();
			this.maxPoolSize = maxPoolSize;
		}
		finally {
			taskQueueLock.unlock();
		}
	}

	private volatile long threadTimeout = 60000L;
	private volatile TimeUnit threadTimeoutUnit = TimeUnit.MILLISECONDS;

	/**
	 * Sets the pool thread timeout, defaults to <code>60000 ms</code>.<br>
	 * Only if thread level is higher than the {@link #setCorePoolSize(int) core level} then idle threads will stop.
	 *
	 * @param timeout the timeout
	 * @param unit the timeout unit
	 * @throws IllegalStateException if executor already shut down
	 */
	public void setThreadTimeout(long timeout, TimeUnit unit) {
		if (unit == null) throw new IllegalArgumentException("Timeout unit required");

		taskQueueLock.lock();
		try {
			checkNotShutdown();
			threadTimeout = Math.max(timeout, 0L);
			threadTimeoutUnit = unit;
		}
		finally {
			taskQueueLock.unlock();
		}
	}

	/**
	 * Sets the pool thread timeout in milliseconds.
	 *
	 * @param timeout the new thread timeout in ms
	 * @throws IllegalStateException if executor already shut down
	 * @see #setThreadTimeout(long, TimeUnit)
	 */
	public void setThreadTimeoutMillis(long timeout) {
		setThreadTimeout(timeout, TimeUnit.MILLISECONDS);
	}

	private int maxQueuedTasks = Integer.MAX_VALUE;

	/**
	 * Sets the limit of queued commands, defaults to {@link Integer#MAX_VALUE}.<br>
	 * This limit does <i>not</i> include executing commands.
	 *
	 * @param maxQueuedTasks the new limit
	 * @throws IllegalStateException if executor already shut down
	 */
	public void setMaxQueuedTasks(int maxQueuedTasks) {
		if (maxQueuedTasks < 0) throw new IllegalArgumentException("Invalid max queued tasks value");

		taskQueueLock.lock();
		try {
			checkNotShutdown();
			this.maxQueuedTasks = maxQueuedTasks;
		}
		finally {
			taskQueueLock.unlock();
		}
	}

	private volatile Thread.UncaughtExceptionHandler uncaughtExceptionHandler = new UncaughtExceptionHandler() {
		@Override
		public void uncaughtException(Thread t, Throwable e) {
			log.error("Uncaught command exception", e);
		}
	};

	/**
	 * Sets the uncaught exception handler for pool threads.<br>
	 * By default the errors will be logged in error level.
	 *
	 * @param uncaughtExceptionHandler the new handler
	 */
	public void setUncaughtExceptionHandler(UncaughtExceptionHandler uncaughtExceptionHandler) {
		if (uncaughtExceptionHandler == null) throw new IllegalArgumentException("Handler required");

		taskQueueLock.lock();
		try {
			this.uncaughtExceptionHandler = uncaughtExceptionHandler;
		}
		finally {
			taskQueueLock.unlock();
		}
	}


	/**
	 * {@inheritDoc}
	 */
	@Override
	public void shutdown() {
		if (isShutdown()) return;

		taskQueueLock.lock();
		try {
			if (!setState(STATE_SHUTDOWN)) return;
			log.debug("Shutting down executor");

			if (totalThreads > 0) {
				threadTimeout = 0L;
				threadTimeoutUnit = TimeUnit.NANOSECONDS;

				corePoolSize = 0;

				log.trace("Let remaining threads finish: {}", totalThreads);
				taskQueueNotEmpty.signalAll();
			}
			else {
				log.trace("No active threads");
				doTerminate();
			}
		}
		finally {
			taskQueueLock.unlock();
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public List<Runnable> shutdownNow() {
		shutdown();

		if (isShutdown()) return Collections.emptyList();

		List<Runnable> remainingTasks;

		taskQueueLock.lock();
		try {
			if (isShutdown()) return Collections.emptyList();

			remainingTasks = new ArrayList<Runnable>(taskQueue);
			taskQueue.clear();

			threadGroup.interrupt();
			if (log.isTraceEnabled()) log.trace("Shut down now, drained threads: {}", remainingTasks.size());
		}
		finally {
			taskQueueLock.unlock();
		}

		return remainingTasks;
	}

	/**
	 * Throws {@link IllegalStateException} if executor is in shut down.
	 */
	private void checkNotShutdown() {
		if (isShutdown()) throw new IllegalStateException("Executor already shut down");
	}

	/**
	 * Checks if state is active.
	 */
	private boolean isState(int state) {
		return((this.state.get() & state) != 0);
	}

	/**
	 * Activates a state.
	 *
	 * @param state the state to activate
	 * @return true, if successfully set, false if state was already active
	 */
	private boolean setState(int state) {
		for (;;) {
			int currentState = this.state.get();

			if ((currentState & state) != 0) return false;

			if (this.state.compareAndSet(currentState, currentState | state)) return true;
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isShutdown() {
		return isState(STATE_SHUTDOWN);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isTerminated() {
		return isState(STATE_TERMINATED);
	}

	/**
	 * Terminates this executor.
	 */
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

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
		if (isTerminated()) return true;

		terminateLock.lockInterruptibly();
		try {
			if (isTerminated()) return true;
			if (timeout <= 0L) return false;

			log.debug("Awaiting termination for {} {}", timeout, unit);
			boolean result = terminateCondition.await(timeout, unit);
			log.debug("Awaited termination: {}", result);
			return result;
		}
		finally {
			terminateLock.unlock();
		}
	}



	/**
	 * {@inheritDoc}
	 */
	@Override
	public void execute(final Runnable command)
	throws ExecutionAwaitInterruptedException {
		try {
			execute(-1L, TimeUnit.NANOSECONDS, command);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new ExecutionAwaitInterruptedException(e);
		}
	}


	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean execute(final long timeout, final TimeUnit unit, final Runnable command)
	throws InterruptedException {
		if (command == null) throw new IllegalArgumentException("Command required");
		if (unit == null) throw new IllegalArgumentException("Timeout unit required");

		checkNotShutdown();


		long remainingWaitTime = unit.toNanos(timeout);

		taskQueueLock.lockInterruptibly();
		try {
			log.debug("Execute new command with timeout {} ns", remainingWaitTime);

			do {
				// Try to use idle thread
				if (executeWithIdleThread(command)) {
					log.debug("Executing with idle thread");
					return true;
				}

				// Try to create new pool thread
				if (executeWithNewThread(command)) {
					log.debug("Executing with newly created thread");
					return true;
				}

				// Try to put command into task queue
				remainingWaitTime = addToTaskQueue(command, timeout < 0 ? timeout : remainingWaitTime);
			}
			while (remainingWaitTime > 0L);

			return false;
		}
		finally {
			taskQueueLock.unlock();
		}
	}


	/**
	 * Try to execute command with an idle pool thread.
	 *
	 * @param command the command
	 * @return true, if successful
	 */
	private boolean executeWithIdleThread(Runnable command) {
		PoolThread thread = threadQueue.poll();

		if (thread == null) {
			log.trace("Have no idle thread");
			return false;
		}
		log.trace("Using idle thread {}", thread);

		thread.setNextCommand(command);

		taskQueueNotEmpty.signalAll();
		return true;
	}

	/**
	 * Try to execute command with a new pool thread.
	 *
	 * @param command the command
	 * @return true, if successful
	 */
	private boolean executeWithNewThread(Runnable command) {
		if (totalThreads >= maxPoolSize) {
			log.trace("Cannot create new thread, maximum of {} reached: {}", maxPoolSize, totalThreads);
			return false;
		}

		PoolThread thread = createNewThread(command);
		totalThreads++;
		log.trace("Created new thread: {} (total: {})", thread, totalThreads);

		thread.start();
		return true;
	}

	/**
	 * Creates a new pool thread.
	 *
	 * @param command the pool task to execute in the thread
	 * @return the pool thread
	 */
	protected PoolThread createNewThread(Runnable command) {
		PoolThread thread = new PoolThread(command, threadGroup, threadName+"-"+(totalThreads + 1));
		thread.setDaemon(threadDaemon);
		thread.setPriority(threadPriority);
		return thread;
	}

	/**
	 * Try to add the command to task queue.
	 *
	 * @param command the command
	 * @param timeout the waiting timeout
	 * @return the remaining time to wait in next try
	 */
	private long addToTaskQueue(Runnable command, long timeout) throws InterruptedException {
		if (taskQueue.size() < maxQueuedTasks) {
			taskQueue.add(command);
			if (log.isTraceEnabled()) log.trace("Command added to task queue ({} of {})", taskQueue.size(), maxQueuedTasks);
			return 0L;
		}
		log.trace("No space left in task queue (max: {})", maxQueuedTasks);

		if (timeout == 0L) {
			log.trace("Don't wait for task queue");
			return 0L;
		}

		if (timeout > 0L) {
			log.trace("Waiting for task queue: {} ns", timeout);
			long remaining = taskQueueNotFull.awaitNanos(timeout);
			log.trace("Waited for task queue, remaining: {} ns", remaining);
			return remaining;
		}

		log.trace("Waiting for task queue until space is free");
		taskQueueNotFull.await();
		log.trace("Waited for task queue");
		return 1L;
	}



	/**
	 * A pool thread.
	 */
	protected class PoolThread extends Thread {

		private final Logger log = LoggerFactory.getLogger(getClass());

		private final AtomicReference<Runnable> command;


		/**
		 * Instantiates a new pool thread.
		 *
		 * @param command the command to execute immediately after start
		 * @param group the pool's thread group
		 * @param name the thread name
		 */
		public PoolThread(Runnable command, ThreadGroup group, String name) {
			super(group, name);
			this.command = new AtomicReference<Runnable>(command);
		}


		/**
		 * Sets the next command to execute.
		 *
		 * @param command the next command
		 * @throws IllegalStateException if a command is still set to execute
		 */
		public void setNextCommand(Runnable command) {
			if (!this.command.compareAndSet(null, command)) {
				throw new IllegalStateException("Cannot set command when other command present");
			}
		}


		/**
		 * In a loop executes the set command and waits for new command, optionally timing out.
		 */
		@Override
		public void run() {
			log.trace("Pool thread ready to start");

			try {
				Runnable command = this.command.getAndSet(null);

				do {
					executeCommand(command);


					taskQueueLock.lockInterruptibly();
					try {
						command = tryGetNextCommand();
						if (command != null) continue;

						command = waitForNextCommand();
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

			log.trace("Pool thread exited");
		}


		/**
		 * Executes the given command.
		 */
		private void executeCommand(Runnable command) {
			try {
				log.debug("Executing task");

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
		}


		/**
		 * Tries to get next command, otherwise putting itself into the idle thread pool.
		 *
		 * @return the next command or <code>null</code> if no command available
		 */
		private Runnable tryGetNextCommand() {
			final Runnable command = taskQueue.poll();

			if (command == null) {
				threadQueue.addFirst(this);
				if (log.isDebugEnabled()) log.debug("No immediate tasks, free threads now {}", threadQueue.size());
			}
			else {
				taskQueueNotFull.signalAll();
				if (log.isDebugEnabled()) log.debug("Got task from task queue, remaining {}", taskQueue.size());
			}

			return command;
		}


		/**
		 * Waits for the next command.
		 *
		 * @return the next command or <code>null</code> if no command available
		 */
		private Runnable waitForNextCommand() throws InterruptedException {
			Runnable command;
			long waittime;

			for (;;) {
				waittime = threadTimeoutUnit.toNanos(threadTimeout);

				for (;;) {
					log.trace("Waiting for {} ns", waittime);

					waittime = taskQueueNotEmpty.awaitNanos(waittime);

					command = this.command.getAndSet(null);
					if (command != null) {
						log.debug("Have new command");
						return command;
					}

					if (isShutdown()) {
						log.debug("Shutdown detected");

						finishPoolThread();
						return null;
					}

					if (waittime <= 0L) {
						log.debug("Waited");

						if (totalThreads > corePoolSize) {
							finishPoolThread();
							return null;
						}
					}
				}
			}
		}


		/**
		 * Finishes this pool thread by removing it from the idle thread pool.
		 */
		private void finishPoolThread() {
			threadQueue.remove(this);
			totalThreads--;

			log.debug("Pool thread exit, remaining threads: {}", totalThreads);

			if (totalThreads == 0) {
				doTerminate();
			}
		}

	}

}
