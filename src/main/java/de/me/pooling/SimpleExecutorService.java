package de.me.pooling;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import de.me.pooling.exception.ExecutionAwaitInterruptedException;


/**
 * Extension of {@link ExecutorService} using blocked command queuing.
 */
public interface SimpleExecutorService extends ExecutorService {

	/**
	 * Executes the given command immediately or waits until it was added to the command queue.
	 *
	 * @param command the command to execute
	 *
	 * @throws ExecutionAwaitInterruptedException if calling thread was interrupted while waiting, thread will be re-interrupted
	 *   (for compatibility to {@link ExecutorService})
	 * @throws IllegalStateException if executor is already in shutdown or terminated
	 */
	@Override
	public void execute(Runnable command) throws ExecutionAwaitInterruptedException;


	/**
	 * Executes the given command immediately or waits the given timeout until it was added to the command queue.
	 *
	 * @param timeout the timeout to wait for enqueuing the command, negative value to block until it is enqueued
	 * @param unit the timeout unit
	 * @param command the command to execute
	 *
	 * @return true, if task was added to the command queue, false if timeout occurred before
	 *
	 * @throws InterruptedException if calling thread was interrupted
	 * @throws IllegalStateException if executor is already in shutdown or terminated
	 */
	public boolean execute(long timeout, TimeUnit unit, Runnable command) throws InterruptedException;

}
