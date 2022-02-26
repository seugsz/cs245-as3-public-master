package cs245.as3.interfaces;

public interface LogManager {
	// During testing, all methods of LogManager might throw
        // CrashException, which is a custom RuntimeException subclass we
        // are using for testing and simulating crashes. You are not expected to
	// catch any of these. Simply allow them to be caught by the test driver.

	/**
	 * @return the offset of the end of the log
	 * 返货log尾部的偏移量
	 */
	public int getLogEndOffset();

	/**
	 * Reads from log at the specified position.
	 * @return bytes in the log record in the range [offset, offset + size)
	 */
	public byte[] readLogRecord(int offset, int size);

	/**
	 * Atomically appends and persists record to the end of the log (implying that all previous appends have succeeded).
	 * @param record
	 * @return the log length prior to the append
	 *  返回append之前的日志长度
	 */
	public int appendLogRecord(byte[] record);

	/**
	 * @return the current log truncation offset
	 * 当前日志截断偏移量
	 */
	public int getLogTruncationOffset();

	/**
	 * Durably stores the offset as the current log truncation offset and truncates (deletes) the log up to that point.
	 * You can assume this occurs atomically. The test code will never call this.
	 * 持久地将offset存储为当前日志截断偏移量，并截断（删除）日志直至该点。
	 * 你可以假设这是原子性的。测试代码永远不会调用它。
	 */
	public void setLogTruncationOffset(int offset);
}
