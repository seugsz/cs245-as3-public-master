package cs245.as3;

import java.util.*;

import cs245.as3.interfaces.LogManager;
import cs245.as3.interfaces.StorageManager;
import cs245.as3.interfaces.StorageManager.TaggedValue;

/**
 * You will implement this class.
 * <p>
 * The implementation we have provided below performs atomic transactions but the changes are not durable.
 * Feel free to replace any of the data structures in your implementation, though the instructor solution includes
 * the same data structures (with additional fields) and uses the same strategy of buffering writes until commit.
 * <p>
 * Your implementation need not be threadsafe, i.e. no methods of TransactionManager are ever called concurrently.
 * <p>
 * You can assume that the constructor and initAndRecover() are both called before any of the other methods.
 */
public class TransactionManager {
    class WritesetEntry {
        public long key;
        public byte[] value;

        public WritesetEntry(long key, byte[] value) {
            this.key = key;
            this.value = value;
        }
    }

    /**
     * Holds the latest value for each key.
     * 保存每个键的最新值
     */
    private HashMap<Long, TaggedValue> latestValues;
    /**
     * Hold on to writesets until commit.
     * 保留writesets，直到提交。
     */
    private HashMap<Long, ArrayList<WritesetEntry>> writesets;

    private LogManager lm;
    private StorageManager sm;
    // key=txID，v=事务对应的logRecord
    private Map<Long, ArrayList<LogRecord>> logRecordSets;
    // 需要持久化的写事务对应的logRecord的尾部在logManger的偏移
    private PriorityQueue<Long> persistent;
    // 延迟删除最小堆里的 persistent_tag
    private Map<Long,Integer> deletedTag;


    public TransactionManager() {
        writesets = new HashMap<>();
        latestValues = null;
        logRecordSets = new HashMap<>();
        persistent = new PriorityQueue<Long>();
        deletedTag = new HashMap<>();
    }

    /**
     * Prepare the transaction manager to serve operations.
     * At this time you should detect whether the StorageManager is inconsistent and recover it.
     * 准备事务管理器，以服务于操作。此时，您应该检测StorageManager是否不一致，并将其恢复。
     */
    public void initAndRecover(StorageManager sm, LogManager lm) {
        latestValues = sm.readStoredTable();
        this.lm = lm;
        this.sm = sm;

        ArrayList<LogRecord> logRecords = new ArrayList<>();
        ArrayList<Long> tags = new ArrayList<>();
        Set<Long> txCommit = new HashSet<>();
        // 从截断点读取所有的日志记录
        long offset = lm.getLogTruncationOffset();
        while (offset < lm.getLogEndOffset()) {
            LogRecord logRecord = LogRecord.readLogRecord(lm, offset);
            logRecords.add(logRecord);
            offset += logRecord.getSize();
            tags.add(offset);
            // 把提交的事务的ID记录下来，
            if (logRecord.getType() == LogRecordType.commit) {
                txCommit.add(logRecord.getTxID());
            }
        }

        for (int i = 0; i < logRecords.size(); i++) {
            LogRecord logRecord = logRecords.get(i);
            if (txCommit.contains(logRecord.getTxID()) && logRecord.getType() == LogRecordType.write) {
                long tag = tags.get(i);
                latestValues.put(logRecord.getKey(), new TaggedValue(tag, logRecord.getValue()));
                sm.queueWrite(logRecord.getKey(), tag, logRecord.getValue());
                persistent.add(tag);
            }
        }
    }

    /**
     * Indicates the start of a new transaction. We will guarantee that txID always increases (even across crashes)
     * 指示新事务的开始。我们将保证txID始终增加（即使在发生碰撞时）
     */
    public void start(long txID) {
        // TODO: Not implemented for non-durable transactions, you should implement this
    }

    /**
     * Returns the latest committed value for a key by any transaction.
     * 返回任何事务为密钥提交的最新值。
     */
    public byte[] read(long txID, long key) {
        TaggedValue taggedValue = latestValues.get(key);
        return taggedValue == null ? null : taggedValue.value;
    }

    /**
     * Indicates a write to the database. Note that such writes should not be visible to read()
     * calls until the transaction making the write commits. For simplicity, we will not make reads
     * to this same key from txID itself after we make a write to the key.
     * 指示对数据库的写入。请注意，在进行写操作的事务提交之前，read（）调用不应看到此类写操作。
     * 为了简单起见，在对同一个密钥进行写入之后，我们不会从txID本身读取该密钥。
     */
    public void write(long txID, long key, byte[] value) {
        ArrayList<WritesetEntry> writeset = writesets.get(txID);
        if (writeset == null) {
            writeset = new ArrayList<>();
            writesets.put(txID, writeset);
        }
        writeset.add(new WritesetEntry(key, value));

        ArrayList<LogRecord> logRecordSet = logRecordSets.get(txID);
        if (logRecordSet == null) {
            logRecordSet = new ArrayList<>();
            logRecordSet.add(LogRecord.start(txID));
            logRecordSets.put(txID, logRecordSet);
        }
        logRecordSet.add(LogRecord.write(txID, key, value));
    }

    /**
     * Commits a transaction, and makes its writes visible to subsequent read operations.\
     * 提交事务，并使其写操作对后续读取操作可见。
     */
    public void commit(long txID) {
        ArrayList<LogRecord> logRecordSet = logRecordSets.get(txID);
        if (logRecordSet == null) {
            return;
        }
        logRecordSet.add(LogRecord.commit(txID));

        // 把事务txID的LogRecord写到LogManager中
        Map<Long, Long> keyToTag = new HashMap<>();
        for (LogRecord logRecord : logRecordSet) {
            long tag = LogRecord.writeLogRecord(this.lm, logRecord);
            if (logRecord.getType() == LogRecordType.write) {
                keyToTag.put(logRecord.getKey(), tag);
            }
        }
        // 持久化数据
        ArrayList<WritesetEntry> writeset = writesets.get(txID);
        if (writeset != null) {
            for (WritesetEntry x : writeset) {
                //tag是该条日志对应的尾部，在持久化的时候，调用writePersisted，就知道这条日志对应的数据已经被持久化
                long tag = keyToTag.get(x.key);
                latestValues.put(x.key, new TaggedValue(tag, x.value));
                sm.queueWrite(x.key, tag, x.value);
                persistent.add(tag);
            }
            logRecordSets.remove(txID);
            writesets.remove(txID);
        }
    }

    /**
     * Aborts a transaction.
     * 回滚事务
     */
    public void abort(long txID) {
        logRecordSets.remove(txID);
        writesets.remove(txID);
    }

    /**
     * The storage manager will call back into this procedure every time a queued write becomes persistent.
     * These calls are in order of writes to a key and will occur once for every such queued write, unless a crash occurs.
     * 每当队列写入变为持久时，存储管理器都会调用此过程。这些调用是按写入密钥的顺序进行的，除非发生崩溃，否则每一次这样的排队写入都会发生一次。
     */
    public void writePersisted(long key, long persisted_tag, byte[] persisted_value) {
        // persistent_tag是该条日志对应的尾部，在持久化的时候，说明这条日志对应的数据已经被持久化
        // 那么最小的persistent_tag之前的日志都可以被截断了
        // 如果persistent_tag等于最小的需要持久化的日志对应的数据，就可以作为截断点
        // 堆直接删除非堆顶元素很复杂，所以采用延迟删除，当某个元素需要从堆里删掉时，将其标记
        // 然后直到它是堆顶才删除
        long tag = -1;
        deletedTag.put(persisted_tag,deletedTag.getOrDefault(persisted_tag,0)+1);
        while (!persistent.isEmpty() && deletedTag.containsKey(persistent.peek())) {
            tag = persistent.poll();
            int temp=deletedTag.get(tag)-1;
            if(temp!=0){
                deletedTag.put(tag,temp);
            }else{
                deletedTag.remove(tag);
            }
        }
        if(tag!=-1){
            lm.setLogTruncationOffset((int) tag);
        }


//        if(persisted_tag==persistent.peek()){
//            lm.setLogTruncationOffset((int)persisted_tag);
//        }
//        persistent.remove(persisted_tag);
    }
}
