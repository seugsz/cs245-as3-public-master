package cs245.as3;

import cs245.as3.interfaces.LogManager;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class LogRecord {
    // 头部长度
    public static final int headLen = 13;
    // LogManager最大的byte数组长度
    public static final int maxLen = 128;

    private int size;
    private byte type;
    private long txID;
    private long key;
    private byte[] value;

    public LogRecord(byte type, long txID) {
        this.type = type;
        this.txID = txID;
        this.size = headLen;
    }

    public LogRecord(byte type, long txID, long key, byte[] value) {
        this.type = type;
        this.txID = txID;
        this.key = key;
        this.value = value;
        this.size = headLen + 8 + value.length;
    }

    public int getSize() {
        return size;
    }

    public byte getType() {
        return type;
    }

    public long getTxID() {
        return txID;
    }

    public long getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }

    @Override
    public String toString() {
        if (this.type == LogRecordType.write) {
            return "LogRecord{" +
                    "size=" + size +
                    ", type=" + type +
                    ", txID=" + txID +
                    ", key=" + key +
                    ", value=" + Arrays.toString(value) +
                    '}';
        }
        return "LogRecord{" +
                "size=" + size +
                ", type=" + type +
                ", txID=" + txID +
                '}';
    }

    public static LogRecord start(long txID) {
        return new LogRecord(LogRecordType.start, txID);
    }

    public static LogRecord write(long txID, long key, byte[] value) {
        return new LogRecord(LogRecordType.write, txID, key, value);
    }

    public static LogRecord commit(long txID) {
        return new LogRecord(LogRecordType.commit, txID);
    }

    public static LogRecord rollback(long txID) {
        return new LogRecord(LogRecordType.rollback, txID);
    }

    public static long writeLogRecord(LogManager lm, LogRecord logRecord) {
        int sz = logRecord.getSize();
        ByteBuffer buffer = ByteBuffer.allocate(sz);
        buffer.putInt(sz);
        buffer.put(logRecord.getType());
        buffer.putLong(logRecord.getTxID());
        if (logRecord.getType() == LogRecordType.write) {
            buffer.putLong(logRecord.getKey());
            buffer.put(logRecord.getValue());
        }
        long tag = lm.getLogEndOffset();
        for (int i = 0; i < sz; i += maxLen) {
            int len = Math.min(sz - i, maxLen);
            byte[] temp = new byte[len];
            buffer.get(i, temp);
            lm.appendLogRecord(temp);
        }
        return tag+sz;
    }

    public static LogRecord readLogRecord(LogManager lm,long offset){
        byte[] head=lm.readLogRecord((int) offset,headLen);
        int sz=ByteUtils.BytesToUnsignedInt(head,0);
        byte type=head[4];
        long txID=ByteUtils.BytesToLong(head,5);
        if(type!=LogRecordType.write){
            return new LogRecord(type,txID);
        }

        int index=headLen;
        byte[] value=new byte[sz-headLen-8];

        int len=Math.min(sz-index,maxLen);
        byte[] data=lm.readLogRecord((int) (offset+index),len);
        long key=ByteUtils.BytesToLong(data,0);
        int k=0;
        System.arraycopy(data,8,value,k,len-8);
        k += len-8;
        index += len;

        while(k<value.length){
            len=Math.min(sz-index,maxLen);
            data=lm.readLogRecord((int) (offset+index),len);
            System.arraycopy(data,0,value,k,len);
            k += len;
            index += len;
        }
        return new LogRecord(type,txID,key,value);
    }

}
