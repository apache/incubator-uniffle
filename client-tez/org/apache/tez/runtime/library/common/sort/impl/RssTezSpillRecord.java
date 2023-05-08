package org.apache.tez.runtime.library.common.sort.impl;

import java.io.IOException;
import java.util.zip.Checksum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;


public class RssTezSpillRecord extends TezSpillRecord {
    private int numPartitions;
    private int[] numRecordsPerPartition;

    public RssTezSpillRecord(int numPartitions) {
        super(numPartitions);
        this.numPartitions = numPartitions;
    }

    public RssTezSpillRecord(int numPartitions, int[] numRecordsPerPartition) {
        super(numPartitions);
        this.numPartitions = numPartitions;
        this.numRecordsPerPartition = numRecordsPerPartition;
    }


    public RssTezSpillRecord(Path indexFileName, Configuration job) throws IOException {
        super(indexFileName, job);
    }

    public RssTezSpillRecord(Path indexFileName, Configuration job, String expectedIndexOwner) throws IOException {
        super(indexFileName, job, expectedIndexOwner);
    }

    public RssTezSpillRecord(Path indexFileName, Configuration job, Checksum crc, String expectedIndexOwner)
            throws IOException {
        super(indexFileName, job, crc, expectedIndexOwner);
    }

    @Override
    public int size() {
        return numPartitions;
    }

    @Override
    public RssTezIndexRecord getIndex(int i) {
        int records = numRecordsPerPartition[i];
        RssTezIndexRecord rssTezIndexRecord = new RssTezIndexRecord();
        rssTezIndexRecord.setData(!(records == 0));
        return rssTezIndexRecord;
    }


    class RssTezIndexRecord extends TezIndexRecord {
        private boolean flag;

        private void setData(boolean flag) {
            this.flag = flag;
        }

        @Override
        public boolean hasData() {
            return flag;
        }
    }

}