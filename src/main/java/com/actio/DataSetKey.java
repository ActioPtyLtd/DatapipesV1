package com.actio;

import java.util.Date;
import java.util.UUID;

/**
 * Created by jim on 21/03/2016.
 */
public class DataSetKey {

    UUID batchID;
    int chunkStart=0;
    int chunkEnd=0;
    int maxChunk=0;
    Date created = new Date();

    public DataSetKey()
    {
        batchID = UUID.randomUUID();
    }

    public DataSetKey(UUID _batchID, int _chunkStart, int _chunkEnd, int _maxChunk)
    {
        batchID = _batchID;
        chunkStart = _chunkStart;
        chunkEnd = _chunkEnd;
        maxChunk = _maxChunk;
    }

    public String getKey()
    {
        return null;
    }

    public boolean equals(String inKey)
    {
        return false;
    }

    public boolean equals(DataSetKey inKey)
    {
        return false;
    }


}
