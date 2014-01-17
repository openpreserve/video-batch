package at.ac.ait.dme.video_batch.hadoop20workaround;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.io.compress.CompressionInputStream;

/**
 * Workaround for Hadoop 0.20
 * This class exists in Hadoop 0.21 and is copied from there.
 */
public abstract class SplitCompressionInputStream
    extends CompressionInputStream {

  private long start;
  private long end;

  public SplitCompressionInputStream(InputStream in, long start, long end)
      throws IOException {
    super(in);
    this.start = start;
    this.end = end;
  }

  protected void setStart(long start) {
    this.start = start;
  }

  protected void setEnd(long end) {
    this.end = end;
  }

  /**
   * After calling createInputStream, the values of start or end
   * might change.  So this method can be used to get the new value of start.
   * @return The changed value of start
   */
  public long getAdjustedStart() {
    return start;
  }

  /**
   * After calling createInputStream, the values of start or end
   * might change.  So this method can be used to get the new value of end.
   * @return The changed value of end
   */
  public long getAdjustedEnd() {
    return end;
  }
}
