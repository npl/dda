package dda.hadoop.osm;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec.READ_MODE;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class OsmRecordReader extends RecordReader<NullWritable,Text> {
	private final byte[] nodeStart;
	private final byte[] wayStart;
	private final byte[] relStart;
	private final byte slash;
	private final byte lessThan;
	private final byte greaterThan;
	
	private long startOffset;
	private long endOffset;
	private boolean useCompressedStream;
	
	
	private CompressionInputStream inputStream_compressed;
	private FSDataInputStream inputStream;
	private DataOutputBuffer buffer = new DataOutputBuffer();

	private Text currentValue;
	
	public OsmRecordReader() {
		nodeStart = Bytes.toBytes("<node ");
		wayStart = Bytes.toBytes("<way ");
		relStart = Bytes.toBytes("<relation ");
		
		slash = Bytes.toBytes("/")[0];
		lessThan = Bytes.toBytes("<")[0];
		greaterThan = Bytes.toBytes(">")[0];
	}

	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
		FileSplit fileSplit = (FileSplit)split;
		Path path = fileSplit.getPath();
		Configuration configuration = context.getConfiguration();

		startOffset = fileSplit.getStart();
		endOffset = startOffset + fileSplit.getLength();
		FileSystem fileSystem = path.getFileSystem(configuration);
		inputStream = fileSystem.open(fileSplit.getPath());
		
		if(path.getName().endsWith(".bz2"))
		{
			useCompressedStream=true;
			//Nur zu Testzwecken, falls Split zu klein wird er mehrmals verarbeitet:
			if(fileSplit.getLength()<15000)
			{
				throw new IOException("Fehler: Split vermutlich kleiner als Bockgröße: " + fileSplit.getLength());
			}
			BZip2Codec deCompressionCodec = new BZip2Codec();
			inputStream_compressed = deCompressionCodec.createInputStream(inputStream, null, startOffset, 0, READ_MODE.BYBLOCK);
		}
		else
		{
			useCompressedStream=false;
			inputStream.seek(startOffset);
		}
			
	}

	@Override
	public boolean nextKeyValue() throws IOException
	{
		if ((useCompressedStream ? inputStream_compressed.getPos() : inputStream.getPos()) < endOffset) {
			if (scanUntilStartTag()) {
				try {
					if (scanUntilEndTag()) {
						currentValue = new Text();
						currentValue.set(buffer.getData(), 0, buffer.getLength());
						return true;
					}
				} finally {
					buffer.reset();
				}
			}

		}
		return false;
	}

	public NullWritable getCurrentKey() {
		return NullWritable.get();
	}

	public Text getCurrentValue() {
		return currentValue;
	}

	public void close() throws IOException {
		if(useCompressedStream)
		{
			inputStream_compressed.close();
		}
		inputStream.close();
	}

	public float getProgress() throws IOException {
		if(useCompressedStream)
		{
			return (inputStream_compressed.getPos() - startOffset) / (float) (endOffset - startOffset);
		}
		else
		{
			return (inputStream.getPos() - startOffset) / (float) (endOffset - startOffset);
		}
		
	}

	private boolean scanUntilStartTag() throws IOException {

		int nodeTagIdx = 0;
		int wayTagIdx = 0;
		int relationTagIdx = 0;
		
		int bytesSinceLessThan = -1;
		
		while (true) {
			Integer currentByte;
			if(useCompressedStream)
			{
				currentByte=inputStream_compressed.read();
			}
			else
			{
				currentByte=inputStream.read();
			}
			
			if (isEndOfFile(currentByte))
				return false;

			if (currentByte == lessThan) {
				bytesSinceLessThan = 0;
				nodeTagIdx = 1;
				wayTagIdx = 1;
				relationTagIdx = 1;
			} else if (bytesSinceLessThan >= 0) {
				bytesSinceLessThan++;
				
				if (nodeTagIdx == bytesSinceLessThan || wayTagIdx == bytesSinceLessThan || relationTagIdx == bytesSinceLessThan) {
					if (nodeTagIdx == bytesSinceLessThan && currentByte == nodeStart[nodeTagIdx]) {
						nodeTagIdx++;
						if (nodeTagIdx >= nodeStart.length) {
							buffer.write(nodeStart);
							return true;
						}
					} else if (wayTagIdx == bytesSinceLessThan && currentByte == wayStart[wayTagIdx]) {
						wayTagIdx++;
						if (wayTagIdx >= wayStart.length) {
							buffer.write(wayStart);
							return true;
						}
					} else if (relationTagIdx == bytesSinceLessThan && currentByte == relStart[relationTagIdx]) {
						relationTagIdx++;
						if (relationTagIdx >= relStart.length) {
							buffer.write(relStart);
							return true;
						}
					} else {
						bytesSinceLessThan = -1;
						nodeTagIdx = 0;
						wayTagIdx = 0;
						relationTagIdx = 0;
					}
				}
			}
			
			if (bytesSinceLessThan > 0 && (useCompressedStream ? inputStream_compressed.getPos() : inputStream.getPos()) >= endOffset)
				return false;
		}
	}
	private boolean scanUntilEndTag() throws IOException {
		int nestLevel = 1;
		int lastByte = '#';
		
		while (true) {
			Integer currentByte;
			if(useCompressedStream)
			{
				currentByte=inputStream_compressed.read();
			}
			else
			{
				currentByte=inputStream.read();
			}
			
			if (isEndOfFile(currentByte))
				return false;
			buffer.write(currentByte);
			
			if (lastByte == slash && currentByte == greaterThan)
				nestLevel--;
			
			if (lastByte == lessThan) {
				if (currentByte == slash) {
					nestLevel--;
					
					// read the remaining part of the closing tag
					while (true) {
						if(useCompressedStream)
						{
							currentByte=inputStream_compressed.read();
						}
						else
						{
							currentByte=inputStream.read();
						}
						if (isEndOfFile(currentByte))
							return false;
						buffer.write(currentByte);
						if (currentByte == greaterThan)
							break;
					}
				} else
					nestLevel++;
			}
			lastByte = currentByte;
			
			if (nestLevel == 0)
				return true;
		}
	}
	private boolean isEndOfFile(int b) { return b == -1; }
}
