package org.apache.giraph.examples;

import java.io.IOException;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.giraph.io.formats.TextVertexOutputFormat;


public class SVOutputFormat
		extends TextVertexOutputFormat<IntWritable, SVWritable, NullWritable> {
	public static final String LINE_TOKENIZE_VALUE_DEFAULT = "\t";

	@Override
	public TextVertexWriter createVertexWriter(TaskAttemptContext context) {
		return new SVVertexWriter();
	}

	/**
	 * Vertex writer used with {@link IdWithValueTextOutputFormat}.
	 */
	protected class SVVertexWriter extends TextVertexWriterToEachLine {
		/** Saved delimiter */
		private String delimiter = "\t";

		@Override
		protected Text convertVertexToLine(Vertex<IntWritable, SVWritable, NullWritable, ?> vertex)
				throws IOException {

			StringBuilder str = new StringBuilder();

			str.append(vertex.getId().toString());
			str.append(delimiter);
			str.append(vertex.getValue().getD());

			return new Text(str.toString());
		}
	}
}