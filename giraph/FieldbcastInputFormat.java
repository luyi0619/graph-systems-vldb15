package org.apache.giraph.examples;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Input format for HashMin IntWritable, NullWritable, NullWritable Vertex ,
 * Vertex Value, Edge Value Graph vertex \t neighbor1 neighbor 2
 */
public class FieldbcastInputFormat extends
		TextVertexInputFormat<IntWritable, IntWritable, IntWritable> {
	/** Separator of the vertex and neighbors */
	private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

	@Override
	public TextVertexReader createVertexReader(InputSplit split,
			TaskAttemptContext context) throws IOException {
		return new FieldbcastVertexReader();
	}

	/**
	 * Vertex reader associated with {@link IntIntNullTextInputFormat}.
	 */
	public class FieldbcastVertexReader extends
			TextVertexReaderFromEachLineProcessed<String[]> {
		/**
		 * Cached vertex id for the current line
		 */
		private IntWritable id;
		private IntWritable value;
		@Override
		protected String[] preprocessLine(Text line) throws IOException {
			String[] tokens = SEPARATOR.split(line.toString());
			id = new IntWritable(Integer.parseInt(tokens[0]));
			value = new IntWritable(Integer.parseInt(tokens[1]));
			return tokens;
		}

		@Override
		protected IntWritable getId(String[] tokens) throws IOException {
			return id;
		}

		@Override
		protected IntWritable getValue(String[] tokens) throws IOException {
			return value;
		}

		@Override
		protected Iterable<Edge<IntWritable, IntWritable>> getEdges(
				String[] tokens) throws IOException {
			List<Edge<IntWritable, IntWritable>> edges = Lists
					.newArrayListWithCapacity(tokens.length - 3);
			for (int n = 3; n < tokens.length; n ++) {
				edges.add(EdgeFactory.create(
						new IntWritable(Integer.parseInt(tokens[n])),
						new IntWritable(-1)));
			}
			return edges;
		}
	}
}