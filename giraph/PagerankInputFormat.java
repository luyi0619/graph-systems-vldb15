package org.apache.giraph.examples;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

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
public class PagerankInputFormat extends
	TextVertexInputFormat<IntWritable, DoubleWritable, NullWritable> {
    /** Separator of the vertex and neighbors */
    private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

    @Override
    public TextVertexReader createVertexReader(InputSplit split,
	    TaskAttemptContext context) throws IOException {
	return new PagerankVertexReader();
    }

    /**
     * Vertex reader associated with {@link IntIntNullTextInputFormat}.
     */
    public class PagerankVertexReader extends
	    TextVertexReaderFromEachLineProcessed<String[]> {

	private IntWritable id;

	@Override
	protected String[] preprocessLine(Text line) throws IOException {
	    String[] tokens = SEPARATOR.split(line.toString());
	    id = new IntWritable(Integer.parseInt(tokens[0]));
	    return tokens;
	}

	@Override
	protected IntWritable getId(String[] tokens) throws IOException {
	    return id;
	}

	@Override
	protected DoubleWritable getValue(String[] tokens) throws IOException {
	    return new DoubleWritable(1.0);
	}

	@Override
	protected Iterable<Edge<IntWritable, NullWritable>> getEdges(
		String[] tokens) throws IOException {

	    List<Edge<IntWritable, NullWritable>> edges = Lists
		    .newArrayListWithCapacity(tokens.length - 2);

	    for (int n = 2; n < tokens.length; n++) {
		edges.add(EdgeFactory.create(new IntWritable(Integer
			.parseInt(tokens[n]))));
	    }
	    return edges;
	}
    }
}