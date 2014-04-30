
package org.apache.giraph.examples;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
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
public class BMMInputFormat extends
	TextVertexInputFormat<IntWritable, BMMWritable, NullWritable> {
    /** Separator of the vertex and neighbors */
    private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

    @Override
    public TextVertexReader createVertexReader(InputSplit split,
	    TaskAttemptContext context) throws IOException {
	return new BMMVertexReader();
    }

    /**
     * Vertex reader associated with {@link IntIntNullTextInputFormat}.
     */
    public class BMMVertexReader extends
	    TextVertexReaderFromEachLineProcessed<String[]> {
	private IntWritable id;
	private IntWritable left;
	@Override
	protected String[] preprocessLine(Text line) throws IOException {
	    String[] tokens = SEPARATOR.split(line.toString());
	    id = new IntWritable(Integer.parseInt(tokens[0]));
	    left = new IntWritable(Integer.parseInt(tokens[1]) == 0 ? 1 : 0);
	    return tokens;
	}

	@Override
	protected IntWritable getId(String[] tokens) throws IOException {
	    return id;
	}

	@Override
	protected BMMWritable getValue(String[] tokens) throws IOException {
	    return new BMMWritable(left.get(), -1);
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