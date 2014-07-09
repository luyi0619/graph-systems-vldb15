package org.apache.giraph.examples;

import java.io.IOException;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.giraph.io.formats.TextVertexOutputFormat;

public class FieldbcastOutputFormat extends
	TextVertexOutputFormat<IntWritable, IntWritable, IntWritable> {
    public static final String LINE_TOKENIZE_VALUE_DEFAULT = "\t";

    @Override
    public TextVertexWriter createVertexWriter(TaskAttemptContext context) {
	return new FieldbcastVertexWriter();
    }

    /**
     * Vertex writer used with {@link IdWithValueTextOutputFormat}.
     */
    protected class FieldbcastVertexWriter extends TextVertexWriterToEachLine {
	/** Saved delimiter */
	private String delimiter = "\t";

	@Override
	protected Text convertVertexToLine(
		Vertex<IntWritable, IntWritable, IntWritable, ?> vertex)
		throws IOException {

	    StringBuilder str = new StringBuilder();

	    str.append(vertex.getId().toString() + " " + vertex.getValue()
		    + delimiter);
	    for (Edge<IntWritable, IntWritable> e : vertex.getEdges()) {
		str.append(" " + e.getTargetVertexId().toString() + " "
			+ e.getValue().toString());
	    }
	    return new Text(str.toString());
	}
    }
}