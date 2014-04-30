package gps.examples.benchmark;

import java.util.Vector;

import org.apache.commons.cli.CommandLine;

import gps.writable.BooleanWritable;
import gps.writable.NullWritable;
import gps.writable.SVWritable;

import gps.globalobjects.BooleanANDGlobalObject;
import gps.graph.Edge;
import gps.graph.NullEdgeVertex;
import gps.graph.NullEdgeVertexFactory;
import gps.node.GPSJobConfiguration;
import gps.writable.IntWritable;

public class SVVertex extends NullEdgeVertex<SVWritable, IntWritable> {
    public SVVertex(CommandLine line) {
	java.util.HashMap<String, String> arg_map = new java.util.HashMap<String, String>();
	gps.node.Utils.parseOtherOptions(line, arg_map);
    }

    private void treeInit_D() {

	int D = getValue().getD();
	boolean star = getValue().getStar();
	for (int nb : getNeighborIds()) {
	    if (nb < D) {
		D = nb;
	    }
	}
	setValue(new SVWritable(D, star));
    }

    private void rtHook_1S() {
	int Du = getValue().getD();
	sendMessage(Du, new IntWritable(getId()));
    }

    private void rtHook_2R(Iterable<IntWritable> msgs) {
	int Dw = getValue().getD();
	for (IntWritable requester : msgs) {
	    sendMessage(requester.getValue(), new IntWritable(Dw));
	}
    }

    private void rtHook_2S() {
	int Dv = getValue().getD();
	for (int nb : getNeighborIds()) {
	    sendMessage(nb, new IntWritable(-Dv - 1));
	}
    }

    private void rtHook_3GDS(Iterable<IntWritable> msgs) {
	int Dw = -1;
	int Du = getValue().getD();
	int Dv = -1;// pick the min
	for (IntWritable m : msgs) {
	    int msg = m.getValue();
	    if (msg >= 0) {
		Dw = msg;
	    } else {
		int cur = -msg - 1;
		if (Dv == -1 || cur < Dv)
		    Dv = cur;
	    }
	}
	if (Dw == Du && Dv != -1 && Dv < Du) {
	    sendMessage(Du, new IntWritable(Dv));
	}
    }

    private void rtHook_4GD(Iterable<IntWritable> msgs) {
	int Dv = -1;
	for (IntWritable m : msgs) {
	    int cur = m.getValue();
	    if (Dv == -1 || cur < Dv)
		Dv = cur;
	}
	if (Dv != -1) {
	    boolean star = getValue().getStar();
	    setValue(new SVWritable(Dv, star));
	}
    }

    private void starHook_3GDS(Vector<Integer> msgs) {
	boolean star = getValue().getStar();

	if (star) {
	    int Du = getValue().getD();
	    int Dv = -1;
	    for (Integer msg : msgs) {
		int cur = msg.intValue();
		if (Dv == -1 || cur < Dv)
		    Dv = cur;
	    }
	    if (Dv != -1 && Dv < Du) {
		sendMessage(Du, new IntWritable(Dv));
	    }
	}
    }

    private void shortcut_3GD(Iterable<IntWritable> msgs) {

	int D = msgs.iterator().next().getValue();
	boolean star = getValue().getStar();
	setValue(new SVWritable(D, star));
    }

    private void setStar_1S() {

	int Du = getValue().getD();
	boolean star = true;
	setValue(new SVWritable(Du, star));

	sendMessage(Du, new IntWritable(getId()));
    }

    private void setStar_2R(Iterable<IntWritable> msgs) {
	int Dw = getValue().getD();
	for (IntWritable requester : msgs) {
	    sendMessage(requester.getValue(), new IntWritable(Dw));
	}
    }

    private void setStar_3GDS(Iterable<IntWritable> msgs) {
	int Du = getValue().getD();
	int Dw = msgs.iterator().next().getValue();
	if (Du != Dw) {
	    boolean star = false;
	    setValue(new SVWritable(Du, star));

	    sendMessage(Du, new IntWritable(-1));

	    sendMessage(Dw, new IntWritable(-1));
	}
	sendMessage(Du, new IntWritable(getId()));
    }

    private void setStar_4GDS(Iterable<IntWritable> msgs) {
	int D = getValue().getD();
	boolean star = getValue().getStar();

	Vector<IntWritable> requesters = new Vector<IntWritable>();
	for (IntWritable msg : msgs) {
	    if (msg.getValue() == -1) {
		star = false;
	    } else {
		requesters.add(new IntWritable(msg.getValue()));

	    }
	}
	for (IntWritable requester : requesters) {
	    sendMessage(requester.getValue(), new IntWritable(star ? 1 : 0));
	}

	setValue(new SVWritable(D, star));
    }

    private void setStar_5GD(Iterable<IntWritable> msgs) {
	int D = getValue().getD();

	for (IntWritable m : msgs) { // at most one

	    boolean star = m.getValue() != 0;

	    setValue(new SVWritable(D, star));
	}
    }

    private Vector<Integer> setStar_5GD_starhook(Iterable<IntWritable> messages) {
	Vector<Integer> msgs = new Vector<Integer>();
	int D = getValue().getD();
	boolean star = getValue().getStar();

	for (IntWritable m : messages) {
	    int msg = m.getValue();
	    if (msg >= 0) {
		star = msg != 0;
	    } else {
		msgs.add(new Integer(-msg - 1));
	    }
	}
	setValue(new SVWritable(D, star));
	return msgs;
    }

    @Override
    public void compute(Iterable<IntWritable> messageValues, int superstepNo) {
	int cycle = 14;	
	if(superstepNo == 1)
	{
	    this.setValue( new SVWritable(getId(), false));
	    return;
	}
	if (superstepNo == 2) {
	    treeInit_D();
	    rtHook_1S();
	} else if (superstepNo % cycle == 3) {
	    rtHook_2R(messageValues);
	    rtHook_2S();
	} else if (superstepNo % cycle == 4) {
	    rtHook_3GDS(messageValues);
	} else if (superstepNo % cycle == 5) {
	    rtHook_4GD(messageValues);
	    setStar_1S();
	} else if (superstepNo % cycle == 6) {
	    setStar_2R(messageValues);
	} else if (superstepNo % cycle == 7) {
	    setStar_3GDS(messageValues);
	} else if (superstepNo % cycle == 8) {
	    setStar_4GDS(messageValues);
	    rtHook_2S();
	} else if (superstepNo % cycle == 9) {
	    Vector<Integer> msgs = setStar_5GD_starhook(messageValues);
	    starHook_3GDS(msgs);
	} else if (superstepNo % cycle == 10) {
	    rtHook_4GD(messageValues);
	    rtHook_1S();
	} else if (superstepNo % cycle == 11) {
	    rtHook_2R(messageValues);
	} else if (superstepNo % cycle == 12) {
	    shortcut_3GD(messageValues);
	    setStar_1S();
	} else if (superstepNo % cycle == 13) {
	    setStar_2R(messageValues);
	} else if (superstepNo % cycle == 0) {
	    setStar_3GDS(messageValues);
	} else if (superstepNo % cycle == 1) {
	    setStar_4GDS(messageValues);
	} else if (superstepNo % cycle == 2) {
	    setStar_5GD(messageValues);
	    rtHook_1S();
	}
	getGlobalObjectsMap().putOrUpdateGlobalObject("Star",
		new BooleanANDGlobalObject(getValue().getStar()));
    }

    @Override
    public SVWritable getInitialValue(int id) {
	return new SVWritable(id, false);
    }

    /**
     * Factory class for {@link ConnectedComponentsVertex}.
     * 
     * @author Yi Lu
     */
    public static class SVVertexFactory extends
	    NullEdgeVertexFactory<SVWritable, IntWritable> {

	@Override
	public NullEdgeVertex<SVWritable, IntWritable> newInstance(
		CommandLine commandLine) {
	    return new SVVertex(commandLine);
	}
    }

    public static class JobConfiguration extends GPSJobConfiguration {

	@Override
	public Class<?> getVertexFactoryClass() {
	    return SVVertexFactory.class;
	}

	@Override
	public Class<?> getVertexClass() {
	    return SVVertex.class;
	}

	@Override
	public Class<?> getVertexValueClass() {
	    return SVWritable.class;
	}
	public Class<?> getMasterClass() {
		return SVMaster.class;
	}
	@Override
	public Class<?> getMessageValueClass() {
	    return IntWritable.class;
	}

	@Override
	public boolean hasVertexValuesInInput() {
	    return true;
	}
    }
}
