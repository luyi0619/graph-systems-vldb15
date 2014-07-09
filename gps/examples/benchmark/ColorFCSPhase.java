package gps.examples.benchmark;


import java.util.HashMap;
import java.util.Map;

public class ColorFCSPhase {
	public static enum Phase {
		COLOR(0),
		FCS1(1),
		FCS2(2);
		private static Map<Integer, Phase> idComputationStateMap =
			new HashMap<Integer, Phase>();
		static {
			for (Phase type : Phase.values()) {
				idComputationStateMap.put(type.id, type);
			}
		}

		private int id;

		private Phase(int id) {
			this.id = id;
		}

		public int getId() {
			return id;
		}

		public static Phase getComputationStageFromId(int id) {
			return idComputationStateMap.get(id);
		}
	}
}
