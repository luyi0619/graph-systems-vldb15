package gps.examples.benchmark;

import java.util.HashMap;
import java.util.Map;

public class BMMFCSPhase {
	public static enum Phase {
		BMM(0), BMM1(1), BMM2(2), BMM3(3);

		private static Map<Integer, Phase> idComputationStateMap = new HashMap<Integer, Phase>();
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
