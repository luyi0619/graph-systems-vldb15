package gps.examples.benchmark;


import java.util.HashMap;
import java.util.Map;

public class ColorECODPhase {
	public static enum Phase {
		COLOR_ECOD(0),
		RECOVERY1(1),
		RECOVERY2(2),
		COLOR(3);
		
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
