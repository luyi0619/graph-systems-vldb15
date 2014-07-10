package gps.examples.benchmark;

import java.util.HashMap;
import java.util.Map;

public class ConnectedComponentsSinglePivotPhase {
	public static enum Phase {
		BFS(0), HASHMIN_ROUND1(1), HASHMIN_REST(2);

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
