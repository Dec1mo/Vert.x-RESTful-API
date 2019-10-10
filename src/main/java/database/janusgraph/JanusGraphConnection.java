package database.janusgraph;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;

public class JanusGraphConnection {
	public JanusGraph graph = null;
	public GraphTraversalSource g = null;
	
	static JanusGraphConnection instance;
	
	public JanusGraphConnection() {
		graph = JanusGraphFactory.open("inmemory");
		try {
			g = graph.traversal().withRemote("conf/remote-graph.properties");
		} catch (Exception e) {
			System.out.println("Error with traversal");
			e.printStackTrace();
		}
	}

	public static JanusGraphConnection getInstance() {
		if (instance == null) {
			instance = new JanusGraphConnection();
		}
		return instance;
	}
}
