#include <cmath>
#include <string>
#include <cassert>
#include "graphchi_basic_includes.hpp"
#include "util/labelanalysis.hpp"

using namespace graphchi;

const int LEFT_THRESHOLD = 3201203; // < left 3201203

typedef int VertexDataType;
typedef int EdgeDataType;


struct BipartiteMaximumMatching : public GraphChiProgram<VertexDataType, EdgeDataType>
{

    /**
     *  Vertex update function.
     *  On first iteration ,each vertex chooses a label = the vertex id.
     *  On subsequent iterations, each vertex chooses the minimum of the neighbor's
     *  label (and itself).
     */
    int converged;
    std::vector<bool> matched;
    bool isLeft(int id)
    {
        return id < LEFT_THRESHOLD;
    }
    void clearEdges(graphchi_vertex<VertexDataType, EdgeDataType> &vertex)
    {
        for(int i=0; i < vertex.num_edges(); i++)
        {
            vertex.edge(i)->set_data(0);
        }
    }
    int pickminIndex(graphchi_vertex<VertexDataType, EdgeDataType> &vertex)
    {
        int idx = -1, minID;
        for(int i=0; i < vertex.num_edges(); i++)
        {
            if(vertex.edge(i)->get_data() == 1)
            {
                if (idx == -1)
                {
                    idx = i;
                    minID = vertex.edge(i)->vertexid;
                }
                else
                {
                    if(vertex.edge(i)->vertexid < minID)
                    {
                        idx = i;
                        minID = vertex.edge(i)->vertexid;
                    }
                }
            }
        }
        return idx;
    }
    void update(graphchi_vertex<VertexDataType, EdgeDataType> &vertex, graphchi_context &gcontext)
    {
        /* This program requires selective scheduling. */
        if(gcontext.iteration == 0)
        {
            vertex.set_data(-1);
            clearEdges(vertex);
            return;
        }

        if(gcontext.iteration % 4 == 1)
        {
            if(isLeft(vertex.id()))
            {
                clearEdges(vertex);
            }
            if(isLeft(vertex.id()) && vertex.get_data() == -1)
            {
                for(int i=0; i < vertex.num_edges(); i++)
                {
		   if(!matched[vertex.edge(i)->vertexid])
                    vertex.edge(i)->set_data(1);
                }
            }
        }
        else if(gcontext.iteration % 4 == 2)
        {
            if(isLeft(vertex.id()) == false && vertex.get_data() == -1)
            {
                int idx = pickminIndex(vertex);
		if(idx != -1)
                {
                    clearEdges(vertex);
                    vertex.edge(idx)->set_data(1);
                }
            }
        }
        else if(gcontext.iteration % 4 == 3)
        {
            if(isLeft(vertex.id()) && vertex.get_data() == -1)
            {
                int idx = pickminIndex(vertex);
                if(idx != -1)
                {
                    vertex.set_data(vertex.edge(idx)->vertexid);
		    matched[vertex.id()] = true;
                    clearEdges(vertex);
                    vertex.edge(idx)->set_data(1);
                }
            }

        }
        else if(gcontext.iteration % 4 == 0)
        {
            if(isLeft(vertex.id()) == false && vertex.get_data() == -1)
            {
                int idx = pickminIndex(vertex);
                if(idx != -1)
                {
                	vertex.set_data(vertex.edge(idx)->vertexid);
		    	matched[vertex.id()] = true;
                	converged++;
                }
                clearEdges(vertex);
            }
        }

    }
    void before_iteration(int iteration, graphchi_context &info)
    {
        converged = 0;
	if(iteration == 0)
	{
		matched = std::vector<bool>(info.nvertices,false);
	}
    }
    void after_iteration(int iteration, graphchi_context &ginfo)
    {
	if(iteration > 0 && iteration % 4 == 0)
	{
		std::cout << "#####: " << converged << std::endl;
	}
	if (iteration > 0 && iteration % 4 == 0 && converged == 0)
	{
		std::cout << "Converged!" << std::endl;
		ginfo.set_last_iteration(iteration);
	}
    }

}
;

int main(int argc, const char ** argv)
{

	graphchi_init(argc, argv);


	metrics m("Bipartite Maximum Matching");

	/* Basic arguments for application */
	std::string filename = get_option_string("file");  // Base filename
	int niters           = get_option_int("niters", 10); // Number of iterations (max)
	bool scheduler       = false;    // Always run with scheduler

	/* Process input file - if not already preprocessed */
	int nshards             = (int) convert_if_notexists<EdgeDataType>(filename, get_option_string("nshards", "auto"));

	BipartiteMaximumMatching program;
	graphchi_engine<VertexDataType, EdgeDataType> engine(filename, nshards, scheduler, m);

	engine.run(program, niters);

	/* Report execution metrics */
	metrics_report(m);
	return 0;
}
