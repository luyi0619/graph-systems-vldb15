#include <cmath>
#include <string>

#include "graphchi_basic_includes.hpp"
#include "util/labelanalysis.hpp"

using namespace graphchi;

typedef int VertexDataType;
typedef int EdgeDataType;
const int SourceVertexId = 44881114;
const int inf = 1000000000;

struct UnweightedSingleSourceShortestPathProgram : public GraphChiProgram<VertexDataType, EdgeDataType>
{

    VertexDataType * vertex_values;

    int neighbor_value(graphchi_edge<EdgeDataType> * edge)
    {
        return vertex_values[edge->vertex_id()];
    }

    void set_data(graphchi_vertex<VertexDataType, EdgeDataType> &vertex, int value)
    {
        vertex_values[vertex.id()] = value;
        vertex.set_data(value);
    }

    /**
     *  Vertex update function.
     *  On first iteration ,each vertex chooses a label = the vertex id.
     *  On subsequent iterations, each vertex chooses the minimum of the neighbor's
     *  label (and itself).
     */
    void update(graphchi_vertex<VertexDataType, EdgeDataType> &vertex, graphchi_context &gcontext)
    {
        /* This program requires selective scheduling. */
        assert(gcontext.scheduler != NULL);

        if(gcontext.iteration == 0)
        {
            if( vertex.id() == SourceVertexId)
            {
                set_data(vertex, 0);
                for(int i=0; i < vertex.num_outedges(); i++)
                {
                	gcontext.scheduler->add_task(vertex.outedge(i)->vertex_id());
                }
            }
            else
                set_data(vertex, inf);

            /* Schedule neighbor for update */

            return;
        }
        else
        {
            /* On subsequent iterations, find the minimum label of my neighbors */
            int curmin = vertex_values[vertex.id()];
            for(int i=0; i < vertex.num_inedges(); i++)
            {
                int ndistance = neighbor_value(vertex.inedge(i)) + 1;
                curmin = std::min(ndistance, curmin);
            }

            /* If my label changes, schedule neighbors */
            if (curmin < vertex.get_data())
            {
                for(int i=0; i < vertex.num_outedges(); i++)
                {
                    int newdistance = curmin + 1;
                    if (newdistance < neighbor_value(vertex.outedge(i)))
                    {
                        /* Schedule neighbor for update */
                        gcontext.scheduler->add_task(vertex.outedge(i)->vertex_id());
                    }
                }
            }
            set_data(vertex, curmin);
        }



    }

    void before_iteration(int iteration, graphchi_context &ctx)
    {
        if (iteration == 0)
        {
            /* initialize  each vertex with its own lable */
            vertex_values = new VertexDataType[ctx.nvertices];
            for(int i=0; i < (int)ctx.nvertices; i++)
            {
                if( i == SourceVertexId)
                    vertex_values[i] = 0;
                else
                    vertex_values[i] = inf;

            }
        }
    }

}
;

int main(int argc, const char ** argv)
{

    graphchi_init(argc, argv);


    metrics m("Connected Components Program");

    /* Basic arguments for application */
    std::string filename = get_option_string("file");  // Base filename
    int niters           = get_option_int("niters", 10); // Number of iterations (max)
    bool scheduler       = true;    // Always run with scheduler

    /* Process input file - if not already preprocessed */
    int nshards             = (int) convert_if_notexists<EdgeDataType>(filename, get_option_string("nshards", "auto"));

    UnweightedSingleSourceShortestPathProgram program;
    graphchi_engine<VertexDataType, EdgeDataType> engine(filename, nshards, scheduler, m);
    engine.set_modifies_inedges(false); // Improves I/O performance.
    engine.set_modifies_outedges(false); // Improves I/O performance.

    engine.run(program, niters);


    /* Report execution metrics */
    metrics_report(m);
    return 0;
}
