#include <cmath>
#include <string>

#include "graphchi_basic_includes.hpp"
#include "util/labelanalysis.hpp"

using namespace graphchi;


typedef double VertexDataType;
typedef double EdgeDataType;
const int SourceVertexId = 0;
const int inf = 1000000000;


struct WeightedSingleSourceShortestPathProgram : public GraphChiProgram<VertexDataType, EdgeDataType>
{

    VertexDataType * vertex_values;

    double neighbor_value(graphchi_edge<EdgeDataType> * edge)
    {
        return vertex_values[edge->vertex_id()];
    }

    void set_data(graphchi_vertex<VertexDataType, EdgeDataType> &vertex, double value)
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
            double curmin = vertex_values[vertex.id()];
            for(int i=0; i < vertex.num_inedges(); i++)
            {
            	double ndistance = neighbor_value(vertex.inedge(i)) +  vertex.inedge(i)->get_data();
                curmin = std::min(ndistance, curmin);
            }

            /* If my label changes, schedule neighbors */
            if (curmin < vertex.get_data())
            {
                for(int i=0; i < vertex.num_outedges(); i++)
                {
                    double newdistance = curmin + vertex.outedge(i)->get_data();
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
    /* special_edgelist */
    int nshards             = (int) convert_if_notexists<EdgeDataType>(filename, get_option_string("nshards", "auto"));

    WeightedSingleSourceShortestPathProgram program;
    graphchi_engine<VertexDataType, EdgeDataType> engine(filename, nshards, scheduler, m);
    engine.set_modifies_inedges(false); // Improves I/O performance.
    engine.set_modifies_outedges(false); // Improves I/O performance.

    engine.run(program, niters);

    for(int i = 0 ;i < 5; i ++)
    {
        std::cout << "vid: " << i << " dis: " << program.vertex_values[i] << std::endl;
    }
    /* Report execution metrics */
    metrics_report(m);
    return 0;
}
