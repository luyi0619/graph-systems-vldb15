#include <cmath>
#include <string>

#include "graphchi_basic_includes.hpp"
#include "util/labelanalysis.hpp"

using namespace graphchi;

typedef int VertexDataType;
typedef int EdgeDataType;

struct ConnectedComponentsProgram : public GraphChiProgram<VertexDataType, EdgeDataType>
{

    VertexDataType * vertex_values;

    vid_t neighbor_value(graphchi_edge<EdgeDataType> * edge)
    {
        return vertex_values[edge->vertex_id()];
    }

    void set_data(graphchi_vertex<VertexDataType, EdgeDataType> &vertex, vid_t value)
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
            set_data(vertex, vertex.id());
            /* Schedule neighbor for update */
            gcontext.scheduler->add_task(vertex.id());
            return;
        }
        else
        {
            vid_t curmin = vertex_values[vertex.id()];
            for(int i=0; i < vertex.num_edges(); i++)
            {
                vid_t nblabel = neighbor_value(vertex.edge(i));
                curmin = std::min(nblabel, curmin);

            }
            if ( curmin < vertex.get_data() )
            {
                for(int i=0; i < vertex.num_edges(); i++)
                {
                    if (curmin < neighbor_value(vertex.edge(i)))
                    {
                        /* Schedule neighbor for update */
                        gcontext.scheduler->add_task(vertex.edge(i)->vertex_id());
                    }
                }
                set_data(vertex, curmin);
            }
        }



        /* On subsequent iterations, find the minimum label of my neighbors */


        /* If my label changes, schedule neighbors */


    }

    void before_iteration(int iteration, graphchi_context &ctx)
    {
        if (iteration == 0)
        {
            /* initialize  each vertex with its own lable */
            vertex_values = new VertexDataType[ctx.nvertices];
            for(int i=0; i < (int)ctx.nvertices; i++)
            {
                vertex_values[i] = i;
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

    ConnectedComponentsProgram program;
    graphchi_engine<VertexDataType, EdgeDataType> engine(filename, nshards, scheduler, m);
    engine.set_modifies_inedges(false); // Improves I/O performance.
    engine.set_modifies_outedges(false); // Improves I/O performance.

    engine.run(program, niters);

    m.start_time("label-analysis");

    analyze_labels<vid_t>(filename);

    m.stop_time("label-analysis");
    /* Report execution metrics */
    metrics_report(m);
    return 0;
}

