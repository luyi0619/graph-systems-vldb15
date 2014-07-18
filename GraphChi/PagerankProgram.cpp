#include <string>
#include <fstream>
#include <cmath>

#define GRAPHCHI_DISABLE_COMPRESSION

#include "graphchi_basic_includes.hpp"
#include "util/toplist.hpp"

using namespace graphchi;

#define RANDOMRESETPROB 0.15

typedef double VertexDataType;
typedef double EdgeDataType;

struct PagerankProgram : public GraphChiProgram<VertexDataType, EdgeDataType>
{

    std::vector<EdgeDataType> pr;
    PagerankProgram(int nvertices) :   pr(nvertices, RANDOMRESETPROB)
    {}

    void update(graphchi_vertex<VertexDataType, EdgeDataType> &v, graphchi_context &ginfo)
    {
        if (ginfo.iteration > 0)
        {
            float sum=0;
            for(int i=0; i < v.num_inedges(); i++)
            {
                sum += pr[v.inedge(i)->vertexid];
            }
            if (v.outc > 0)
            {
                pr[v.id()] = (RANDOMRESETPROB + (1 - RANDOMRESETPROB) * sum) / v.outc;
            }
            else
            {
                pr[v.id()] = (RANDOMRESETPROB + (1 - RANDOMRESETPROB) * sum);
            }
        }
        else if (ginfo.iteration == 0)
        {
            if (v.outc > 0)
                pr[v.id()] = 1.0f / v.outc;
        }
        if (ginfo.iteration == ginfo.num_iterations - 1)
        {
            /* On last iteration, multiply pr by degree and store the result */
            v.set_data(v.outc > 0 ? pr[v.id()] * v.outc : pr[v.id()]);
        }
    }

};
int main(int argc, const char ** argv)
{
    graphchi_init(argc, argv);
    metrics m("pagerank");
    global_logger().set_log_level(LOG_DEBUG);

    /* Parameters */
    std::string filename = get_option_string("file"); // Base filename
    int niters = get_option_int("niters", 10);
    bool scheduler = false;                  // Non-dynamic version of pagerank.

    /* Process input file - if not already preprocessed */
    int nshards = convert_if_notexists<EdgeDataType>(filename,
                  get_option_string("nshards", "auto"));

    /* Run */
    graphchi_engine<double, double> engine(filename, nshards, scheduler, m);
    engine.set_modifies_inedges(false); // Improves I/O performance.
    engine.set_modifies_outedges(false);
    engine.set_disable_outedges(true);
    engine.set_only_adjacency(true);

    PagerankProgram program(engine.num_vertices());
    engine.run(program, niters);

    metrics_report(m);
    return 0;
}

