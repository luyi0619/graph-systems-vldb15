#include <cmath>
#include <string>

#include "graphchi_basic_includes.hpp"
#include "util/labelanalysis.hpp"

using namespace graphchi;

//helper function
double myrand()
{
    return static_cast<double>(rand() / (RAND_MAX + 1.0));
}

//helper function to return a hash value for Flajolet & Martin bitmask
size_t hash_value()
{
    size_t ret = 0;
    while (myrand() < 0.5)
    {
        ret++;
    }
    return ret;
}


const int DUPULICATION_OF_BITMASKS = 10;
//helper function to compute bitwise-or
void bitwise_or(int* v1, const int* v2)
{
    for (size_t a = 0; a < DUPULICATION_OF_BITMASKS; ++a)
    {
        v1[a] |= v2[a];
    }
}
struct vdata
{
    int bitmask[DUPULICATION_OF_BITMASKS];
    vdata()
    {}

    vdata(const vdata& others)
    {
        for (size_t a = 0; a < DUPULICATION_OF_BITMASKS; ++a)
        {
            bitmask[a] = others.bitmask[a];
        }
    }
    vdata& operator+=(const vdata& other)
    {
        bitwise_or(bitmask, other.bitmask);
        return *this;
    }
    //for approximate Flajolet & Martin counting
    void create_hashed_bitmask()
    {
        for (size_t i = 0; i < DUPULICATION_OF_BITMASKS; ++i)
        {
            size_t hash_val = hash_value();
            bitmask[i]= (1 << hash_val);
        }
    }
};

typedef vdata VertexDataType;
typedef int EdgeDataType;

struct DiameterApproximationProgram : public GraphChiProgram<VertexDataType, EdgeDataType>
{

    VertexDataType* even_values;
    VertexDataType* odd_values;
    VertexDataType* read_values;
    VertexDataType* write_values;;
    const VertexDataType& neighbor_value(graphchi_edge<EdgeDataType> * edge)
    {
        return read_values[edge->vertex_id()];
    }

    void set_data(graphchi_vertex<VertexDataType, EdgeDataType> &vertex, const VertexDataType& value)
    {
    	write_values[vertex.id()] = value;
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
        /* On subsequent iterations, find the minimum label of my neighbors */
        VertexDataType curmin = read_values[vertex.id()];
        for(int i=0; i < vertex.num_edges(); i++)
        {
            curmin += neighbor_value(vertex.edge(i));
        }

        set_data(vertex, curmin);
    }

    void before_iteration(int iteration, graphchi_context &ctx)
    {
        if (iteration == 0)
        {
            /* initialize  each vertex with its own lable */
        	even_values = new VertexDataType[ctx.nvertices];
            odd_values = new VertexDataType[ctx.nvertices];
        	write_values = (iteration % 2 == 0 ? even_values : odd_values);
    	    read_values = (iteration % 2 == 1 ? even_values : odd_values);

            for(int i=0; i < (int)ctx.nvertices; i++)
            {
            	write_values[i].create_hashed_bitmask();
            }
        }
    	write_values = (iteration % 2 == 0 ? even_values : odd_values);
    	read_values = (iteration % 2 == 1 ? even_values : odd_values);

    }

}
;

int main(int argc, const char ** argv)
{

    graphchi_init(argc, argv);


    metrics m("Diameter Approximation Program");

    /* Basic arguments for application */
    std::string filename = get_option_string("file");  // Base filename
    int niters           = get_option_int("niters", 10); // Number of iterations (max)
    bool scheduler       = false;    // Always run with scheduler

    /* Process input file - if not already preprocessed */
    int nshards             = (int) convert_if_notexists<EdgeDataType>(filename, get_option_string("nshards", "auto"));

    DiameterApproximationProgram program;
    graphchi_engine<VertexDataType, EdgeDataType> engine(filename, nshards, scheduler, m);
    engine.set_modifies_inedges(false); // Improves I/O performance.
    engine.set_modifies_outedges(false); // Improves I/O performance.

    engine.run(program, niters);


    /* Report execution metrics */
    metrics_report(m);
    return 0;
}
