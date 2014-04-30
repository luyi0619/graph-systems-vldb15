#include "basic/pregel-dev.h"
#include <cmath>
using namespace std;
int DIAMETER = 13;
//helper function
double myrand()
{
    return static_cast<double>(rand() / (RAND_MAX + 1.0));
}

//helper function to return a hash value for Flajolet & Martin bitmask
size_t hash_value()
{
    size_t ret = 0;
    while (myrand() < 0.5) {
        ret++;
    }
    return ret;
}

const size_t DUPULICATION_OF_BITMASKS = 10;
const double termination_criteria = 0.0001;

struct ApproxdiameterValue {
    //use two bitmasks for consistency
    std::vector<int> bitmask;
    std::vector<VertexID> edges;

    ApproxdiameterValue()
        : bitmask()
    {
    }
    //for approximate Flajolet & Martin counting
    void create_hashed_bitmask(size_t id)
    {
        for (size_t i = 0; i < DUPULICATION_OF_BITMASKS; ++i) {
            size_t hash_val = hash_value();
            int mask = 1 << hash_val;
            bitmask.push_back(mask);
        }
    }
};
void bitwise_or(std::vector<int>& v1,
                const std::vector<int>& v2)
{
    for (size_t a = 0; a < v1.size(); ++a) {
        v1[a] |= v2[a];
    }
}

ibinstream& operator<<(ibinstream& m, const ApproxdiameterValue& v)
{
    m << v.bitmask;
    m << v.edges;
    return m;
}

obinstream& operator>>(obinstream& m, ApproxdiameterValue& v)
{
    m >> v.bitmask;
    m >> v.edges;
    return m;
}

//====================================

class ApproxdiameterVertex : public Vertex<VertexID, ApproxdiameterValue, std::vector<int> > {
public:
    virtual void compute(MessageContainer& messages)
    {
        if (step_num() > DIAMETER)
        {
            vote_to_halt();
            return;
        }

        std::vector<int>& bitmask = value().bitmask;
        for (int i = 0; i < messages.size(); i++) {
            bitwise_or(bitmask, messages[i]);
        }
        const std::vector<VertexID>& edges = value().edges;
        for (int i = 0; i < edges.size(); i++) {
            send_message(edges[i], bitmask);
        }
    }
};

//count the number of vertices reached in the current hop with Flajolet & Martin counting method
size_t approximate_pair_number(const std::vector<int>& bitmask)
{
    double sum = 0.0;
    for (size_t a = 0; a < bitmask.size(); ++a) {
        for (size_t i = 0; i < 32; ++i) {
            if ((bitmask[a] & (1 << i)) == 0) {
                sum += (double)i;
                break;
            }
        }
    }
    return (size_t)(pow(2.0, sum / (double)(bitmask.size())) / 0.77351);
}

class ApproxdiameterAgg : public Aggregator<ApproxdiameterVertex, size_t, size_t> {
private:
    size_t pair_sum;
    size_t last_sum;
public:
    virtual void init()
    {
        pair_sum = 0;
        if(step_num() == 1)
        {
        	last_sum = 0;
        }
        else
        {
        	last_sum = *(size_t*)getAgg();
        }
    }

    virtual void stepPartial(ApproxdiameterVertex* v)
    {
        pair_sum += approximate_pair_number(v->value().bitmask);
    }

    virtual void stepFinal(size_t* part)
    {
        pair_sum += *part;
    }

    virtual size_t* finishPartial()
    {
        return &pair_sum;
    }
    virtual size_t* finishFinal()
    {
    	if(step_num() > 1)
    	{
    		if(pair_sum < last_sum * (1.0 + termination_criteria))
    			;//DIAMETER = step_num();
    		cout << "Approximate pairs number in " << step_num() - 1 << " hop : " << pair_sum << endl;
    	}
        return &pair_sum;
    }
};

class ApproxdiameterWorker : public Worker<ApproxdiameterVertex, ApproxdiameterAgg> {
    char buf[100];

public:
    virtual ApproxdiameterVertex* toVertex(char* line)
    {
        char* pch;
        pch = strtok(line, "\t");
        ApproxdiameterVertex* v = new ApproxdiameterVertex;
        v->id = atoi(pch);
        pch = strtok(NULL, " ");
        int num = atoi(pch);
        for (int i = 0; i < num; i++) {
            pch = strtok(NULL, " ");
            v->value().edges.push_back(atoi(pch));
        }
        v->value().create_hashed_bitmask(v->id);
        return v;
    }

    virtual void toline(ApproxdiameterVertex* v, BufferedWriter& writer)
    {
    }
};

class ApproxdiameterCombiner : public Combiner<std::vector<int> > {
public:
    virtual void combine(std::vector<int>& old, const std::vector<int>& new_msg)
    {
        bitwise_or(old, new_msg);
    }
};

void pregel_approxdiameter(string in_path, string out_path, bool use_combiner)
{
    srand(time(0));
    WorkerParams param;
    param.input_path = in_path;
    param.output_path = out_path;
    param.force_write = true;
    param.native_dispatcher = false;
    ApproxdiameterWorker worker;
    ApproxdiameterCombiner combiner;
    if (use_combiner)
        worker.setCombiner(&combiner);
    ApproxdiameterAgg agg;
    worker.setAggregator(&agg);
    worker.run(param);
    if (_my_rank == 0) {
        cout << "The diameter is : " << step_num() - 3 << endl;
    }
}
