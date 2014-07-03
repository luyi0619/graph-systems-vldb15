#include "basic/pregel-dev.h"
#include <sstream>
#include <ctime>
#include <cstdlib>
using namespace std;

struct CCValue
{
    int color;
    vector<VertexID> edges;
};

ibinstream& operator<<(ibinstream& m, const CCValue& v)
{
    m << v.color;
    m << v.edges;
    return m;
}

obinstream& operator>>(obinstream& m, CCValue& v)
{
    m >> v.color;
    m >> v.edges;
    return m;
}

//====================================

class CCVertex : public Vertex<VertexID, CCValue, VertexID>
{
public:
    void broadcast(VertexID msg)
    {
        vector<VertexID>& nbs = value().edges;
        for (int i = 0; i < nbs.size(); i++)
        {
            send_message(nbs[i], msg);
        }
    }

    virtual void compute(MessageContainer& messages)
    {
        if(phase_num() == 1)
        {
            if(step_num() == 1)
            {
                return; // for agg to pick up a pivot
            }
            else if(step_num() == 2)
            {
                int pivot = *((int*)getAgg());
                if(id == pivot)
                {
                    value().color = -1;
                    broadcast(value().color);
                }
            }
            else
            {
                if(value().color != -1)
                {
                    value().color = -1;
                    broadcast(value().color);
                }
            }
            vote_to_halt();
        }
        else
        {
            if (step_num() == 1)
            {
                if(value().color == -1)
                {
                    vote_to_halt();
                    return;
                }
                VertexID min = id;
                vector<VertexID>& nbs = value().edges;
                for (int i = 0; i < nbs.size(); i++)
                {
                    if (min > nbs[i])
                        min = nbs[i];
                }
                value().color = min;
                broadcast(min);
                vote_to_halt();
            }
            else
            {
                VertexID min = messages[0];
                for (int i = 1; i < messages.size(); i++)
                {
                    if (min > messages[i])
                        min = messages[i];
                }
                if (min < value().color)
                {
                    value().color = min;
                    broadcast(min);
                }
                vote_to_halt();
            }
        }

    }
};



class CCAgg : public Aggregator<CCVertex, int, int>
{
private:
    int VertexToPick;
    int counts;
    double myrand()
    {
        return 1.0 * rand() / RAND_MAX;
    }
public:


    virtual void init()
    {
        if(phase_num() == 1)
        {
            VertexToPick = -1;
        }

        counts = 0;
    }

    virtual void stepPartial(CCVertex* v)
    {
        if(phase_num() == 1)
        {
            if(VertexToPick == -1)
                VertexToPick = v->id;
            else
            {
                if (myrand() < 1.0 / (counts + 1) )
                    VertexToPick = v->id;
            }
            counts += 1;
        }
        else
        {
            counts += v->value().color == -1;
        }

    }

    virtual void stepFinal(int* part)
    {
        if(myrand() < 0.5)
            VertexToPick = *part;
        if(phase_num() == 2)
            counts += *part;
    }

    virtual int* finishPartial()
    {
        if(phase_num() == 1)

            return &VertexToPick;
        else
            return &counts;
    }
    virtual int* finishFinal()
    {
        // hard code for random pivot;
        if(phase_num() == 1 &&  step_num() == 1)
        {
            cout << VertexToPick << " is picked up as a pivot" << endl;
        }
        if(phase_num() == 2 &&  step_num() == 1)
        {
            cout << "SP removes " << counts << " vertices. Percents: " << 1.0 * counts / get_vnum() << endl;
        }
        return &VertexToPick;
    }
};

class CCWorker : public Worker<CCVertex,CCAgg>
{
    char buf[100];

public:
    //C version
    virtual CCVertex* toVertex(char* line)
    {
        char* pch;
        pch = strtok(line, "\t");
        CCVertex* v = new CCVertex;
        v->id = atoi(pch);
        pch = strtok(NULL, " ");
        int num = atoi(pch);
        for (int i = 0; i < num; i++)
        {
            pch = strtok(NULL, " ");
            v->value().edges.push_back(atoi(pch));
        }
        return v;
    }

    virtual void toline(CCVertex* v, BufferedWriter& writer)
    {
        sprintf(buf, "%d\t%d\n", v->id, v->value().color);
        writer.write(buf);
    }
};

class CCCombiner : public Combiner<VertexID>
{
public:
    virtual void combine(VertexID& old, const VertexID& new_msg)
    {
        if (old > new_msg)
            old = new_msg;
    }
};

void pregel_hashminSP(string in_path, string out_path, bool use_combiner)
{
	srand(time(0));
    WorkerParams param;
    param.input_path = in_path;
    param.output_path = out_path;
    param.force_write = true;
    param.native_dispatcher = false;
    CCWorker worker;
    CCCombiner combiner;
    if (use_combiner)
        worker.setCombiner(&combiner);
    CCAgg agg;
    worker.setAggregator(&agg);
    worker.run(param,2);
}
