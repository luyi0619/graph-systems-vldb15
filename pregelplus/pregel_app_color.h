#include "basic/pregel-dev.h"
#include <cassert>
using namespace std;

struct ColorValue_pregel {
    int color; // -1 not assigned, -2 tentative, selected, -3 in MIS
    vector<VertexID> edges;
};

ibinstream& operator<<(ibinstream& m, const ColorValue_pregel& v)
{
    m << v.color;
    m << v.edges;
    return m;
}

obinstream& operator>>(obinstream& m, ColorValue_pregel& v)
{
    m >> v.color;
    m >> v.edges;
    return m;
}

//====================================
double myrand()
{
    return static_cast<double>(rand() / (RAND_MAX + 1.0));
}

class ColorVertex_pregel : public Vertex<VertexID, ColorValue_pregel, VertexID> {
public:
    void broadcast(VertexID msg)
    {
        vector<VertexID>& nbs = value().edges;
        for (int i = 0; i < nbs.size(); i++) {
            send_message(nbs[i], msg);
        }
    }

    virtual void compute(MessageContainer& messages)
    {
    	// all active unless be assigned with a color
    	vector<VertexID>& nbs = value().edges;
        if (step_num() % 3 == 1) {

        	int degree = nbs.size();
        	bool selected;

        	if(degree == 0)
        		selected = true;
        	else
        		selected = myrand() < (1.0 / (2 * degree));

        	if(selected)
        	{
        		value().color = -2;
        		broadcast(id);
        	}

        } else if (step_num() % 3 == 2) {
            if(value().color == -1)
            {
            	return;
            }

        	VertexID min = id;

        	for(int i = 0 ; i < messages.size() ; i ++)
        	{
        		if(messages[i] < min)
        			min = messages[i];
        	}
            if(min < id)
            {
            	value().color = -1;
            }
            else
            {
            	value().color = step_num() /3;
            	broadcast(id);
                vote_to_halt();
                nbs.clear();
            }
        }
        else if (step_num() % 3 == 0)
        {
            if(messages.size() == 0)
                return;
        	vector<VertexID> new_nbs;
        	hash_set<VertexID> msg;
        	for(int i = 0; i < messages.size() ; i ++)
        	{
        		msg.insert(messages[i]);
        	}
            for(int i = 0; i < nbs.size() ; i ++)
        	{
        		if( msg.count(nbs[i]) == 0 )
        		{
        			new_nbs.push_back(nbs[i]);
        		}
        	}
        	nbs.swap(new_nbs);
        }
    }
};

class ColorAgg_pregel : public Aggregator<ColorVertex_pregel, int, int> {
private:
	int sum;
    int add;
public:
    virtual void init()
    {
    	if(step_num() == 1)
    		sum = 0;
    	else
    		sum = *(int*)getAgg();
        add = 0;
    }

    virtual void stepPartial(ColorVertex_pregel* v)
    {
    	if(step_num() % 3 == 2)
    	{
    		if (v->value().color >= 0)
    		{
    			add ++;
    		}
    	}
    }

    virtual void stepFinal(int* part)
    {
    	if(step_num() % 3 == 2)
    	{
    		add += *part;
    	}
    }

    virtual int* finishPartial()
    {
        return &add;
    }
    virtual int* finishFinal()
    {
    	if(step_num() % 3 == 2)
    	{
            sum += add;
    		cout << sum << " vertices have been assigned color "  << step_num() / 3 << endl;
    		cout << get_vnum() - sum << " vertices need to assign a color " << endl;
    	}
        cout <<  "active: " << active_vnum() << endl;
        return &sum;
    }
};

class ColorWorker_pregel : public Worker<ColorVertex_pregel,ColorAgg_pregel> {
    char buf[100];

public:
    //C version
    virtual ColorVertex_pregel* toVertex(char* line)
    {
        char* pch;
        pch = strtok(line, "\t");
        ColorVertex_pregel* v = new ColorVertex_pregel;
        v->id = atoi(pch);
        pch = strtok(NULL, " ");
        int num = atoi(pch);
        for (int i = 0; i < num; i++) {
            pch = strtok(NULL, " ");
            if(v->id != atoi(pch))
                v->value().edges.push_back(atoi(pch));
        }
        v->value().color = -1;
        return v;
    }

    virtual void toline(ColorVertex_pregel* v, BufferedWriter& writer)
    {
        sprintf(buf, "%d\t%d\n", v->id, v->value().color);
        writer.write(buf);
    }
};

void pregel_color(string in_path, string out_path)
{
    WorkerParams param;
    param.input_path = in_path;
    param.output_path = out_path;
    param.force_write = true;
    param.native_dispatcher = false;
    ColorWorker_pregel worker;
    ColorAgg_pregel agg;
    worker.setAggregator(&agg);
    worker.run(param);
}
