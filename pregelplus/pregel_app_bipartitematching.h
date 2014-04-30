#include "basic/pregel-dev.h"
#include <cmath>
using namespace std;

struct BipartiteMatchingValue {
    int left;
    int matchTo;
    std::vector<VertexID> edges;
};

ibinstream& operator<<(ibinstream& m, const BipartiteMatchingValue& v)
{
    m << v.left;
    m << v.matchTo;
    m << v.edges;
    return m;
}

obinstream& operator>>(obinstream& m, BipartiteMatchingValue& v)
{
    m >> v.left;
    m >> v.matchTo;
    m >> v.edges;
    return m;
}

//====================================

class BipartiteMatchingVertex : public Vertex<VertexID, BipartiteMatchingValue, int> {
public:
	int minMsg(MessageContainer& messages)
	{
		int min = messages[0];
		for(int i = 1 ;i < messages.size() ; i ++)
		{
			if (messages[i] < min)
				min = messages[i];
		}
		return min;
	}
    virtual void compute(MessageContainer& messages)
    {
        std::vector<VertexID>& edges = value().edges;
        if (step_num() % 4 == 1) {
            if (value().left == 1 && value().matchTo == -1) // left not matched
            {
                for (int i = 0; i < edges.size(); i++) {
                    send_message(edges[i], id); // request
                }
                vote_to_halt();
            }

        } else if (step_num() % 4 == 2) {
            if (value().left == 0 && value().matchTo == -1) //right  not matched
            {
                if (messages.size() > 0) {
                	int min = minMsg(messages);
                    send_message(min, id); // ask for granting
                    for(int i = 0; i < messages.size(); i ++)
                    {
                    	if(messages[i] != min)
                    		send_message(messages[i], -id - 1); // deny
                    }
                }
                vote_to_halt();
            }

        } else if (step_num() % 4 == 3) {
            if (value().left == 1 && value().matchTo == -1) // left not matched
            {
            	vector<int> grants;
            	for(int i = 0 ;i < messages.size() ;i ++)
            	{
            		if(messages[i] >= 0)
            			grants.push_back(messages[i]);
            	}
                if (grants.size() > 0) {
                    value().matchTo = minMsg(grants);
                    send_message(value().matchTo, id); // grant
                    vote_to_halt();
                }
            }

        } else if (step_num() % 4 == 0) {
            if (value().left == 0 && value().matchTo == -1) //right  not matched
            {
                if (messages.size() == 1) {
                    value().matchTo = messages[0]; // update
                }
                vote_to_halt();
            }
        }
        if(value().left == 0) //right vote to halt
        	vote_to_halt();
    }
};

class BipartiteMatchingWorker : public Worker<BipartiteMatchingVertex> {
    char buf[100];

public:
    // vid \t left=1 num v1 v2
    virtual BipartiteMatchingVertex* toVertex(char* line)
    {
        char* pch;
        pch = strtok(line, "\t");
        BipartiteMatchingVertex* v = new BipartiteMatchingVertex;
        v->id = atoi(pch);
        pch = strtok(NULL, " ");
        v->value().left = atoi(pch) == 0 ? 1 : 0;
        while (pch = strtok(NULL, " ")) {
            v->value().edges.push_back(atoi(pch));
        }
        v->value().matchTo = -1;

        return v;
    }

    virtual void toline(BipartiteMatchingVertex* v, BufferedWriter& writer)
    {
        sprintf(buf, "%d\t%d\n", v->id, v->value().matchTo);
        writer.write(buf);
    }
};

void pregel_bipartitematching(string in_path, string out_path)
{
    WorkerParams param;
    param.input_path = in_path;
    param.output_path = out_path;
    param.force_write = true;
    param.native_dispatcher = false;
    BipartiteMatchingWorker worker;
    worker.run(param);
}