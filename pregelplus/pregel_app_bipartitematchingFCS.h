#include "basic/pregel-dev.h"
#include <cmath>
#include <cassert>
using namespace std;
const int EdgeThreshold = 5000000;
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
class BipartiteMatchingVertex; // forward declaration
//====================================
struct BMMAggType {
    long long activeEdge;
    vector<BipartiteMatchingVertex> graph;
    hash_map<int, int> matchTo;
};

ibinstream& operator<<(ibinstream& m, const BMMAggType& v)
{
    m << v.activeEdge;
    m << v.graph;
    m << v.matchTo;
    return m;
}

obinstream& operator>>(obinstream& m, BMMAggType& v)
{
    m >> v.activeEdge;
    m >> v.graph;
    m >> v.matchTo;
    return m;
}

class BipartiteMatchingVertex : public Vertex<VertexID, BipartiteMatchingValue, int> {
public:
    int minMsg(MessageContainer& messages)
    {
        int min = messages[0];
        for (int i = 1; i < messages.size(); i++) {
            if (messages[i] < min)
                min = messages[i];
        }
        return min;
    }
    virtual void compute(MessageContainer& messages)
    {
        BMMAggType* aggValue = (BMMAggType*)getAgg();
        std::vector<VertexID>& edges = value().edges;
        if (step_num() % 4 == 1 && aggValue->activeEdge <= EdgeThreshold) {
            if (value().left == 1 && value().matchTo == -1) // left not matched
            {
                for (int i = 0; i < edges.size(); i++) {
                    send_message(edges[i], id); // request
                }
            }
            return;
        } else if (step_num() % 4 == 2 && aggValue->activeEdge <= EdgeThreshold) {
            return;
        } else if (step_num() % 4 == 3 && aggValue->activeEdge <= EdgeThreshold) {
            if (aggValue->matchTo.count(id) != 0) {
                value().matchTo = aggValue->matchTo[id];
            }
            vote_to_halt();
            return;
        }

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
                    for (int i = 0; i < messages.size(); i++) {
                        if (messages[i] != min)
                            send_message(messages[i], -id - 1); // deny
                    }
                }
                vote_to_halt();
            }

        } else if (step_num() % 4 == 3) {
            if (value().left == 1 && value().matchTo == -1) // left not matched
            {
                vector<int> grants;
                for (int i = 0; i < messages.size(); i++) {
                    if (messages[i] >= 0)
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
        if (value().left == 0) //right vote to halt
            vote_to_halt();
    }
};

class BMMAgg : public Aggregator<BipartiteMatchingVertex, BMMAggType, BMMAggType> {
private:
    BMMAggType value;
    long long lastActiveEdge;

public:
    virtual void init()
    {
        value.activeEdge = 0;
        value.graph.clear();

        if (step_num() == 1) {
            lastActiveEdge = EdgeThreshold + 1; // larger than EdgeThreshold
        } else if (step_num() % 4 == 1 || step_num() % 4 == 2) {
            lastActiveEdge = ((BMMAggType*)getAgg())->activeEdge;
        }
    }

    virtual void stepPartial(BipartiteMatchingVertex* v)
    {
        if (step_num() % 4 == 0 && v->value().matchTo == -1) {
            value.activeEdge += v->value().edges.size();
        }
        if (step_num() % 4 == 2 && lastActiveEdge <= EdgeThreshold) {
            if (v->value().matchTo == -1) {
                value.graph.push_back(*v); // right & left
            }
        }
    }

    virtual void stepFinal(BMMAggType* part)
    {
        value.activeEdge += part->activeEdge;
        value.graph.insert(value.graph.end(), part->graph.begin(), part->graph.end());
    }

    virtual BMMAggType* finishPartial()
    {
        return &value;
    }
    virtual BMMAggType* finishFinal()
    {
        if (step_num() % 4 != 0) {
            value.activeEdge = lastActiveEdge;
        }
        if (step_num() % 4 == 0) {
            cout << step_num() << " " << value.activeEdge << endl;
        }
        /*FCS*/
        if (step_num() % 4 == 2 && value.activeEdge <= EdgeThreshold) {
            vector<BipartiteMatchingVertex>& graph = value.graph;
            hash_map<int, int>& matchTo = value.matchTo;

            hash_map<int, vector<int> > right;
            hash_map<int, vector<int> > left;

            int lastmatch = -1, match = 0;
            for (int superstep = 1;; superstep++) {
                if (superstep % 4 == 1) {
                    right.clear();
                    for (int i = 0; i < graph.size(); i++) {
                        BipartiteMatchingVertex& vertex = graph[i];
                        if (vertex.value().left == 1 && matchTo.count(vertex.id) == 0) // left not matched
                        {
                            vector<VertexID>& edges = vertex.value().edges;
                            for (int j = 0; j < edges.size(); j++) {
                                right[edges[j]].push_back(vertex.id);
                            }
                        }
                    }
                } else if (superstep % 4 == 2) {
                    left.clear();
                    for (int i = 0; i < graph.size(); i++) {
                        BipartiteMatchingVertex& vertex = graph[i];
                        if (vertex.value().left == 0 && matchTo.count(vertex.id) == 0) // right  not matched
                        {
                            if (right.count(vertex.id) != 0) {
                                vector<int>& messages = right[vertex.id];
                                vector<VertexID>& edges = vertex.value().edges;
                                int min = *min_element(messages.begin(), messages.end());
                                left[min].push_back(vertex.id); // ask for granting

                                for (int j = 0; j < messages.size(); j++) {
                                    if (messages[j] != min) {
                                        left[messages[j]].push_back(-vertex.id - 1); // deny
                                    }
                                }
                            }
                        }
                    }
                } else if (superstep % 4 == 3) {
                    right.clear();
                    for (int i = 0; i < graph.size(); i++) {
                        BipartiteMatchingVertex& vertex = graph[i];
                        if (vertex.value().left == 1 && matchTo.count(vertex.id) == 0) // left not matched
                        {
                            if (left.count(vertex.id) != 0) {
                                vector<int> grants;
                                vector<int>& messages = left[vertex.id];
                                for (int j = 0; j < messages.size(); j++) {
                                    if (messages[j] >= 0)
                                        grants.push_back(messages[j]);
                                }
                                if (grants.size() > 0) {
                                    int m = *min_element(grants.begin(), grants.end());
                                    matchTo[vertex.id] = m;
                                    right[m].push_back(vertex.id); // grant
                                    match += 1;
                                }
                            }
                        }
                    }
                } else if (superstep % 4 == 0) {
                    left.clear();
                    for (int i = 0; i < graph.size(); i++) {
                        BipartiteMatchingVertex& vertex = graph[i];
                        if (vertex.value().left == 0 && matchTo.count(vertex.id) == 0) // right  not matched
                        {
                            if (right.count(vertex.id) == 1) {
                                matchTo[vertex.id] = right[vertex.id][0];
                                match += 1;
                            }
                        }
                    }
                    cout << "step-sub: " << superstep << " matchTosize: " << matchTo.size() << " graphsize: " << graph.size() << endl;
                    if (match == lastmatch) {
                        break;
                    }
                    lastmatch = match;
                }
            }
        }
        return &value;
    }
};

class BipartiteMatchingWorker : public Worker<BipartiteMatchingVertex, BMMAgg> {
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

void pregel_bipartitematchingFCS(string in_path, string out_path)
{
    WorkerParams param;
    param.input_path = in_path;
    param.output_path = out_path;
    param.force_write = true;
    param.native_dispatcher = false;
    BipartiteMatchingWorker worker;
    BMMAgg agg;
    worker.setAggregator(&agg);
    worker.run(param);
}
