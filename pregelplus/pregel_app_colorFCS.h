#include "basic/pregel-dev.h"
#include <cassert>
using namespace std;

const int EdgeThreshold = 5000000;

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

class ColorVertex_pregel; // forward declaration

struct ColorAggType {
    long long activeEdge;
    vector<ColorVertex_pregel> graph;
    hash_map<int, int> colormap;
};

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

        ColorAggType* aggValue = (ColorAggType*)getAgg();

        if (step_num() % 3 == 1 && aggValue->activeEdge <= EdgeThreshold) {
            // for Agg FCS
            return;
        } else if (step_num() % 3 == 2 && aggValue->activeEdge == -1) {
            value().color = aggValue->colormap[id];
            vote_to_halt();
            return;
        }
        // all active unless be assigned with a color
        vector<VertexID>& nbs = value().edges;
        if (step_num() % 3 == 1) {

            int degree = nbs.size();
            bool selected;

            if (degree == 0)
                selected = true;
            else
                selected = myrand() < (1.0 / (2 * degree));

            if (selected) {
                value().color = -2;
                broadcast(id);
            }

        } else if (step_num() % 3 == 2) {
            if (value().color == -1) {
                return;
            }

            VertexID min = id;

            for (int i = 0; i < messages.size(); i++) {
                if (messages[i] < min)
                    min = messages[i];
            }
            if (min < id) {
                value().color = -1;
            } else {
                value().color = step_num() / 3;
                broadcast(id);
                nbs.clear();
                vote_to_halt();
            }
        } else if (step_num() % 3 == 0) {
            if (messages.size() == 0)
                return;
            vector<VertexID> new_nbs;
            hash_set<VertexID> msg;
            for (int i = 0; i < messages.size(); i++) {
                msg.insert(messages[i]);
            }
            for (int i = 0; i < nbs.size(); i++) {
                if (msg.count(nbs[i]) == 0) {
                    new_nbs.push_back(nbs[i]);
                }
            }
            nbs.swap(new_nbs);
        }
    }
};

ibinstream& operator<<(ibinstream& m, const ColorAggType& v)
{
    m << v.activeEdge;
    m << v.graph;
    m << v.colormap;
    return m;
}

obinstream& operator>>(obinstream& m, ColorAggType& v)
{
    m >> v.activeEdge;
    m >> v.graph;
    m >> v.colormap;
    return m;
}

class ColorAgg_pregel : public Aggregator<ColorVertex_pregel, ColorAggType, ColorAggType> {
private:
    ColorAggType value;
    long long lastActiveEdge;

public:
    virtual void init()
    {
        value.activeEdge = 0;
        value.graph.clear();

        if (step_num() == 1) {
            lastActiveEdge = EdgeThreshold + 1; // larger than EdgeThreshold
        } else if (step_num() % 3 == 1) {
            lastActiveEdge = ((ColorAggType*)getAgg())->activeEdge;
        }
    }

    virtual void stepPartial(ColorVertex_pregel* v)
    {
        if (step_num() % 3 == 0) {
            value.activeEdge += v->value().edges.size();
        }
        if (step_num() % 3 == 1 && lastActiveEdge <= EdgeThreshold) {
            value.graph.push_back(*v);
        }
    }

    virtual void stepFinal(ColorAggType* part)
    {
        value.activeEdge += part->activeEdge;
        value.graph.insert(value.graph.end(), part->graph.begin(), part->graph.end());
    }

    virtual ColorAggType* finishPartial()
    {
        return &value;
    }
    virtual ColorAggType* finishFinal()
    {
        if (value.graph.size() != 0) {
            value.activeEdge = -1;
        }
        cout << step_num() << " " << value.activeEdge << endl;
        /*FCS*/
        if (step_num() % 3 == 1 && lastActiveEdge <= EdgeThreshold) {
            hash_map<int, int>& colormap = value.colormap;
            int latestColor = (step_num() + 2) / 3;
            for (int i = 0; i < value.graph.size(); i++) {
                vector<int> neighborColors;
                for (int j = 0; j < value.graph[i].value().edges.size(); j++) {
                    int nid = value.graph[i].value().edges[j];
                    if (colormap.count(nid) != 0) {
                        neighborColors.push_back(colormap[nid]);
                    }
                }
                sort(neighborColors.begin(), neighborColors.end());
                int previousNeighborColor = -1;

                for (int j = 0; j < neighborColors.size(); j++) {
                    int neigborColor = neighborColors[j];
                    if (previousNeighborColor < 0) {
                        previousNeighborColor = neigborColor;
                    } else if (previousNeighborColor == neigborColor) {
                        continue;
                    } else if (neigborColor == (previousNeighborColor + 1)) {
                        previousNeighborColor = neigborColor;
                    } else {
                        break;
                    }
                }
                if (previousNeighborColor == -1) {
                    colormap[value.graph[i].id] = latestColor;
                } else {
                    colormap[value.graph[i].id] = previousNeighborColor + 1;
                }
            }
        }
        return &value;
    }
};

class ColorWorker_pregel : public Worker<ColorVertex_pregel, ColorAgg_pregel> {
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
            if (v->id != atoi(pch))
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

void pregel_colorFCS(string in_path, string out_path)
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
