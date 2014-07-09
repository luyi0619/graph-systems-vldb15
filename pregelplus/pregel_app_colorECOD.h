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
struct ColorAggType {
    int sum;
    int phase;
};
ibinstream& operator<<(ibinstream& m, const ColorAggType& v)
{
    m << v.sum;
    m << v.phase;
    return m;
}

obinstream& operator>>(obinstream& m, ColorAggType& v)
{
    m >> v.sum;
    m >> v.phase;
    return m;
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
    void cleanbymsg(vector<VertexID>& nbs, MessageContainer& messages)
    {
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
    virtual void compute(MessageContainer& messages)
    {
        ColorAggType* agg = (ColorAggType*)getAgg();
        vector<VertexID>& nbs = value().edges;
        if (step_num() == 1 || agg->phase == 0) {
            if (value().color >= 0) {
                return;
            }
            // all active unless be assigned with a color
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
                // ECOD
                if (value().color >= 0) {
                    for (int i = 0; i < messages.size(); i++) {
                        send_message(messages[i], id);
                    }
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
                    //broadcast(id);
                    //nbs.clear();
                    //vote_to_halt();
                }
            } else if (step_num() % 3 == 0) {
                cleanbymsg(nbs, messages);
            }
        } else if (agg->phase == 1) {

            if (value().color >= 0) {
                broadcast(id);
                nbs.clear();
                vote_to_halt(); // ECOD
                return;
            }

        } else if (agg->phase == 2) {
            cleanbymsg(nbs, messages);
            if (value().color >= 0) {
                vote_to_halt(); // ECOD
                return;
            }
        } else if (agg->phase == 3) {
            if (step_num() % 3 == 0) {
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
            } else if (step_num() % 3 == 1) {
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
                    value().color = (step_num() - 2) / 3;
                    broadcast(id);
                    vote_to_halt();
                    nbs.clear();
                }
            } else if (step_num() % 3 == 2) {
                cleanbymsg(nbs, messages);
            }
        }
    }
};

class ColorAgg_pregel : public Aggregator<ColorVertex_pregel, ColorAggType, ColorAggType> {
private:
    ColorAggType value;

public:
    // 0 -> ECOD Color 1 -> send 2 -> remove 3 -> color
    virtual void init()
    {
        value.sum = 0;
        if (step_num() == 1)
            value.phase = 0;
        else {
            ColorAggType* agg = (ColorAggType*)getAgg();
            if (agg->sum >= 0.01 * get_vnum()) {
                value.phase = 0;
            } else if (agg->sum < 0.01 * get_vnum() && agg->phase == 0) {
                if (step_num() % 3 == 0)
                    value.phase = 1;
                else
                    value.phase = 0;
            } else if (agg->phase == 1) {
                value.phase = 2;
            } else if (agg->phase == 2) {
                value.phase = 3;
            } else {
                value.phase = 3;
            }
        }
    }

    virtual void stepPartial(ColorVertex_pregel* v)
    {
        if (v->value().color < 0)
            value.sum++;
    }

    virtual void stepFinal(ColorAggType* part)
    {
        value.sum += part->sum;
        value.phase = part->phase;
    }

    virtual ColorAggType* finishPartial()
    {
        return &value;
    }
    virtual ColorAggType* finishFinal()
    {
        cout << "active: " << value.sum << endl;
        cout << "phase: " << value.phase << endl;
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

void pregel_colorECOD(string in_path, string out_path)
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
