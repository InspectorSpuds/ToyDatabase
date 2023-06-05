//Authors: Ishan Parikh, Ryan S.
//Date:    5-21-23
//Purpose: implementation of a sql evalutatoin plan class 


#ifndef EVAL_PLAN
#define EVAL_PLAN

#include "storage_engine.h"
#include <memory>

typedef std::pair<DbRelation *, Handles *> EvalPipeline;

class EvalPlan {
    public:
        enum PlanType {
            PROJECTALL, PROJECT, SELECT, TABLESCAN
        };

        
        EvalPlan(PlanType type, EvalPlan *relation);  // use for ProjectAll, e.g., EvalPlan(EvalPlan::ProjectAll, table);
        EvalPlan(ColumnNames *projection, EvalPlan *relation); // use for Project
        EvalPlan(ValueDict *conjunction, EvalPlan *relation);  // use for Select
        EvalPlan(DbRelation &table);  // use for TableScan
        EvalPlan(const EvalPlan *other);  // use for copying
        virtual ~EvalPlan();

        // Evaluate the plan: evaluate gets values, pipeline gets handles
        ValueDicts *evaluate();

        EvalPipeline pipeline();

    protected:

        PlanType type;                  //what's the tyep of plan needed

        EvalPlan* relation;             // for everything except TableScan
        ColumnNames* projection;        // columns to project
        ValueDict* select_conjunction;  // values gotten from select
        DbRelation &table;              // for TableScan
};

#endif