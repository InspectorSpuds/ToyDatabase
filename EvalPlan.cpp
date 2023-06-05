#include "EvalPlan.h"

typedef std::pair<DbRelation*, Handles> EvalPipeline;

EvalPlan::EvalPlan(PlanType type, EvalPlan* relation) {
  this->type = type;
  this->relation = relation;
  this->projection = nullptr;
  this->select_conjunction = nullptr;
  this->table = DbRelation();
}

EvalPlan::EvalPlan(ColumnNames *projection, EvalPlan *relation) {
  const PlanType EVAL_TYPE = PROJECT;

  this->type = EVAL_TYPE;
  this->relation = relation;
  this->projection = projection;
  this->select_conjunction = nullptr;
  this->table = DbRelation();
}

EvalPlan::EvalPlan(ValueDict *conjunction, EvalPlan *relation) {
  const PlanType EVAL_TYPE = SELECT;

  this->type = EVAL_TYPE;
  this->relation = relation;
  this->projection = nullptr;
  this->select_conjunction = conjunction;
  this->table = DbRelation();
}

EvalPlan::EvalPlan(DbRelation &table) {
  this->type = PlanType.TABLESCAN;
  this->projection = nullptr;
  this->select_conjunction = nullptr;
  this->table = table;
}

EvalPlan::EvalPlan(const EvalPlan* other) {
  this->type = other->type;
  this->table = other->table;
  this->relation = (other->relation? new EvalPlan(*other->relation) : nullptr);
  this->projection = (other->projection ? new ColumnNames(*other->projection) : nullptr);
  this->select_conjunction(other->select_conjunction ? new ValueDict(*other->select_conjunction) : nullptr);
}

EvalPlan::~EvalPlan() {
  if(relation) delete relation;
  if(projection) delete projection;
  if(select_conjunction) delete select_conjunction;
}

ValueDicts* EvalPlan::evaluate() {
  //need to know what to project column wise to return results
  if(this->type != ProjectAll && this->type != Project)
    throw DbRelationError("Eval Plan must end with a projection");
  
  ValueDict returnVals = nullptr;
  EvalPipeline pipeline = this->relation->pipeline();
  DbRelation* tempTable = pipeline.first;
  Handles* handlese = pipeline.second;
  
  if(this->type == PROJECTALL)
    returnVals = tempTable->project(handles);
  else if(this->type == PROJECT)
    returnVals = tempTable->project(handles, this->projection);

  delete handles;
  return returnVals;
}

EvalPipeline EvalPlan::pipeline() {
  if(this->type == SELECT && this->relation->type == TABLESCAN)
    return EvalPipeline(&this->relation->table, this->relation->table.select(this->select_conjunction));
  
  if(this->type == TABLESCAN) return EvalPipeline(&this->table, this->table.select());

  if(this->type == SELECT) {
    EvalPipeline pipe = this->relation->pipeline();
    DbRelation* tempTable = pipe.first;
    Handles* handles = pipe.second; 
    
    //make new pipeline with select
    EvalPipeline ret(tempTable, tempTable->select(handles, this->select_conjunction));

    delete handles;
    return ret;
  }


  throw DbRelationError("Only Selected and TableScan implemented");
}
