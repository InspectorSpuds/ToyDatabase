#pragma once

#include "storage_engine.h"
#include "HeapFile.h"
#include <cstring>
#include "db_cxx.h"
using namespace std;
using u16 = u_int16_t;
using u32 = u_int32_t;
/**
 * @class HeapTable - Heap storage engine (implementation of DbRelation)
 */

class HeapTable : public DbRelation {
public:
    HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes);

    virtual ~HeapTable() {}

    HeapTable(const HeapTable &other) = delete;

    HeapTable(HeapTable &&temp) = delete;

    HeapTable &operator=(const HeapTable &other) = delete;

    HeapTable &operator=(HeapTable &&temp) = delete;

    virtual void create();

    virtual void create_if_not_exists();

    virtual void drop();

    virtual void open();

    virtual void close();

    virtual Handle insert(const ValueDict *row);

    //Note: ignore these for now, they will be updated in future milestones
    virtual void update(const Handle handle, const ValueDict *new_values);

    virtual void del(const Handle handle) ;

    virtual Handles *select();

    virtual Handles *select(const ValueDict *where);

    Handles* select(Handles *current, const ValueDict *where);

    virtual ValueDict *project(Handle handle);

    virtual ValueDict *project(Handle handle, const ColumnNames *column_names);

protected:
    HeapFile file;

    virtual ValueDict *validate(const ValueDict *row);

    virtual Handle append(const ValueDict *row);

    virtual Dbt *marshal(const ValueDict *row);

    virtual ValueDict *unmarshal(Dbt *data);

    bool selected(Handle handle, ValueDict where);

    ValueDict *project(Handle handle, ValueDict where);
};
