//Ryan Silveira
// 4/16/2023
//CPSC 4300 - Milestone 1

#include <iostream>
#include <cstdio>
#include <cstdlib>
#include <string>
#include "db_cxx.h"
#include "SQLParser.h"
#include "ParseTreeToString.h"
#include "SQLExec.h"
using namespace std;
using namespace hsql;


string parse(const SQLStatement* result);
string expressionToString(const Expr *expr);
string operatorExpressionToString(const Expr *expr);
string tableRefInfoToString(const TableRef *table);
string parseInsert(const InsertStatement *statement);
string parseCreate(const CreateStatement *stmt);
string parseSelect(const SelectStatement *stmt);
string parseShow(const ShowStatement *stmt);

//db environment variables
u_int32_t env_flags = DB_CREATE | DB_INIT_MPOOL; //If the environment does not exist, create it.  Initialize memory.
u_int32_t db_flags = DB_CREATE; //If the database does not exist, create it.
DbEnv *_DB_ENV;
int transactionLevel = 0;

int main(int argc, char **argv) {
   if(argc != 2){
        cerr << "Missing path." << endl;
        return -1;
    }
    string dbPath = argv[1];

    //init db environment locally and globally
    DbEnv environment(0U);
    try {
        environment.set_message_stream(&cout);
	    environment.set_error_stream(&cerr);
	    environment.open(dbPath.c_str(), env_flags, 0);
    } catch(DbException &E) {
        cout << "Error creating DB environment" << endl;
        exit(EXIT_FAILURE);
    }
    _DB_ENV = &environment;
    initialize_schema_tables();


    //main parse loop
    while(true){
        string sqlcmd;
        cout << "SQL> ";
        getline(cin, sqlcmd);
        if(sqlcmd == "quit"){
            break;
        }
        SQLParserResult* result = SQLParser::parseSQLString(sqlcmd);
        if(!result->isValid()){
            cout << "Invalid command: " << sqlcmd << endl;
        } else {
            for(uint i = 0; i < result->size(); ++i){
                const SQLStatement* statement = result->getStatement(i);
                try {
                    cout << ParseTreeToString::statement(statement) << endl;
                    QueryResult *q_result = SQLExec::execute(statement);
                    cout << *q_result << endl;
                    delete q_result;
                }
                catch (SQLExecError &e) {
                    cerr << e.what() << endl;
                }
            }
        }
        delete result;
    }

    //close db environment
    return 0;
} 

string expressionToString(const Expr *expr) {
    string ret;
    switch (expr->type){
        case kExprStar:
            ret += "*";
            break;
        case kExprColumnRef:
            if (expr->table != NULL)
                ret += string(expr->table) + ".";
        case kExprLiteralString:
            ret += expr->name;
            break;
        case kExprLiteralFloat:
            ret += to_string(expr->fval);
            break;
        case kExprLiteralInt:
            ret += to_string(expr->ival);
            break;
        case kExprFunctionRef:
            ret += string(expr->name) + "?" + expr->expr->name;
            break;
        case kExprOperator:
            ret += operatorExpressionToString(expr);
            break;
        default:
            ret += "???";  // in case there are exprssion types we don't know about here
            break;
    }
    if (expr->alias != NULL)
        ret += string(" AS ") + expr->alias;
    return ret;
}

string operatorExpressionToString(const Expr *expr) {
    if (expr == NULL)
        return "null";
    string ret;
    // Unary prefix operator: NOT
    if (expr->opType == Expr::NOT)
        ret += "NOT ";
    // Left-hand side of expression
    ret += expressionToString(expr->expr) + " ";
    // Operator itself
    switch (expr->opType){
        case Expr::SIMPLE_OP:
            ret += expr->opChar;
            break;
        case Expr::AND:
            ret += "AND";
            break;
        case Expr::OR:
            ret += "OR";
            break;
        default:
            break; 
    }
    if (expr->expr2 != NULL)
        ret += " " + expressionToString(expr->expr2);
    return ret;
}

string tableRefInfoToString(const TableRef *table) {
    string ret;
    switch (table->type) {
        case kTableSelect:
            ret += "kTableSelect FIXME"; // FIXME
            break;
        case kTableName:
            ret += table->name;
            if (table->alias != NULL)
                ret += string(" AS ") + table->alias;
            break;
        case kTableJoin:
            ret += tableRefInfoToString(table->join->left);
            switch (table->join->type){
                case kJoinCross:
                case kJoinInner:
                    ret += " JOIN ";
                    break;
                case kJoinOuter:
                case kJoinLeftOuter:
                case kJoinLeft:
                    ret += " LEFT JOIN ";
                    break;
                case kJoinRightOuter:
                case kJoinRight:
                    ret += " RIGHT JOIN ";
                    break;
                case kJoinNatural:
                    ret += " NATURAL JOIN ";
                    break;
            }
            ret += tableRefInfoToString(table->join->right);
            if (table->join->condition != NULL)
                ret += " ON " + expressionToString(table->join->condition);
            break;
        case kTableCrossProduct:
            bool doComma = false;
            for (TableRef *tbl : *table->list){
                if (doComma)
                    ret += ", ";
                ret += tableRefInfoToString(tbl);
                doComma = true;
            }
            break;
    }
    return ret;
}

string columnDefinitionToString(const ColumnDefinition *col) {
    string ret(col->name);
    switch (col->type) {
        case ColumnDefinition::DOUBLE:
            ret += " DOUBLE";
            break;
        case ColumnDefinition::INT:
            ret += " INT";
            break;
        case ColumnDefinition::TEXT:
            ret += " TEXT";
            break;
        default:
            ret += " ...";
            break;
    }
    return ret;
}

string parse(const SQLStatement* result){
    switch(result->type()){
        case kStmtSelect:
            return parseSelect((const SelectStatement *) result);
        case kStmtCreate:
            return parseCreate((const CreateStatement *) result);
        case kStmtInsert:
            return parseInsert((const InsertStatement *) result);
        case kStmtShow:
            return parseShow((const ShowStatement *) result);

        //something else is handling this now, no need to refractor honestly
        case kStmtTransaction:
            return "";

        default:
            return "Not implemented.";
    }
} 

string parseInsert(const InsertStatement *statement){
    return "INSERT ...";
}

string parseShow(const ShowStatement *stmt) {
    string ret("SHOW ");
    switch(stmt->type) {
        case ShowStatement::kTables:
            ret += "TABLES";
            break;
        case ShowStatement::kColumns:
            ret += string("COLUMNS FROM ") + stmt->tableName;
            break;
        case ShowStatement::kIndex:
            ret += string("INDEX FROM ") + stmt->tableName;
            break;
        default:
            return "Invalid show";
    }
    return ret;
}

string parseCreate(const CreateStatement *stmt){
    string ret("CREATE TABLE ");
    if (stmt->type != CreateStatement::kTable)
        return ret + "...";
    if (stmt->ifNotExists)
        ret += "IF NOT EXISTS ";
    ret += string(stmt->tableName) + " (";
    bool doComma = false;
    for (ColumnDefinition *col : *stmt->columns){
        if (doComma)
            ret += ", ";
        ret += columnDefinitionToString(col);
        doComma = true;
    }
    ret += ")";
    return ret;
}

string parseSelect(const SelectStatement *stmt){
    string ret("SELECT ");
    bool doComma = false;
    for (Expr *expr : *stmt->selectList){
        if (doComma)
            ret += ", ";
        ret += expressionToString(expr);
        doComma = true;
    }
    ret += " FROM " + tableRefInfoToString(stmt->fromTable);
    if (stmt->whereClause != NULL)
        ret += " WHERE " + expressionToString(stmt->whereClause);
    return ret;
}
