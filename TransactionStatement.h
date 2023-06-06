//note: I got this straight from the sql-parser source code

#ifndef HYRISE_TRANSACTIONSTATEMENT_H
#define HYRISE_TRANSACTIONSTATEMENT_H

#include "SQLParser.h"
using namespace hsql;

// Represents SQL Transaction statements.
// Example: BEGIN TRANSACTION;
enum TransactionCommand { kBeginTransaction, kCommitTransaction, kRollbackTransaction };

struct TransactionStatement : SQLStatement {
    TransactionStatement(TransactionCommand command) : SQLStatement(hsql::StatementType::kStmtExport) {this->command = command;};
    ~TransactionStatement() override {};

    TransactionCommand command;
};


#endif