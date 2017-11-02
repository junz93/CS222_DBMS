#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>
#include <unordered_map>
#include "../rbf/rbfm.h"
using namespace std;

#define RM_EOF (-1)  // end of a scan operator

class RelationManager;

// RM_ScanIterator is an iteratr to go through tuples
class RM_ScanIterator {

    friend class RelationManager;

public:
    RM_ScanIterator() {};

    ~RM_ScanIterator() {};

    // "data" follows the same format as RelationManager::insertTuple()
    RC getNextTuple(RID &rid, void *data)
    {
        return (rbfm_scanIterator.getNextRecord(rid, data) == RBFM_EOF) ? RM_EOF : SUCCESS;
    }

    RC close() { return rbfm_scanIterator.close(); }

private:
    RBFM_ScanIterator rbfm_scanIterator;
};


// Relation Manager
class RelationManager {
public:
    static RelationManager *instance();

    RC createCatalog();

    RC deleteCatalog();

    RC createTable(const string &tableName, const vector<Attribute> &attrs);

    RC deleteTable(const string &tableName);

    RC getAttributes(const string &tableName, vector<Attribute> &attrs);

    RC insertTuple(const string &tableName, const void *data, RID &rid);

    RC deleteTuple(const string &tableName, const RID &rid);

    RC updateTuple(const string &tableName, const void *data, const RID &rid);

    RC readTuple(const string &tableName, const RID &rid, void *data);

    // Print a tuple that is passed to this utility method.
    // The format is the same as printRecord().
    RC printTuple(const vector<Attribute> &attrs, const void *data);

    RC readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data);

    // Scan returns an iterator to allow the caller to go through the results one by one.
    // Do not store entire results in the scan iterator.
    RC scan(const string &tableName,
            const string &conditionAttribute,
            const CompOp compOp,                  // comparison type such as "<" and "="
            const void *value,                    // used in the comparison
            const vector<string> &attributeNames, // a list of projected attributes
            RM_ScanIterator &rm_ScanIterator);

// Extra credit work (10 points)
public:
    RC addAttribute(const string &tableName, const Attribute &attr);

    RC dropAttribute(const string &tableName, const string &attributeName);

protected:
    RelationManager();

    ~RelationManager();

private:
    const string TABLES_TABLE = "Tables";
    const string COLUMNS_TABLE = "Columns";
    const string TABLE_ID = "table-id";
    const string TABLE_NAME = "table-name";
    const string FILE_NAME = "file-name";
    const string SYSTEM_FLAG = "system-flag";
    const string COLUMN_NAME = "column-name";
    const string COLUMN_TYPE = "column-type";
    const string COLUMN_LENGTH = "column-length";
    const string COLUMN_POSITION = "column-position";
    const int TABLES_ATTR_NUM = 4;
    const int COLUMNS_ATTR_NUM = 6;
    const int TABLES_ID = 1;
    const int COLUMNS_ID = 2;

    const string CATALOG_INFO = "catalog_information";

    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    /** private functions called by createCatalog(...) **/
    RC insertCatalogTuple(const string &tableName, const void *data, RID &rid);
    void initializeTablesTable(); // insert essential tuples to "Tables" table as an initialization of catalog
    void initializeColumnsTable(); // insert essential tuples to "Columns" table as an initialization of catalog

    /** private functions called by insertTuple(...) **/
    void prepareRecordDescriptorForTablesTable(vector<Attribute> &recordDescriptor);

    void prepareRecordDescriptorForColumnsTable(vector<Attribute> &recordDescriptor);

    /** private functions for reading and writing metadata **/
    RC prepareTableIdAndTablesRid(const string tableName, int &tableId, RID &rid);

    RC preparePositionAttributeMap(int tableId, unordered_map<int, Attribute> &positionAttributeMap);

    RC deleteTargetTableTuplesInColumnsTable(int tableId);

    RC deleteCatalogTuple(const string &tableName, const RID &rid);
    
    /** private functions for general use **/
    void updateLastTableId(uint32_t tableId);

    uint32_t getLastTableId();

    bool isSystemTable(const string tableName);

    bool isSystemTuple(const string tableName, const RID rid);

    RC prepareRecordDescriptor(const string tableName, vector<Attribute> &recordDescriptor);

};

#endif
