
#include "rm.h"
#include "util.h"
#include <map>


RelationManager *RelationManager::instance() {
    static RelationManager _rm;
    return &_rm;
}

RelationManager::RelationManager() {
}

RelationManager::~RelationManager() {
}

RC RelationManager::createCatalog() {
    // create files
    if (rbfm->createFile(TABLES_TABLE) == FAIL) {
        rbfm->destroyFile(TABLES_TABLE);
        return FAIL;
    }
    if (rbfm->createFile(COLUMNS_TABLE) == FAIL) {
        rbfm->destroyFile(COLUMNS_TABLE);
        return FAIL;
    }

    // initialize catalog tables
    initializeTablesTable();
    initializeColumnsTable();

    // create catalog information file and store last table id
    ofstream file(CATALOG_INFO, fstream::out | fstream::binary);
    updateLastTableId(2);

    return SUCCESS;
}

RC RelationManager::deleteCatalog() {
    if (rbfm->destroyFile(TABLES_TABLE) == FAIL || rbfm->destroyFile(COLUMNS_TABLE) == FAIL
        || rbfm->destroyFile(CATALOG_INFO) == FAIL) {
        return FAIL;
    }
    return SUCCESS;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs) {
    RID rid;
    void *tuple = malloc(PAGE_SIZE);
    int tableId = (int) getLastTableId() + 1;

    // create file
    if (rbfm->createFile(tableName) == FAIL) {
        rbfm->destroyFile(tableName);
        return FAIL;
    }
    // write schema to "Tables" table
    prepareTupleForTables(TABLES_ATTR_NUM, tableId, tableName, false, tuple);
    insertCatalogTuple(TABLES_TABLE, tuple, rid);
    // write schema to "Columns" table
    int columnPosition = 0;
    for (const Attribute &attribute : attrs) {
        prepareTupleForColumns(COLUMNS_ATTR_NUM, tableId, attribute.name, attribute.type, attribute.length,
                               ++columnPosition, false, tuple);
        insertCatalogTuple(COLUMNS_TABLE, tuple, rid);
    }
    // Plus last table id by 1 in catalog_information file
    updateLastTableId(tableId);

    free(tuple);

    return SUCCESS;
}

RC RelationManager::deleteTable(const string &tableName) {
    int tableId;
    RID rid;

    if (isSystemTable(tableName)) {
        return FAIL;
    }

    // delete schema in Catalog
    if (prepareTableIdAndTablesRid(tableName, tableId, rid) == FAIL) { return FAIL; }
    if (deleteCatalogTuple(TABLES_TABLE, rid) == FAIL) {return FAIL;}
    if (deleteTargetTableTuplesInColumnsTable(tableId) == FAIL) {return FAIL;}
    // delete table file
    if (rbfm->destroyFile(tableName) == FAIL) { return FAIL; }


    return SUCCESS;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs) {
    int tableId;
    unordered_map<int, Attribute> positionAttributeMap;
    RID rid;

    if (prepareTableIdAndTablesRid(tableName, tableId, rid) == FAIL) { return FAIL; }
    if (preparePositionAttributeMap(tableId, positionAttributeMap) == FAIL) { return FAIL; }

    // prepare attrs ordered by column position
    for (int i = 1; i <= positionAttributeMap.size(); i++) {
        auto it = positionAttributeMap.find(i);
        if (it == positionAttributeMap.end()) {
            return FAIL;
        } else {
            attrs.push_back(positionAttributeMap[i]);
        }
    }

    return SUCCESS;
}


RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid) {
    FileHandle fileHandle;
    vector<Attribute> recordDescriptor;

    if (isSystemTable(tableName)) {
        return FAIL;
    }

    if (rbfm->openFile(tableName, fileHandle) == FAIL) {
        return FAIL;
    }

    prepareRecordDescriptor(tableName, recordDescriptor);

    if (rbfm->insertRecord(fileHandle, recordDescriptor, data, rid) == FAIL) {
        return FAIL;
    }

    rbfm->closeFile(fileHandle);

    return SUCCESS;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid) {
    FileHandle fileHandle;
    vector<Attribute> recordDescriptor;

    if (isSystemTable(tableName) || isSystemTuple(tableName, rid)) {
        return FAIL;
    }
    if (rbfm->openFile(tableName, fileHandle) == FAIL) {
        return FAIL;
    }
    prepareRecordDescriptor(tableName, recordDescriptor);
    if (rbfm->deleteRecord(fileHandle, recordDescriptor, rid) == FAIL) {
        return FAIL;
    }
    rbfm->closeFile(fileHandle);

    return SUCCESS;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid) {
    FileHandle fileHandle;
    vector<Attribute> recordDescriptor;

    if (isSystemTable(tableName) || isSystemTuple(tableName, rid)) {
        return FAIL;
    }

    if (rbfm->openFile(tableName, fileHandle) == FAIL) {
        return FAIL;
    }
    prepareRecordDescriptor(tableName, recordDescriptor);
    if (rbfm->updateRecord(fileHandle, recordDescriptor, data, rid) == FAIL) {
        return FAIL;
    }
    rbfm->closeFile(fileHandle);

    return SUCCESS;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data) {
    FileHandle fileHandle;
    vector<Attribute> recordDescriptor;
    if (rbfm->openFile(tableName, fileHandle) == FAIL || getAttributes(tableName, recordDescriptor) == FAIL) {
        return FAIL;
    }
    if (rbfm->readRecord(fileHandle, recordDescriptor, rid, data) == FAIL) { return FAIL; }
    rbfm->closeFile(fileHandle);
    return SUCCESS;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data) {
    return rbfm->printRecord(attrs, data);
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data) {
    FileHandle fileHandle;
    vector<Attribute> recordDescriptor;

    if (rbfm->openFile(tableName, fileHandle) == FAIL) {
        return FAIL;
    }
    prepareRecordDescriptor(tableName, recordDescriptor);
    if (rbfm->readAttribute(fileHandle, recordDescriptor, rid, attributeName, data) == FAIL) {
        return FAIL;
    }
    rbfm->closeFile(fileHandle);

    return SUCCESS;
}

RC RelationManager::scan(const string &tableName,
                         const string &conditionAttribute,
                         const CompOp compOp,
                         const void *value,
                         const vector<string> &attributeNames,
                         RM_ScanIterator &rm_ScanIterator) {
    FileHandle *fileHandle = new FileHandle;
    vector<Attribute> recordDescriptor;
    RBFM_ScanIterator &rbfm_scanIterator = rm_ScanIterator.rbfm_scanIterator;

    if (rbfm->openFile(tableName, *fileHandle) == FAIL) { return FAIL; }

    prepareRecordDescriptor(tableName, recordDescriptor);
    rbfm->scan(*fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames, rbfm_scanIterator);

    return SUCCESS;
}

// Extra credit work
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName) {
    return -1;
}

// Extra credit work
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr) {
    return -1;
}

/** private functions called by createCatalog(...) **/
RC RelationManager::insertCatalogTuple(const string &tableName, const void *data, RID &rid) {
    FileHandle fileHandle;
    vector<Attribute> recordDescriptor;

    if (rbfm->openFile(tableName, fileHandle) == FAIL) {
        return FAIL;
    }

    prepareRecordDescriptor(tableName, recordDescriptor);

    if (rbfm->insertRecord(fileHandle, recordDescriptor, data, rid) == FAIL) {
        return FAIL;
    }

    rbfm->closeFile(fileHandle);

    return SUCCESS;
}

void RelationManager::initializeTablesTable() {
    RID rid;
    void *tuple = malloc(PAGE_SIZE);

    prepareTupleForTables(TABLES_ATTR_NUM, TABLES_ID, TABLES_TABLE, true, tuple);
    insertCatalogTuple(TABLES_TABLE, tuple, rid);

    prepareTupleForTables(TABLES_ATTR_NUM, COLUMNS_ID, COLUMNS_TABLE, true, tuple);
    insertCatalogTuple(TABLES_TABLE, tuple, rid);

    free(tuple);
}

void RelationManager::initializeColumnsTable() {
    RID rid;
    void *tuple = malloc(PAGE_SIZE);

    prepareTupleForColumns(COLUMNS_ATTR_NUM, TABLES_ID, TABLE_ID, TypeInt, 4, 1, true, tuple);
    insertCatalogTuple(COLUMNS_TABLE, tuple, rid);
    prepareTupleForColumns(COLUMNS_ATTR_NUM, TABLES_ID, TABLE_NAME, TypeVarChar, 50, 2, true, tuple);
    insertCatalogTuple(COLUMNS_TABLE, tuple, rid);
    prepareTupleForColumns(COLUMNS_ATTR_NUM, TABLES_ID, FILE_NAME, TypeVarChar, 50, 3, true, tuple);
    insertCatalogTuple(COLUMNS_TABLE, tuple, rid);
    prepareTupleForColumns(COLUMNS_ATTR_NUM, TABLES_ID, SYSTEM_FLAG, TypeInt, 4, 4, true, tuple);
    insertCatalogTuple(COLUMNS_TABLE, tuple, rid);

    prepareTupleForColumns(COLUMNS_ATTR_NUM, COLUMNS_ID, TABLE_ID, TypeInt, 4, 1, true, tuple);
    insertCatalogTuple(COLUMNS_TABLE, tuple, rid);
    prepareTupleForColumns(COLUMNS_ATTR_NUM, COLUMNS_ID, COLUMN_NAME, TypeVarChar, 50, 2, true, tuple);
    insertCatalogTuple(COLUMNS_TABLE, tuple, rid);
    prepareTupleForColumns(COLUMNS_ATTR_NUM, COLUMNS_ID, COLUMN_TYPE, TypeInt, 4, 3, true, tuple);
    insertCatalogTuple(COLUMNS_TABLE, tuple, rid);
    prepareTupleForColumns(COLUMNS_ATTR_NUM, COLUMNS_ID, COLUMN_LENGTH, TypeInt, 4, 4, true, tuple);
    insertCatalogTuple(COLUMNS_TABLE, tuple, rid);
    prepareTupleForColumns(COLUMNS_ATTR_NUM, COLUMNS_ID, COLUMN_POSITION, TypeInt, 4, 5, true, tuple);
    insertCatalogTuple(COLUMNS_TABLE, tuple, rid);
    prepareTupleForColumns(COLUMNS_ATTR_NUM, COLUMNS_ID, SYSTEM_FLAG, TypeInt, 4, 6, true, tuple);
    insertCatalogTuple(COLUMNS_TABLE, tuple, rid);

    free(tuple);
}

/** private functions called by insertTuple(...) **/
void RelationManager::prepareRecordDescriptorForTablesTable(vector<Attribute> &recordDescriptor) {
    Attribute attribute;

    attribute.name = TABLE_ID;
    attribute.type = TypeInt;
    attribute.length = (AttrLength) 4;
    recordDescriptor.push_back(attribute);

    attribute.name = TABLE_NAME;
    attribute.type = TypeVarChar;
    attribute.length = (AttrLength) 50;
    recordDescriptor.push_back(attribute);

    attribute.name = FILE_NAME;
    attribute.type = TypeVarChar;
    attribute.length = (AttrLength) 50;
    recordDescriptor.push_back(attribute);

    attribute.name = SYSTEM_FLAG;
    attribute.type = TypeInt;
    attribute.length = (AttrLength) 4;
    recordDescriptor.push_back(attribute);
}

void RelationManager::prepareRecordDescriptorForColumnsTable(vector<Attribute> &recordDescriptor) {
    Attribute attribute;

    attribute.name = TABLE_ID;
    attribute.type = TypeInt;
    attribute.length = (AttrLength) 4;
    recordDescriptor.push_back(attribute);

    attribute.name = COLUMN_NAME;
    attribute.type = TypeVarChar;
    attribute.length = (AttrLength) 50;
    recordDescriptor.push_back(attribute);

    attribute.name = COLUMN_TYPE;
    attribute.type = TypeInt;
    attribute.length = (AttrLength) 4;
    recordDescriptor.push_back(attribute);

    attribute.name = COLUMN_LENGTH;
    attribute.type = TypeInt;
    attribute.length = (AttrLength) 4;
    recordDescriptor.push_back(attribute);

    attribute.name = COLUMN_POSITION;
    attribute.type = TypeInt;
    attribute.length = (AttrLength) 4;
    recordDescriptor.push_back(attribute);

    attribute.name = SYSTEM_FLAG;
    attribute.type = TypeInt;
    attribute.length = (AttrLength) 4;
    recordDescriptor.push_back(attribute);

}

/** private functions for reading and writing metadata **/
RC RelationManager::prepareTableIdAndTablesRid(const string tableName, int &tableId, RID &rid) {
    RM_ScanIterator rm_scanIterator;
    void *returnedData = malloc(200);
    void *scanValueOfTableName = malloc(tableName.size() + 4);
    vector<string> attributeNames;
    attributeNames.push_back(TABLE_ID);

    prepareScanValue(tableName, scanValueOfTableName);
    if (scan(TABLES_TABLE, TABLE_NAME, EQ_OP, scanValueOfTableName, attributeNames, rm_scanIterator) == FAIL) {
        return FAIL;
    }
    if (rm_scanIterator.getNextTuple(rid, returnedData) == RM_EOF) { return FAIL; }
    tableId = *((int *) ((char *) returnedData + getByteOfNullsIndicator(attributeNames.size())));
    free(scanValueOfTableName);
    free(returnedData);
    rm_scanIterator.close();
    return SUCCESS;
}

RC RelationManager::preparePositionAttributeMap(int tableId, unordered_map<int, Attribute> &positionAttributeMap) {
    RID rid;
    RM_ScanIterator rm_scanIterator;
    void *returnedData = malloc(200);
    vector<string> attributeNames;
    attributeNames.push_back(COLUMN_NAME);
    attributeNames.push_back(COLUMN_TYPE);
    attributeNames.push_back(COLUMN_LENGTH);
    attributeNames.push_back(COLUMN_POSITION);
    int nullFieldIndicatorSize = getByteOfNullsIndicator(attributeNames.size());

    if (scan(COLUMNS_TABLE, TABLE_ID, EQ_OP, getScanValue(tableId), attributeNames, rm_scanIterator) == FAIL) {
        return FAIL;
    }
    while (rm_scanIterator.getNextTuple(rid, returnedData) != RM_EOF) {
        Attribute attribute;
        int offset = 0;

        int columnNameLength = *(uint32_t *) ((char *) returnedData + nullFieldIndicatorSize + offset);
        offset += sizeof(uint32_t);
        attribute.name = string((char *) returnedData + nullFieldIndicatorSize + offset, columnNameLength);
        offset += columnNameLength;

        int columnType = *(int *) ((char *) returnedData + offset + nullFieldIndicatorSize);
        attribute.type = (AttrType) columnType;
        offset += sizeof(int);

        int columnLength = *(int *) ((char *) returnedData + offset + nullFieldIndicatorSize);
        attribute.length = columnLength;
        offset += sizeof(int);

        int columnPositon = *(int *) ((char *) returnedData + offset + nullFieldIndicatorSize);
        positionAttributeMap[columnPositon] = attribute;
        offset += sizeof(int);
    }
    free(returnedData);
    rm_scanIterator.close();
    return SUCCESS;
}

RC RelationManager::deleteTargetTableTuplesInColumnsTable(int tableId) {
    RID rid;
    RM_ScanIterator rm_scanIterator;
    void *returnedData = malloc(200);
    vector<string> attributeNames;
    vector<Attribute> recordDescriptor;
    prepareRecordDescriptorForColumnsTable(recordDescriptor);
    attributeNames.push_back(COLUMN_NAME);
    int nullFieldIndicatorSize = getByteOfNullsIndicator(attributeNames.size());

    if (scan(COLUMNS_TABLE, TABLE_ID, EQ_OP, getScanValue(tableId), attributeNames, rm_scanIterator) == FAIL) {
        return FAIL;
    }
    while (rm_scanIterator.getNextTuple(rid, returnedData) != RM_EOF) {
        deleteCatalogTuple(COLUMNS_TABLE, rid);
    }
    free(returnedData);
    rm_scanIterator.close();
    return SUCCESS;
}

RC RelationManager::deleteCatalogTuple(const string &tableName, const RID &rid) {
    FileHandle fileHandle;
    vector<Attribute> recordDescriptor;

    if (isSystemTuple(tableName, rid)) {
        return FAIL;
    }
    if (rbfm->openFile(tableName, fileHandle) == FAIL) {
        return FAIL;
    }
    prepareRecordDescriptor(tableName, recordDescriptor);
    if (rbfm->deleteRecord(fileHandle, recordDescriptor, rid) == FAIL) {
        return FAIL;
    }
    rbfm->closeFile(fileHandle);

    return SUCCESS;
}

/** private functions for general use **/
void RelationManager::updateLastTableId(uint32_t tableId) {
    fstream file;
    file.open(CATALOG_INFO);
    file.write((char *) &tableId, sizeof(uint32_t));
    file.close();
}

uint32_t RelationManager::getLastTableId() {
    fstream file;
    file.open(CATALOG_INFO, fstream::in | fstream::binary);
    uint32_t tableId;
    file.read((char *) &tableId, sizeof(uint32_t));
    file.close();
    return tableId;
}

bool RelationManager::isSystemTable(const string tableName) {
    return tableName == TABLES_TABLE || tableName == COLUMNS_TABLE || tableName == CATALOG_INFO;
}

bool RelationManager::isSystemTuple(const string tableName, const RID rid) {
    if (tableName != TABLES_TABLE && tableName != COLUMNS_TABLE) {
        return false;
    }
    void *returnedData = malloc(PAGE_SIZE);
    readAttribute(tableName, rid, SYSTEM_FLAG, returnedData);
    int systemFlag = *((int *) ((char *)returnedData + getByteOfNullsIndicator(1)));
    free(returnedData);
    return systemFlag == 1;
}

RC RelationManager::prepareRecordDescriptor(const string tableName, vector<Attribute> &recordDescriptor) {
    if (tableName == COLUMNS_TABLE) {
        prepareRecordDescriptorForColumnsTable(recordDescriptor);
    } else if (tableName == TABLES_TABLE) {
        prepareRecordDescriptorForTablesTable(recordDescriptor);
    } else {
        if (getAttributes(tableName, recordDescriptor) == FAIL) { return FAIL; }
    }
    return SUCCESS;
}


