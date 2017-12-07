#include <cstring>
#include "rm.h"

RelationManager *RelationManager::_rm = nullptr;

RelationManager *RelationManager::instance() {
    if (!_rm)
        _rm = new RelationManager();

    return _rm;
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
    if (rbfm->createFile(INDICES_TABLE) == FAIL) {
        rbfm->destroyFile(INDICES_TABLE);
        return FAIL;
    }

    // initialize catalog tables
    initializeTablesTable();
    initializeColumnsTable();

    // create catalog information file and store last table id
    ofstream file(CATALOG_INFO, fstream::out | fstream::binary);
    updateLastTableId(INDICES_ID);

    return SUCCESS;
}

RC RelationManager::deleteCatalog() {
    if (rbfm->destroyFile(TABLES_TABLE) == FAIL || rbfm->destroyFile(COLUMNS_TABLE) == FAIL
        || rbfm->destroyFile(CATALOG_INFO) == FAIL || rbfm->destroyFile(INDICES_TABLE) == FAIL) {
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
    vector<Index> relatedIndices;

    if (isSystemTable(tableName)) {
        return FAIL;
    }

    prepareRelatedIndices(tableName, relatedIndices);
    // delete schema in Catalog
    if (prepareTableIdAndTablesRid(tableName, tableId, rid) == FAIL) { return FAIL; }
    if (deleteCatalogTuple(TABLES_TABLE, rid) == FAIL) { return FAIL; }
    if (deleteTargetTableTuplesInColumnsTable(tableId) == FAIL) { return FAIL; }
    if (deleteRelatedIndicesTableTuples(tableName) == FAIL) { return FAIL; }
    // delete table file
    if (rbfm->destroyFile(tableName) == FAIL) { return FAIL; }
    // delete index files
    if (deleteRelatedIndexFiles(relatedIndices) == FAIL) { return FAIL; }

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
    vector<Index> relatedIndices;

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
    prepareRelatedIndices(tableName, relatedIndices);
    insertEntriesToRelatedIndices(relatedIndices, recordDescriptor, data, rid);

    rbfm->closeFile(fileHandle);

    return SUCCESS;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid) {
    FileHandle fileHandle;
    vector<Attribute> recordDescriptor;
    vector<Index> relatedIndices;
    void *data = malloc(PAGE_SIZE);

    if (isSystemTable(tableName) || isSystemTuple(tableName, rid)) {
        return FAIL;
    }
    if (rbfm->openFile(tableName, fileHandle) == FAIL) {
        return FAIL;
    }
    prepareRecordDescriptor(tableName, recordDescriptor);
    if (rbfm->readRecord(fileHandle, recordDescriptor, rid, data) == FAIL) {
        return FAIL;
    }
    if (rbfm->deleteRecord(fileHandle, recordDescriptor, rid) == FAIL) {
        return FAIL;
    }
    prepareRelatedIndices(tableName, relatedIndices);
    deleteEntriesToRelatedIndices(relatedIndices, recordDescriptor, data);
    rbfm->closeFile(fileHandle);
    free(data);

    return SUCCESS;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid) {
    FileHandle fileHandle;
    vector<Attribute> recordDescriptor;
    vector<Index> relatedIndices;
    void *oldData = malloc(PAGE_SIZE);

    if (isSystemTable(tableName) || isSystemTuple(tableName, rid)) {
        return FAIL;
    }

    if (rbfm->openFile(tableName, fileHandle) == FAIL) {
        return FAIL;
    }
    prepareRecordDescriptor(tableName, recordDescriptor);
    if (rbfm->readRecord(fileHandle, recordDescriptor, rid, oldData) == FAIL) {
        return FAIL;
    }
    if (rbfm->updateRecord(fileHandle, recordDescriptor, data, rid) == FAIL) {
        return FAIL;
    }
    prepareRelatedIndices(tableName, relatedIndices);
    deleteEntriesToRelatedIndices(relatedIndices, recordDescriptor, oldData);
    insertEntriesToRelatedIndices(relatedIndices, recordDescriptor, data, rid);
    rbfm->closeFile(fileHandle);
    free(oldData);

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
    FileHandle fileHandle;
    vector<Attribute> recordDescriptor;
    RBFM_ScanIterator &rbfm_scanIterator = rm_ScanIterator.rbfm_scanIterator;

    if (rbfm->openFile(tableName, fileHandle) == FAIL) { return FAIL; }

    prepareRecordDescriptor(tableName, recordDescriptor);
    rbfm->scan(fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames, rbfm_scanIterator);

    return SUCCESS;
}

RC RelationManager::createIndex(const string &tableName, const string &attributeName) {
    RID rid;
    void *tuple = malloc(PAGE_SIZE);
    string indexName = tableName + "：" + attributeName;

    if (ix->createFile(indexName) == FAIL) {
        return FAIL;
    }
    prepareTupleForIndices(INDICES_ATTR_NUM, indexName, attributeName, tableName, true, tuple);
    if (insertCatalogTuple(INDICES_TABLE, tuple, rid) == FAIL) {
        return FAIL;
    }
    if (populateIndex(tableName, attributeName) == FAIL) {
        return FAIL;
    }
    free(tuple);

    return SUCCESS;
}

RC RelationManager::populateIndex(const string tableName, const string attributeName) {
    string indexName = tableName + "：" + attributeName;
    RID rid;
    RM_ScanIterator rm_scanIterator;
    IXFileHandle ixFileHandle;
    void *returnedData = malloc(PAGE_SIZE);
    void *key = malloc(PAGE_SIZE);
    Attribute attribute;
    vector<string> attributeNames;
    vector<Attribute> recordDescriptor;
    if (getAttributes(tableName, recordDescriptor) == FAIL) { return FAIL; }
    for (Attribute attr : recordDescriptor) {
        attributeNames.push_back(attr.name);
    }

    if (ix->openFile(indexName, ixFileHandle) == FAIL) {
        return FAIL;
    }
    if (scan(tableName, "", NO_OP, NULL, attributeNames, rm_scanIterator) == FAIL) {
        return FAIL;
    }
    while (rm_scanIterator.getNextTuple(rid, returnedData) != RM_EOF) {
        if (prepareKeyAndAttribute(recordDescriptor, returnedData, attributeName, key, attribute) == FAIL) {
            return FAIL;
        }
        if (ix->insertEntry(ixFileHandle, attribute, key, rid) == FAIL) { return FAIL; }
    }
    free(returnedData);
    free(key);
    rm_scanIterator.close();

    return SUCCESS;
}

RC RelationManager::destroyIndex(const string &tableName, const string &attributeName) {
    RID rid;
    string indexName = tableName + "：" + attributeName;

    if (ix->destroyFile(indexName) == FAIL) {
        return FAIL;
    }
    if (prepareIndexRid(indexName, rid) == FAIL || deleteCatalogTuple(INDICES_TABLE, rid) == FAIL) {
        return FAIL;
    }

    return SUCCESS;
}

RC RelationManager::indexScan(const string &tableName,
                              const string &attributeName,
                              const void *lowKey,
                              const void *highKey,
                              bool lowKeyInclusive,
                              bool highKeyInclusive,
                              RM_IndexScanIterator &rm_IndexScanIterator) {
    IXFileHandle ixfileHandle;
    IX_ScanIterator &ix_ScanIterator = rm_IndexScanIterator.ix_scanIterator;
    int tableId;
    RID rid;
    Attribute attribute;
    string indexName = tableName + "：" + attributeName;

    if (ix->openFile(indexName, ixfileHandle) == FAIL) {
        return FAIL;
    }
    if (prepareTableIdAndTablesRid(tableName, tableId, rid) == FAIL) {
        return FAIL;
    }
    attribute = getAttribute(attributeName, tableId);
    if (attribute.length == 0) {
        return FAIL;
    }
    if (ix->scan(ixfileHandle, attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive, ix_ScanIterator)
        == FAIL) {
        return FAIL;
    }
//    rm_IndexScanIterator.ix_scanIterator = ix_ScanIterator;

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

    prepareTupleForTables(TABLES_ATTR_NUM, INDICES_ID, INDICES_TABLE, true, tuple);
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

    prepareTupleForColumns(COLUMNS_ATTR_NUM, INDICES_ID, INDEX_NAME, TypeVarChar, 50, 1, true, tuple);
    insertCatalogTuple(COLUMNS_TABLE, tuple, rid);
    prepareTupleForColumns(COLUMNS_ATTR_NUM, INDICES_ID, ATTRIBUTE_NAME, TypeVarChar, 50, 2, true, tuple);
    insertCatalogTuple(COLUMNS_TABLE, tuple, rid);
    prepareTupleForColumns(COLUMNS_ATTR_NUM, INDICES_ID, TABLE_NAME, TypeVarChar, 50, 3, true, tuple);
    insertCatalogTuple(COLUMNS_TABLE, tuple, rid);
    prepareTupleForColumns(COLUMNS_ATTR_NUM, INDICES_ID, SYSTEM_FLAG, TypeInt, 4, 4, true, tuple);
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
RC RelationManager::prepareTableIdAndTablesRid(const string &tableName, int &tableId, RID &rid) {
    RM_ScanIterator rm_scanIterator;
    void *returnedData = malloc(PAGE_SIZE);
    void *scanValueOfTableName = malloc(tableName.size() + 4);
    vector<string> attributeNames;
    attributeNames.push_back(TABLE_ID);

    prepareScanValue(tableName, scanValueOfTableName);
    if (scan(TABLES_TABLE, TABLE_NAME, EQ_OP, scanValueOfTableName, attributeNames, rm_scanIterator) == FAIL) {
        return FAIL;
    }
    if (rm_scanIterator.getNextTuple(rid, returnedData) == RM_EOF) { return FAIL; }
    tableId = *((int *) ((char *) returnedData + getBytesOfNullIndicator(attributeNames.size())));
    free(scanValueOfTableName);
    free(returnedData);
    rm_scanIterator.close();
    return SUCCESS;
}

RC RelationManager::prepareIndexRid(const string &indexName, RID &rid) {
    RM_ScanIterator rm_scanIterator;
    void *returnedData = malloc(PAGE_SIZE);
    void *scanValueOfIndexName = malloc(indexName.size() + 4);
    vector<string> attributeNames;
    attributeNames.push_back(INDEX_NAME);

    prepareScanValue(indexName, scanValueOfIndexName);
    if (scan(INDICES_TABLE, INDEX_NAME, EQ_OP, scanValueOfIndexName, attributeNames, rm_scanIterator) == FAIL) {
        return FAIL;
    }
    if (rm_scanIterator.getNextTuple(rid, returnedData) == RM_EOF) { return FAIL; }
    free(scanValueOfIndexName);
    free(returnedData);
    rm_scanIterator.close();
    return SUCCESS;
}

RC RelationManager::preparePositionAttributeMap(int tableId, unordered_map<int, Attribute> &positionAttributeMap) {
    RID rid;
    RM_ScanIterator rm_scanIterator;
    void *returnedData = malloc(PAGE_SIZE);
    vector<string> attributeNames;
    attributeNames.push_back(COLUMN_NAME);
    attributeNames.push_back(COLUMN_TYPE);
    attributeNames.push_back(COLUMN_LENGTH);
    attributeNames.push_back(COLUMN_POSITION);
    int nullFieldIndicatorSize = getBytesOfNullIndicator(attributeNames.size());

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
    int nullFieldIndicatorSize = getBytesOfNullIndicator(attributeNames.size());

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

RC RelationManager::deleteRelatedIndicesTableTuples(const string &tableName) {
    RID rid;
    RM_ScanIterator rm_scanIterator;
    void *returnedData = malloc(200);
    void *scanValueOfTableName = malloc(tableName.size() + 4);
    vector<string> attributeNames;
    attributeNames.push_back(INDEX_NAME);

    prepareScanValue(tableName, scanValueOfTableName);
    if (scan(INDICES_TABLE, TABLE_NAME, EQ_OP, scanValueOfTableName, attributeNames, rm_scanIterator) == FAIL) {
        return FAIL;
    }
    while (rm_scanIterator.getNextTuple(rid, returnedData) != RM_EOF) {
        deleteCatalogTuple(INDICES_TABLE, rid);
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
Attribute RelationManager::getAttribute(const string &attributeName, int tableId) {
    Attribute attribute;
    attribute.length = 0;
    RID rid;
    RM_ScanIterator rm_scanIterator;
    void *returnedData = malloc(PAGE_SIZE);
    vector<string> attributeNames;
    attributeNames.push_back(COLUMN_NAME);
    attributeNames.push_back(COLUMN_TYPE);
    attributeNames.push_back(COLUMN_LENGTH);
//    int offset = getBytesOfNullIndicator(attributeNames.size());

    if (scan(COLUMNS_TABLE, TABLE_ID, EQ_OP, &tableId, attributeNames, rm_scanIterator) == FAIL) {
        return attribute;
    }
    while (rm_scanIterator.getNextTuple(rid, returnedData) != RM_EOF) {
        int offset = getBytesOfNullIndicator(attributeNames.size());
        int nameLength = *((int *) ((char *) returnedData + offset));
        offset += sizeof(int);
        string currentAttributeName((char *) returnedData + offset, nameLength);
        if (currentAttributeName == attributeName) {
            attribute.name = attributeName;
            offset += nameLength;
            attribute.type = *((AttrType *) ((char *) returnedData + offset));
            offset += sizeof(AttrType); //
            attribute.length = *((AttrLength *) ((char *) returnedData + offset));
            offset += sizeof(AttrLength);
            break;
        }
    }
    free(returnedData);
    rm_scanIterator.close();
    return attribute;
}

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

bool RelationManager::isSystemTable(const string &tableName) {
    return tableName == TABLES_TABLE || tableName == COLUMNS_TABLE || tableName == CATALOG_INFO;
}

bool RelationManager::isSystemTuple(const string &tableName, const RID &rid) {
    if (tableName != TABLES_TABLE && tableName != COLUMNS_TABLE) {
        return false;
    }
    void *returnedData = malloc(PAGE_SIZE);
    readAttribute(tableName, rid, SYSTEM_FLAG, returnedData);
    int systemFlag = *((int *) ((char *) returnedData + getBytesOfNullIndicator(1)));
    free(returnedData);
    return systemFlag == 1;
}

RC RelationManager::prepareRecordDescriptor(const string &tableName, vector<Attribute> &recordDescriptor) {
    if (tableName == COLUMNS_TABLE) {
        prepareRecordDescriptorForColumnsTable(recordDescriptor);
    } else if (tableName == TABLES_TABLE) {
        prepareRecordDescriptorForTablesTable(recordDescriptor);
    } else {
        if (getAttributes(tableName, recordDescriptor) == FAIL) { return FAIL; }
    }
    return SUCCESS;
}

RC RelationManager::prepareRelatedIndices(const string &tableName, vector<Index> &relatedIndices) {
    RID rid;
    RM_ScanIterator rm_scanIterator;
    void *returnedData = malloc(PAGE_SIZE);
    void *scanValueOfTableName = malloc(tableName.size() + 4);
    vector<string> attributeNames;
    attributeNames.push_back(ATTRIBUTE_NAME);

    prepareScanValue(tableName, scanValueOfTableName);
    if (scan(INDICES_TABLE, TABLE_NAME, EQ_OP, scanValueOfTableName, attributeNames, rm_scanIterator) == FAIL) {
        return FAIL;
    }
    while (rm_scanIterator.getNextTuple(rid, returnedData) != RM_EOF) {
        Index currentIndex;
        int offset = getBytesOfNullIndicator(attributeNames.size());
        int nameLength = *((int *) ((char *) returnedData + offset));
        offset += sizeof(int);
        string currentAttributeName((char *) returnedData + offset, nameLength);
        currentIndex.attributeName = currentAttributeName;
        currentIndex.tableName = tableName;
        currentIndex.indexName = tableName + "：" + currentAttributeName;
        relatedIndices.push_back(currentIndex);
    }
    free(scanValueOfTableName);
    free(returnedData);
    rm_scanIterator.close();

    return SUCCESS;
}

RC RelationManager::insertEntriesToRelatedIndices(const vector<Index> &relatedIndices,
                                                  const vector<Attribute> &recordDescriptor,
                                                  const void *data, const RID &rid) {
    IXFileHandle ixFileHandle;
//    RID rid;
    void *key = malloc(PAGE_SIZE);
    Attribute attribute;

    for (Index relatedIndex : relatedIndices) {

        if (prepareKeyAndAttribute(recordDescriptor, data, relatedIndex.attributeName, key, attribute) == FAIL) {
            return FAIL;
        }
        if (ix->openFile(relatedIndex.indexName, ixFileHandle) == FAIL) {
            return FAIL;
        }
        if (ix->insertEntry(ixFileHandle, attribute, key, rid) == FAIL) {
            return FAIL;
        }
    }

    free(key);

    return SUCCESS;
}

RC RelationManager::deleteEntriesToRelatedIndices(const vector<Index> &relatedIndices,
                                                  const vector<Attribute> &recordDescriptor, const void *data) {
    IXFileHandle ixFileHandle;
    RID rid;
    void *key = malloc(PAGE_SIZE);
    Attribute attribute;

    for (Index relatedIndex : relatedIndices) {

        if (prepareKeyAndAttribute(recordDescriptor, data, relatedIndex.attributeName, key, attribute) == FAIL) {
            return FAIL;
        }
        if (ix->openFile(relatedIndex.indexName, ixFileHandle) == FAIL) {
            return FAIL;
        }
        if (ix->deleteEntry(ixFileHandle, attribute, key, rid) == FAIL) {
            return FAIL;
        }
    }

    free(key);

    return SUCCESS;
}

RC RelationManager::deleteRelatedIndexFiles(const vector<Index> &relatedIndices) {
    for (Index index : relatedIndices) {
        if (ix->destroyFile(index.indexName) == FAIL) {
            return FAIL;
        }
    }
    return SUCCESS;
}

RC RelationManager::prepareKeyAndAttribute(const vector<Attribute> &recordDescriptor, const void *data,
                                           const string &attributeName,
                                           void *key, Attribute &attribute) {
    int offset = getBytesOfNullIndicator(recordDescriptor.size());
    const byte *pFlag = (const byte*) data;
    uint8_t flagMask = 0x80;

    for (Attribute currentAttribute : recordDescriptor) {
        if (currentAttribute.name == attributeName) {
            attribute = currentAttribute;
            switch (currentAttribute.type) {
                case TypeInt:
                case TypeReal:
                    memcpy(key, (char *) data + offset, 4);
                    break;
                case TypeVarChar:
                    uint32_t length = *((uint32_t *) ((char *) data + offset));
                    memcpy(key, (char *) data + offset, 4 + length);
                    break;
            }
            return SUCCESS;
        } else {
            if (!(*pFlag & flagMask)) {
                switch (currentAttribute.type) {
                    case TypeInt:
                    case TypeReal:
                        offset += 4;
                        break;
                    case TypeVarChar:
                        uint32_t length = *((uint32_t *) ((char *) data + offset));
                        offset += (4 + length);
                        break;
                }
            }
            if (flagMask == 0x01) {
                flagMask = 0x80;
                ++pFlag;
            } else {
                flagMask = flagMask >> 1;
            }
        }
    }
    return FAIL;
}

void prepareTupleForTables(int attributeCount, int tableID, const string &name, int isSystemInfo, void *tuple) {
    int offset = 0;
    int nullAttributesIndicatorActualSize = getBytesOfNullIndicator(attributeCount);
    int nameLength = name.size();

    // write Null-indicator to tuple record
    memset((char *) tuple + offset, 0, nullAttributesIndicatorActualSize);
    offset += nullAttributesIndicatorActualSize;

    // write tableID to tuple record
    memcpy((char *) tuple + offset, &tableID, sizeof(int));
    offset += sizeof(int);

    // write tableName to tuple record
    memcpy((char *) tuple + offset, &nameLength, sizeof(uint32_t));
    offset += sizeof(int);
    memcpy((char *) tuple + offset, name.c_str(), nameLength);
    offset += nameLength;

    // write fileName to tuple record
    memcpy((char *) tuple + offset, &nameLength, sizeof(uint32_t));
    offset += sizeof(int);
    memcpy((char *) tuple + offset, name.c_str(), nameLength);
    offset += nameLength;

    // write isSystemInfo to tuple record
    memcpy((char *) tuple + offset, &isSystemInfo, sizeof(int));
    offset += sizeof(int);
}

void prepareTupleForColumns(int attributeCount, int tableID, const string &columnName, int columnType,
                            int columnLength, int columnPosition, int isSystemInfo, void *tuple) {
    int offset = 0;
    int nullAttributesIndicatorActualSize = getBytesOfNullIndicator(attributeCount);
    int nameLength = columnName.size();

    // write Null-indicator to tuple record
    memset((char *) tuple + offset, 0, nullAttributesIndicatorActualSize);
    offset += nullAttributesIndicatorActualSize;

    // write tableID to tuple record
    memcpy((char *) tuple + offset, &tableID, sizeof(int));
    offset += sizeof(int);

    // write columnName to tuple record
    memcpy((char *) tuple + offset, &nameLength, sizeof(uint32_t));
    offset += sizeof(int);
    memcpy((char *) tuple + offset, columnName.c_str(), nameLength);
    offset += nameLength;

    // write columnType to tuple record
    memcpy((char *) tuple + offset, &columnType, sizeof(int));
    offset += sizeof(int);

    // write columnLength to tuple record
    memcpy((char *) tuple + offset, &columnLength, sizeof(int));
    offset += sizeof(int);

    // write columnPosition to tuple record
    memcpy((char *) tuple + offset, &columnPosition, sizeof(int));
    offset += sizeof(int);

    // write isSystemInfo to tuple record
    memcpy((char *) tuple + offset, &isSystemInfo, sizeof(int));
    offset += sizeof(int);
}

void prepareTupleForIndices(int attributeCount, const string &indexName, const string &attributeName,
                            const string &tableName, int isSystemInfo, void *tuple) {
    int offset = 0;
    int nullAttributesIndicatorActualSize = getBytesOfNullIndicator(attributeCount);
    int nameLength = indexName.size();

    // write Null-indicator to tuple record
    memset((char *) tuple + offset, 0, nullAttributesIndicatorActualSize);
    offset += nullAttributesIndicatorActualSize;

    // write indexName to tuple record
    memcpy((char *) tuple + offset, &nameLength, sizeof(uint32_t));
    offset += sizeof(int);
    memcpy((char *) tuple + offset, indexName.c_str(), nameLength);
    offset += nameLength;

    // write attributeName to tuple record
    nameLength = attributeName.size();
    memcpy((char *) tuple + offset, &nameLength, sizeof(uint32_t));
    offset += sizeof(int);
    memcpy((char *) tuple + offset, attributeName.c_str(), nameLength);
    offset += nameLength;

    // write tableName to tuple record
    nameLength = tableName.size();
    memcpy((char *) tuple + offset, &nameLength, sizeof(uint32_t));
    offset += sizeof(int);
    memcpy((char *) tuple + offset, tableName.c_str(), nameLength);
    offset += nameLength;

    // write isSystemInfo to tuple record
    memcpy((char *) tuple + offset, &isSystemInfo, sizeof(int));
    offset += sizeof(int);
}


