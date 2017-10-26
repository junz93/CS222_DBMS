
#include "rm.h"
#include "util.h"


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
    prepareTupleForTables(TABLES_ATTR_NUM, tableId, tableName, false, NATIVE_VERSION, tuple);
    insertCatalogTuple(TABLES_TABLE, tuple, rid);
    // write schema to "Columns" table
    int columnPosition = 0;
    for (const Attribute &attribute : attrs) {
        prepareTupleForColumns(COLUMNS_ATTR_NUM, tableId, attribute.name, attribute.type, attribute.length,
                               ++columnPosition, false, NATIVE_VERSION, tuple);
        insertCatalogTuple(COLUMNS_TABLE, tuple, rid);
    }
    // Plus last table id by 1 in catalog_information file
    updateLastTableId(tableId);

    free(tuple);

    return SUCCESS;
}

RC RelationManager::deleteTable(const string &tableName) {
    int tableId, currentVersion;
    RID rid;

    if (isSystemTable(tableName)) {
        return FAIL;
    }

    // delete schema in Catalog
    if (prepareTableIdAndTablesRid(tableName, tableId, rid) == FAIL) { return FAIL; }
    if (deleteCatalogTuple(TABLES_TABLE, rid) == FAIL) { return FAIL; }
    if (deleteTargetTableTuplesInColumnsTable(tableId) == FAIL) { return FAIL; }
    // delete table file
    if (rbfm->destroyFile(tableName) == FAIL) { return FAIL; }


    return SUCCESS;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs) {
    int tableId, currentVersion;
    priority_queue<AttributeNode, vector<AttributeNode>, cmp> attributeNodeHeap;
    RID tablesRid;

    if (prepareTableIdCurrentVersionAndTablesRid(tableName, tableId, currentVersion, tablesRid) == FAIL) {
        return FAIL;
    }
    if (prepareAttributeNodeMinHeap(tableId, currentVersion, attributeNodeHeap) == FAIL) { return FAIL; }

    // prepare attrs ordered by column position
    while(!attributeNodeHeap.empty()) {
        auto attributeNode = attributeNodeHeap.top();
        attributeNodeHeap.pop();
        attrs.push_back(attributeNode.attribute);
    }

    return SUCCESS;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid) {
    FileHandle fileHandle;
    vector<Attribute> recordDescriptor;
    int currentVersion;
    void *insertData = malloc(PAGE_SIZE);

    if (isSystemTable(tableName)) {
        return FAIL;
    }

    if (rbfm->openFile(tableName, fileHandle) == FAIL) {
        return FAIL;
    }

    if (prepareRecordDescriptor(tableName, recordDescriptor) == FAIL) { return FAIL; }
    if (prepareCurrentVersion(tableName, currentVersion) == FAIL) { return FAIL; }

    int formerRecordFieldNumber = recordDescriptor.size();
    if (formerRecordFieldNumber % 8 == 0) {
        int formerNullFlagLength = getByteOfNullsIndicator(formerRecordFieldNumber);
        int nullFlagLength = formerNullFlagLength + 1;
        memcpy(insertData, data, formerNullFlagLength);
        memset((char *) insertData + formerNullFlagLength, 0x00, 1);
        int dataLength = getDataLength(recordDescriptor, data);
        memcpy((char *) insertData + nullFlagLength, (char *) data + formerNullFlagLength,
               dataLength - formerNullFlagLength);
        memcpy((char *) insertData + dataLength + 1, &currentVersion, sizeof(int));
    } else {
        int dataLength = getDataLength(recordDescriptor, data);
        memcpy(insertData, data, dataLength);
        memcpy((char *) insertData + dataLength, &currentVersion, sizeof(int));
    }

    vector<Attribute> newRecordDescriptor = getNewRecordDescriptor(recordDescriptor);

    if (rbfm->insertRecord(fileHandle, newRecordDescriptor, insertData, rid) == FAIL) {
        return FAIL;
    }
    free(insertData);

    // print inserted record for debugging
    void *readData = malloc(PAGE_SIZE);
    rbfm->readRecord(fileHandle, newRecordDescriptor, rid, readData);
    rbfm->printRecord(newRecordDescriptor, readData);
    free(readData);

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
    int tupleVersion, currentVersion;
    void *versionData = malloc(PAGE_SIZE);
    if (rbfm->openFile(tableName, fileHandle) == FAIL) {
        return FAIL;
    }
    if (prepareRecordDescriptor(tableName, recordDescriptor) == FAIL) {
        return FAIL;
    }

    if (isSystemTable(tableName)) {
        if (rbfm->readRecord(fileHandle, recordDescriptor, rid, data) == FAIL) { return FAIL; }
        rbfm->closeFile(fileHandle);
        return SUCCESS;
    }

    readAttribute(tableName, rid, VERSION, versionData);
    tupleVersion = *((int *) ((char *) versionData + 1));
    free(versionData);
    if (prepareCurrentVersion(tableName, currentVersion) == FAIL) { return FAIL; }
    if (tupleVersion == currentVersion) {
        if (rbfm->readRecord(fileHandle, getNewRecordDescriptor(recordDescriptor), rid, data) == FAIL) { return FAIL; }
    } else {
        if (getAttributesByVersion(tableName, tupleVersion, recordDescriptor) == FAIL) { return FAIL; }
        if (rbfm->readRecord(fileHandle, getNewRecordDescriptor(recordDescriptor), rid, data) == FAIL) { return FAIL; }
        // TODO: transfer data to current version format
    }

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
    if (!isSystemTable(tableName)) {
        recordDescriptor = getNewRecordDescriptor(recordDescriptor);
    }
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
    if (isSystemTable(tableName)) {
        return FAIL;
    }

    RID tablesRid;
    int tableId;
    int oldVersion, newVersion;

    prepareTableIdCurrentVersionAndTablesRid(tableName, tableId, oldVersion, tablesRid);
    // update Tables tuple by making version plus 1
    void *data = malloc(PAGE_SIZE);
    if (readTuple(TABLES_TABLE, tablesRid, data) == FAIL) { return FAIL; }
    int nullFlagLength = getByteOfNullsIndicator(TABLES_ATTR_NUM);
    int nameLength = *((uint32_t *) ((char *) data + nullFlagLength + sizeof(int)));
    int versionOffset = nullFlagLength + sizeof(int) + 2 * sizeof(uint32_t) + 2 * nameLength + sizeof(int);
    newVersion = oldVersion + 1;
    *((uint32_t *) ((char *) data + versionOffset)) = newVersion;
    if (updateCatalogTuple(TABLES_TABLE, data, tablesRid) == FAIL) { return FAIL; }
    free(data);
    // update Columns table
    RID columnsRid;
    RM_ScanIterator rm_scanIterator;
    void *returnedData = malloc(PAGE_SIZE);
    vector<string> attributeNames;
    int largestColumnPos = 0;
    attributeNames.push_back(TABLE_ID);
    attributeNames.push_back(COLUMN_NAME);
    attributeNames.push_back(COLUMN_TYPE);
    attributeNames.push_back(COLUMN_LENGTH);
    attributeNames.push_back(COLUMN_POSITION);
    attributeNames.push_back(SYSTEM_FLAG);
    attributeNames.push_back(VERSION);
    int nullFieldIndicatorSize = getByteOfNullsIndicator(attributeNames.size());
    if (scan(COLUMNS_TABLE, TABLE_ID, EQ_OP, getScanValue(tableId), attributeNames, rm_scanIterator) == FAIL) {
        return FAIL;
    }
    while (rm_scanIterator.getNextTuple(columnsRid, returnedData) != RM_EOF) {
        int nameLength = *((uint32_t *) ((char *) returnedData + nullFlagLength + sizeof(int)));
        int columnNameOffset = nullFieldIndicatorSize + sizeof(int) + sizeof(uint32_t);
        int columnPositionOffset =
                nullFieldIndicatorSize + sizeof(int) + sizeof(uint32_t) + nameLength + 2 * sizeof(int);
        int versionOffset = nullFieldIndicatorSize + sizeof(int) + sizeof(uint32_t) + nameLength + 4 * sizeof(int);
        if (*((uint32_t *) ((char *) returnedData + versionOffset)) == oldVersion) {
            largestColumnPos = max(largestColumnPos, *((int *) ((char *) returnedData + columnPositionOffset)));
            *((uint32_t *) ((char *) returnedData + versionOffset)) = newVersion;
            byte columnNameChars[nameLength];
            memcpy(columnNameChars, (char *) returnedData + columnNameOffset, nameLength);
            string columnName(columnNameChars);
            if (columnName == attributeName) {
                continue;
            }
            RID rid;
            if (insertCatalogTuple(COLUMNS_TABLE, returnedData, rid) == FAIL) {
                return FAIL;
            }
        }
    }
    free(returnedData);
    rm_scanIterator.close();
    // insert new attribute tuple to Columns table


    return SUCCESS;
}

// Extra credit work
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr) {
    if (isSystemTable(tableName)) {
        return FAIL;
    }

    RID tablesRid;
    int tableId;
    int oldVersion, newVersion;

    prepareTableIdCurrentVersionAndTablesRid(tableName, tableId, oldVersion, tablesRid);
    // update Tables tuple by making version plus 1
    void *data = malloc(PAGE_SIZE);
    if (readTuple(TABLES_TABLE, tablesRid, data) == FAIL) { return FAIL; }
    int nullFlagLength = getByteOfNullsIndicator(TABLES_ATTR_NUM);
    int nameLength = *((uint32_t *) ((char *) data + nullFlagLength + sizeof(int)));
    int versionOffset = nullFlagLength + sizeof(int) + 2 * sizeof(uint32_t) + 2 * nameLength + sizeof(int);
    newVersion = oldVersion + 1;
    *((uint32_t *) ((char *) data + versionOffset)) = newVersion;
    if (updateCatalogTuple(TABLES_TABLE, data, tablesRid) == FAIL) { return FAIL; }
    free(data);
    // update Columns table
    RID columnsRid;
    RM_ScanIterator rm_scanIterator;
    void *returnedData = malloc(PAGE_SIZE);
    vector<string> attributeNames;
    int largestColumnPos = 0;
    attributeNames.push_back(TABLE_ID);
    attributeNames.push_back(COLUMN_NAME);
    attributeNames.push_back(COLUMN_TYPE);
    attributeNames.push_back(COLUMN_LENGTH);
    attributeNames.push_back(COLUMN_POSITION);
    attributeNames.push_back(SYSTEM_FLAG);
    attributeNames.push_back(VERSION);
    int nullFieldIndicatorSize = getByteOfNullsIndicator(attributeNames.size());
    if (scan(COLUMNS_TABLE, TABLE_ID, EQ_OP, getScanValue(tableId), attributeNames, rm_scanIterator) == FAIL) {
        return FAIL;
    }
    while (rm_scanIterator.getNextTuple(columnsRid, returnedData) != RM_EOF) {
        int nameLength = *((uint32_t *) ((char *) returnedData + nullFlagLength + sizeof(int)));
        int columnPositionOffset =
                nullFieldIndicatorSize + sizeof(int) + sizeof(uint32_t) + nameLength + 2 * sizeof(int);
        int versionOffset = nullFieldIndicatorSize + sizeof(int) + sizeof(uint32_t) + nameLength + 4 * sizeof(int);
        if (*((uint32_t *) ((char *) returnedData + versionOffset)) == oldVersion) {
            largestColumnPos = max(largestColumnPos, *((int *) ((char *) returnedData + columnPositionOffset)));
            *((uint32_t *) ((char *) returnedData + versionOffset)) = newVersion;
            RID rid;
            if (insertCatalogTuple(COLUMNS_TABLE, returnedData, rid) == FAIL) {
                return FAIL;
            }
        }
    }
    free(returnedData);
    rm_scanIterator.close();
    // insert new attribute tuple to Columns table
    void *tuple = malloc(PAGE_SIZE);
    prepareTupleForColumns(COLUMNS_ATTR_NUM, tableId, attr.name, attr.type, attr.length, largestColumnPos + 1, false,
                           newVersion, tuple);
    insertCatalogTuple(COLUMNS_TABLE, tuple, columnsRid);
    free(tuple);

    return SUCCESS;
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

    // print inserted record for debugging
    void *dataToBeRead = malloc(PAGE_SIZE);
    rbfm->readRecord(fileHandle, recordDescriptor, rid, dataToBeRead);
    rbfm->printRecord(recordDescriptor, dataToBeRead);
    free(dataToBeRead);

    rbfm->closeFile(fileHandle);

    return SUCCESS;
}

void RelationManager::initializeTablesTable() {
    RID rid;
    void *tuple = malloc(PAGE_SIZE);

    prepareTupleForTables(TABLES_ATTR_NUM, TABLES_ID, TABLES_TABLE, true, NATIVE_VERSION, tuple);
    insertCatalogTuple(TABLES_TABLE, tuple, rid);

    prepareTupleForTables(TABLES_ATTR_NUM, COLUMNS_ID, COLUMNS_TABLE, true, NATIVE_VERSION, tuple);
    insertCatalogTuple(TABLES_TABLE, tuple, rid);

    free(tuple);
}

void RelationManager::initializeColumnsTable() {
    RID rid;
    void *tuple = malloc(PAGE_SIZE);

    prepareTupleForColumns(COLUMNS_ATTR_NUM, TABLES_ID, TABLE_ID, TypeInt, 4, 1, true, NATIVE_VERSION, tuple);
    insertCatalogTuple(COLUMNS_TABLE, tuple, rid);
    prepareTupleForColumns(COLUMNS_ATTR_NUM, TABLES_ID, TABLE_NAME, TypeVarChar, 50, 2, true, NATIVE_VERSION, tuple);
    insertCatalogTuple(COLUMNS_TABLE, tuple, rid);
    prepareTupleForColumns(COLUMNS_ATTR_NUM, TABLES_ID, FILE_NAME, TypeVarChar, 50, 3, true, NATIVE_VERSION, tuple);
    insertCatalogTuple(COLUMNS_TABLE, tuple, rid);
    prepareTupleForColumns(COLUMNS_ATTR_NUM, TABLES_ID, SYSTEM_FLAG, TypeInt, 4, 4, true, NATIVE_VERSION, tuple);
    insertCatalogTuple(COLUMNS_TABLE, tuple, rid);
    prepareTupleForColumns(COLUMNS_ATTR_NUM, TABLES_ID, VERSION, TypeInt, 4, 5, true, NATIVE_VERSION, tuple);
    insertCatalogTuple(COLUMNS_TABLE, tuple, rid);

    prepareTupleForColumns(COLUMNS_ATTR_NUM, COLUMNS_ID, TABLE_ID, TypeInt, 4, 1, true, NATIVE_VERSION, tuple);
    insertCatalogTuple(COLUMNS_TABLE, tuple, rid);
    prepareTupleForColumns(COLUMNS_ATTR_NUM, COLUMNS_ID, COLUMN_NAME, TypeVarChar, 50, 2, true, NATIVE_VERSION, tuple);
    insertCatalogTuple(COLUMNS_TABLE, tuple, rid);
    prepareTupleForColumns(COLUMNS_ATTR_NUM, COLUMNS_ID, COLUMN_TYPE, TypeInt, 4, 3, true, NATIVE_VERSION, tuple);
    insertCatalogTuple(COLUMNS_TABLE, tuple, rid);
    prepareTupleForColumns(COLUMNS_ATTR_NUM, COLUMNS_ID, COLUMN_LENGTH, TypeInt, 4, 4, true, NATIVE_VERSION, tuple);
    insertCatalogTuple(COLUMNS_TABLE, tuple, rid);
    prepareTupleForColumns(COLUMNS_ATTR_NUM, COLUMNS_ID, COLUMN_POSITION, TypeInt, 4, 5, true, NATIVE_VERSION, tuple);
    insertCatalogTuple(COLUMNS_TABLE, tuple, rid);
    prepareTupleForColumns(COLUMNS_ATTR_NUM, COLUMNS_ID, SYSTEM_FLAG, TypeInt, 4, 6, true, NATIVE_VERSION, tuple);
    insertCatalogTuple(COLUMNS_TABLE, tuple, rid);
    prepareTupleForColumns(COLUMNS_ATTR_NUM, COLUMNS_ID, VERSION, TypeInt, 4, 7, true, NATIVE_VERSION, tuple);
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

    attribute.name = VERSION;
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

    attribute.name = VERSION;
    attribute.type = TypeInt;
    attribute.length = (AttrLength) 4;
    recordDescriptor.push_back(attribute);

}

/**  private functions called by getAttributes **/
RC RelationManager::getAttributesByVersion(const string &tableName, const int version, vector<Attribute> &attrs) {
    int tableId;
    priority_queue<AttributeNode, vector<AttributeNode>, cmp> attributeNodeHeap;
    RID tablesRid;

    if (prepareTableIdAndTablesRid(tableName, tableId, tablesRid) == FAIL) {
        return FAIL;
    }
    if (prepareAttributeNodeMinHeap(tableId, version, attributeNodeHeap) == FAIL) { return FAIL; }

    while(!attributeNodeHeap.empty()) {
        auto attributeNode = attributeNodeHeap.top();
        attributeNodeHeap.pop();
        attrs.push_back(attributeNode.attribute);
    }

    return SUCCESS;
}

/** private functions for reading and writing metadata **/
RC RelationManager::prepareCurrentVersion(const string tableName, int &version) {
    RM_ScanIterator rm_scanIterator;
    RID rid;
    void *returnedData = malloc(PAGE_SIZE);
    void *scanValueOfTableName = malloc(tableName.size() + sizeof(uint32_t));
    vector<string> attributeNames;
    attributeNames.push_back(VERSION);
    int nullFlagLength = getByteOfNullsIndicator(attributeNames.size());

    prepareScanValue(tableName, scanValueOfTableName);
    if (scan(TABLES_TABLE, TABLE_NAME, EQ_OP, scanValueOfTableName, attributeNames, rm_scanIterator) == FAIL) {
        return FAIL;
    }
    if (rm_scanIterator.getNextTuple(rid, returnedData) == RM_EOF) { return FAIL; }
    version = *((int *) ((char *) returnedData + nullFlagLength));
    free(returnedData);
    free(scanValueOfTableName);
    rm_scanIterator.close();
    return SUCCESS;
}

RC RelationManager::prepareTableIdAndTablesRid(const string tableName, int &tableId, RID &rid) {
    RM_ScanIterator rm_scanIterator;
    void *returnedData = malloc(PAGE_SIZE);
    void *scanValueOfTableName = malloc(tableName.size() + sizeof(uint32_t));
    vector<string> attributeNames;
    attributeNames.push_back(TABLE_ID);
    int nullFlagLength = getByteOfNullsIndicator(attributeNames.size());

    prepareScanValue(tableName, scanValueOfTableName);
    if (scan(TABLES_TABLE, TABLE_NAME, EQ_OP, scanValueOfTableName, attributeNames, rm_scanIterator) == FAIL) {
        return FAIL;
    }
    if (rm_scanIterator.getNextTuple(rid, returnedData) == RM_EOF) { return FAIL; }
    tableId = *((int *) ((char *) returnedData + nullFlagLength));
    free(returnedData);
    free(scanValueOfTableName);
    rm_scanIterator.close();
    return SUCCESS;
}

RC RelationManager::prepareTableIdCurrentVersionAndTablesRid(const string tableName, int &tableId, int &version,
                                                             RID &rid) {
    RM_ScanIterator rm_scanIterator;
    void *returnedData = malloc(PAGE_SIZE);
    void *scanValueOfTableName = malloc(tableName.size() + sizeof(uint32_t));
    vector<string> attributeNames;
    attributeNames.push_back(TABLE_ID);
    attributeNames.push_back(VERSION);
    int nullFlagLength = getByteOfNullsIndicator(attributeNames.size());

    prepareScanValue(tableName, scanValueOfTableName);
    if (scan(TABLES_TABLE, TABLE_NAME, EQ_OP, scanValueOfTableName, attributeNames, rm_scanIterator) == FAIL) {
        return FAIL;
    }
    if (rm_scanIterator.getNextTuple(rid, returnedData) == RM_EOF) { return FAIL; }
    tableId = *((int *) ((char *) returnedData + nullFlagLength));
    version = *((int *) ((char *) returnedData + nullFlagLength + sizeof(int)));
    free(returnedData);
    free(scanValueOfTableName);
    rm_scanIterator.close();
    return SUCCESS;
}

/*RC RelationManager::preparePositionAttributeMap(int tableId, const int version,
                                                unordered_map<int, Attribute> &positionAttributeMap) {
    RID rid;
    RM_ScanIterator rm_scanIterator;
    void *returnedData = malloc(PAGE_SIZE);
    vector<string> attributeNames;
    attributeNames.push_back(VERSION);
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

        int tupleVersion = *(int *) ((char *) returnedData + offset + nullFieldIndicatorSize);
        // skip attribute of wrong version
        if (tupleVersion != version) {
            continue;
        }
        offset += sizeof(int);

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
}*/

RC RelationManager::prepareAttributeNodeMinHeap(int tableId, const int version,
                                                priority_queue<AttributeNode, vector<AttributeNode>, cmp> &attributeNodeHeap) {
    RID rid;
    RM_ScanIterator rm_scanIterator;
    void *returnedData = malloc(PAGE_SIZE);
    vector<string> attributeNames;
    attributeNames.push_back(VERSION);
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

        int tupleVersion = *(int *) ((char *) returnedData + offset + nullFieldIndicatorSize);
        // skip attribute of wrong version
        if (tupleVersion != version) {
            continue;
        }
        offset += sizeof(int);

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
        attributeNodeHeap.push(AttributeNode(columnPositon, attribute));
        offset += sizeof(int);
    }
    free(returnedData);
    rm_scanIterator.close();
    return SUCCESS;
}

RC RelationManager::deleteTargetTableTuplesInColumnsTable(int tableId) {
    RID rid;
    RM_ScanIterator rm_scanIterator;
    void *returnedData = malloc(PAGE_SIZE);
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

RC RelationManager::updateCatalogTuple(const string &tableName, const void *data, const RID &rid) {
    FileHandle fileHandle;
    vector<Attribute> recordDescriptor;

    if (isSystemTuple(tableName, rid)) {
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
    int systemFlag = *((int *) ((char *) returnedData + getByteOfNullsIndicator(1)));
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

unsigned RelationManager::getDataLength(const vector<Attribute> &recordDescriptor, const void *data) {
    auto numOfFields = recordDescriptor.size();
    unsigned dataLength = getByteOfNullsIndicator(numOfFields);
    const byte *pFlag = (const byte *) data;         // pointer to null flags
    const byte *pData = pFlag + getByteOfNullsIndicator(numOfFields);  // pointer to actual field data
    uint8_t nullFlag = 0x80;     // cannot use (signed) byte

    // compute the length of the new record
    for (const Attribute &attr : recordDescriptor) {
        if (!(*pFlag & nullFlag)) {
            switch (attr.type) {
                case TypeInt:
                case TypeReal:
                    dataLength += attr.length;
                    pData += attr.length;
                    break;
                case TypeVarChar:
                    uint32_t length = *((const uint32_t *) pData);
                    dataLength += 4 + length;
                    pData += 4 + length;
                    break;
            }
        }

        if (nullFlag == 0x01) {
            nullFlag = 0x80;
            ++pFlag;
        } else {
            nullFlag = nullFlag >> 1;
        }
    }

    return dataLength;
}

vector<Attribute> RelationManager::getNewRecordDescriptor(const vector<Attribute> recordDescriptor) {
    vector<Attribute> newRecordDescriptor = recordDescriptor;
    Attribute attribute;
    attribute.name = VERSION;
    attribute.type = TypeInt;
    attribute.length = sizeof(int);
    newRecordDescriptor.push_back(attribute);
    return newRecordDescriptor;
}


