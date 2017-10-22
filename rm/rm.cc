
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

// update lastTableId
void RelationManager::updateLastTableId(uint32_t tableId) {
    fstream file;
    file.open(CATALOG_INFO);
    void *data = malloc(sizeof(uint32_t));
    *((uint32_t *) data) = tableId;
    file.write((char *) data, sizeof(uint32_t));
    file.close();
}

uint32_t RelationManager::getLastTableId() {
    fstream file;
    file.open(CATALOG_INFO, fstream::in | fstream::out | fstream::binary);
    void *data = malloc(sizeof(uint32_t));
    file.read((char *) data, sizeof(uint32_t));
    file.close();
    return *((uint32_t *) data);
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
    insertTuple(TABLES_TABLE, tuple, rid);
    // write schema to "Columns" table
    int columnPosition = 0;
    for (const Attribute &attribute : attrs) {
        prepareTupleForColumns(COLUMNS_ATTR_NUM, tableId, attribute.name, attribute.type, attribute.length,
                               ++columnPosition, false, tuple);
        insertTuple(COLUMNS_TABLE, tuple, rid);
    }
    // Plus last table id by 1 in catalog_information file
    updateLastTableId(tableId);

    return SUCCESS;
}

RC RelationManager::deleteTable(const string &tableName) {
    if (tableName == TABLES_TABLE || tableName == COLUMNS_TABLE || tableName == CATALOG_INFO) {
        return FAIL;
    }
    if (rbfm->destroyFile(tableName) == FAIL) { return FAIL; }

    //TODO: delete schema in Catalog

    // Minus last table id by 1 in catalog_information file
    int tableId = (int) getLastTableId() - 1;
    updateLastTableId(tableId);

    return SUCCESS;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs) {
    RID rid;
    RM_ScanIterator rm_scanIterator;
    int tableId;
    map<int, Attribute> positionAttributeMap;
    void *returnedData = malloc(200);
    int nullAttributesIndicatorActualSize = getByteOfNullsIndicator(4);
    vector<string> attributes;
    attributes.push_back(TABLE_ID);

    if (scan(TABLES_TABLE, TABLE_NAME, EQ_OP, tableName.c_str(), attributes, rm_scanIterator) == FAIL) { return FAIL; }
    if (rm_scanIterator.getNextTuple(rid, returnedData) == RM_EOF) { return FAIL; }
    tableId = *((int *) ((char *) returnedData + nullAttributesIndicatorActualSize));
    attributes.clear();
    attributes.push_back(COLUMN_NAME);
    attributes.push_back(COLUMN_TYPE);
    attributes.push_back(COLUMN_LENGTH);
    attributes.push_back(COLUMN_POSITION);

    if (scan(COLUMNS_TABLE, TABLE_ID, EQ_OP, &tableId, attributes, rm_scanIterator) == FAIL) { return FAIL; }
    while(rm_scanIterator.getNextTuple(rid, returnedData) != RM_EOF)
    {
        Attribute attribute;
        int offset = 0;
        //cout << "Real Value: " << *(float *)((char *)returnedData+nullAttributesIndicatorActualSize) << endl;
        int columnNameLength = *(uint32_t *)((char *)returnedData+nullAttributesIndicatorActualSize + offset);
        offset += sizeof(uint32_t);
        char *columnName = (char *)malloc(columnNameLength + 1);
        memcpy(columnName, (char *)returnedData + nullAttributesIndicatorActualSize + offset, columnNameLength);
        columnName[columnNameLength] = 0;
        attribute.name = string(columnName);
        free(columnName);
        offset += columnNameLength;

        int columnType = *(int *)((char *)returnedData + offset + nullAttributesIndicatorActualSize);
        attribute.type = (AttrType)columnType;
        offset += sizeof(int);

        int columnLength = *(int *)((char *)returnedData + offset + nullAttributesIndicatorActualSize);
        attribute.length = columnLength;
        offset += sizeof(int);

        int columnPositon = *(int *)((char *)returnedData + offset + nullAttributesIndicatorActualSize);
        positionAttributeMap[columnPositon] = attribute;
        offset += sizeof(int);
    }
    for (int i = 1; i <= positionAttributeMap.size(); i++) {
        map<int,Attribute>::iterator it = positionAttributeMap.find(i);
        if (it == positionAttributeMap.end()) {
            return FAIL;
        } else {
            attrs.push_back(positionAttributeMap[i]);
        }
    }

    rm_scanIterator.close();
    free(returnedData);
    return SUCCESS;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid) {
    FileHandle fileHandle;
    vector<Attribute> recordDescriptor;

    if (rbfm->openFile(tableName, fileHandle) == FAIL) {
        return FAIL;
    }

    if (tableName == TABLES_TABLE) {
        recordDescriptor = getRecordDescriptorForTablesTable();
    } else if (tableName == COLUMNS_TABLE) {
        recordDescriptor = getRecordDescriptorForColumnsTable();
    } else {
        //TODO:
    }

    if (rbfm->insertRecord(fileHandle, recordDescriptor, data, rid) == FAIL) {
        return FAIL;
    }

    // print inserted record for debugging
    void *dataToBeRead = malloc(200);
    rbfm->readRecord(fileHandle, recordDescriptor, rid, dataToBeRead);
    rbfm->printRecord(recordDescriptor, dataToBeRead);
    free(dataToBeRead);

    rbfm->closeFile(fileHandle);

    return SUCCESS;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid) {
    return -1;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid) {
    return -1;
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
    return -1;
}

RC RelationManager::scan(const string &tableName,
                         const string &conditionAttribute,
                         const CompOp compOp,
                         const void *value,
                         const vector<string> &attributeNames,
                         RM_ScanIterator &rm_ScanIterator) {
    FileHandle fileHandle;
    vector<Attribute> recordDescriptor;
    RBFM_ScanIterator rbfm_scanIterator;
    RM_ScanIterator rm_scanIterator;

    if (rbfm->openFile(tableName, fileHandle) == FAIL) { return FAIL; }

    if (tableName == COLUMNS_TABLE) {
        recordDescriptor = getRecordDescriptorForColumnsTable();
    } else if (tableName == TABLES_TABLE) {
        recordDescriptor = getRecordDescriptorForTablesTable();
    } else {
        getAttributes(tableName, recordDescriptor);  // here recordDescriptor will be update within the fucntion
    }
    rbfm->scan(fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames, rbfm_scanIterator);
    
    // For Debugging: check if the rbfm_scanIterator works WE::
    RID rid;
    void *data = malloc(200);
    if (rbfm_scanIterator.getNextRecord(rid, data) == RM_EOF){
        return FAIL;
    } 
        
        
    rm_scanIterator.setRbfmScanIterator(rbfm_scanIterator);

    rbfm->closeFile(fileHandle);

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
void RelationManager::initializeTablesTable() {
    RID rid;
    void *tuple = malloc(PAGE_SIZE);

    prepareTupleForTables(TABLES_ATTR_NUM, TABLES_ID, TABLES_TABLE, true, tuple);
    insertTuple(TABLES_TABLE, tuple, rid);

    prepareTupleForTables(TABLES_ATTR_NUM, COLUMNS_ID, COLUMNS_TABLE, true, tuple);
    insertTuple(TABLES_TABLE, tuple, rid);
}

void RelationManager::initializeColumnsTable() {
    RID rid;
    void *tuple = malloc(PAGE_SIZE);

    prepareTupleForColumns(COLUMNS_ATTR_NUM, TABLES_ID, TABLE_ID, TypeInt, 4, 1, true, tuple);
    insertTuple(COLUMNS_TABLE, tuple, rid);
    prepareTupleForColumns(COLUMNS_ATTR_NUM, TABLES_ID, TABLE_NAME, TypeVarChar, 50, 2, true, tuple);
    insertTuple(COLUMNS_TABLE, tuple, rid);
    prepareTupleForColumns(COLUMNS_ATTR_NUM, TABLES_ID, FILE_NAME, TypeVarChar, 50, 3, true, tuple);
    insertTuple(COLUMNS_TABLE, tuple, rid);
    prepareTupleForColumns(COLUMNS_ATTR_NUM, TABLES_ID, SYSTEM_FLAG, TypeInt, 4, 4, true, tuple);
    insertTuple(COLUMNS_TABLE, tuple, rid);

    prepareTupleForColumns(COLUMNS_ATTR_NUM, COLUMNS_ID, TABLE_ID, TypeInt, 4, 1, true, tuple);
    insertTuple(COLUMNS_TABLE, tuple, rid);
    prepareTupleForColumns(COLUMNS_ATTR_NUM, COLUMNS_ID, COLUMN_NAME, TypeVarChar, 50, 2, true, tuple);
    insertTuple(COLUMNS_TABLE, tuple, rid);
    prepareTupleForColumns(COLUMNS_ATTR_NUM, COLUMNS_ID, COLUMN_TYPE, TypeInt, 4, 3, true, tuple);
    insertTuple(COLUMNS_TABLE, tuple, rid);
    prepareTupleForColumns(COLUMNS_ATTR_NUM, COLUMNS_ID, COLUMN_LENGTH, TypeInt, 4, 4, true, tuple);
    insertTuple(COLUMNS_TABLE, tuple, rid);
    prepareTupleForColumns(COLUMNS_ATTR_NUM, COLUMNS_ID, COLUMN_POSITION, TypeInt, 4, 5, true, tuple);
    insertTuple(COLUMNS_TABLE, tuple, rid);
    prepareTupleForColumns(COLUMNS_ATTR_NUM, COLUMNS_ID, SYSTEM_FLAG, TypeInt, 4, 6, true, tuple);
    insertTuple(COLUMNS_TABLE, tuple, rid);
}

/** private functions called by insertTuple(...) **/
vector<Attribute> RelationManager::getRecordDescriptorForTablesTable() {
    vector<Attribute> recordDescriptor;
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

    return recordDescriptor;
}

vector<Attribute> RelationManager::getRecordDescriptorForColumnsTable() {
    vector<Attribute> recordDescriptor;
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

    return recordDescriptor;
}

