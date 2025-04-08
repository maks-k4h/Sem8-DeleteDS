// catalog.hpp

#include <string>
#include <vector>
#include <unordered_map>
#include <utility>
#include <cstdint>


// Column specification
struct ColumnMetadata {
    std::string name;
    std::string type;  // "int", "float", "string", etc.
};

// Logical schema definition for a table
struct TableSchema {
    std::string name;
    std::vector<ColumnMetadata> columns;
};

// Reference to a specific record (row) in a table
struct RecordReference {
    std::string key_value;
};

// Represents the owner side of a relationship
struct RelationshipOwner {
    std::string table_name;
    std::vector<std::pair<RecordReference, std::vector<RecordReference>>> links;
};

// Represents the member side of a relationship
struct RelationshipMember {
    std::string table_name;
    std::vector<std::vector<RecordReference>> record_groups;
};

// Complete definition of a relationship between tables
struct RelationshipMetadata {
    std::string name;
    RelationshipOwner owner;
    RelationshipMember member;
};

// Catalog of all table schemas and their physical implementations
struct SchemaCatalog {
    std::vector<TableSchema> schemas;
};

// Top-level system catalog containing all metadata
struct SystemCatalog {
    SchemaCatalog schema_catalog;
    std::vector<RelationshipMetadata> relationships;

    // Core operations
    void add_schema(const TableSchema& schema);
    void add_relationship(const RelationshipMetadata& relationship);
};


using FieldValue = std::string;

class TableRecord {
public:
    void set_field(const std::string& name, FieldValue value);
    FieldValue get_field(const std::string& name) const;
    
private:
    std::unordered_map<std::string, FieldValue> fields_;
};


// --- Physical Data Storage ---
class DataTable {
public:
    void insert_record(const TableRecord& record);
    size_t size() const { return records_.size(); }
    const TableRecord& get_record(size_t index) const { return records_[index]; }
    
private:
    std::vector<TableRecord> records_;
};


// Add to catalog.hpp
class Database {
public:
    // Constructor now takes metadata reference
    Database(const SystemCatalog& catalog) : catalog_(catalog) {}

    void save_to_file(const std::string& filename) const;
    static Database load_from_file(const std::string& filename);
    
    void set_table(const std::string& table_name, const DataTable& table) {
        tables_[table_name] = table;
    }

    void print() const;
    
private:
    SystemCatalog catalog_;
    std::unordered_map<std::string, DataTable> tables_;
};
