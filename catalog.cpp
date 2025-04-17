// catalog.cpp
#include <iostream>
#include <utility>
#include <fstream>
#include <sstream>

#include "catalog.hpp"


// --- SystemCatalog Implementation ---
void SystemCatalog::add_schema(const TableSchema& schema) {
    schema_catalog.schemas.push_back(schema);
}

void SystemCatalog::add_relationship(const RelationshipMetadata& relationship) {
    relationships.push_back(relationship);
}

// --- TableRecord Implementation ---
void TableRecord::set_field(const std::string& name, FieldValue value) {
    fields_[name] = std::move(value);
}

FieldValue TableRecord::get_field(const std::string& name) const {
    return fields_.at(name);
}

void DataTable::insert_record(const TableRecord& record) {
    records_.push_back(record);
}

void Database::print() const {
    for (const auto& [table_name, data_table] : tables_) {
        // Find the schema for the current table
        const TableSchema* schema = nullptr;
        for (const auto& s : catalog_.schema_catalog.schemas) {
            if (s.name == table_name) {
                schema = &s;
                break;
            }
        }

        if (!schema) {
            std::cerr << "Error: Schema not found for table '" << table_name << "'\n";
            continue;
        }

        // Print table header
        std::cout << "=== " << table_name << " ===\n";
        
        // Print columns
        std::cout << "Columns:\n";
        for (const auto& col : schema->columns) {
            std::cout << "  " << col.name << " (" << col.type << ")\n";
        }

        // Print records
        std::cout << "\nRecords (" << data_table.size() << " entries):\n";
        // Header
        for (const auto& col : schema->columns) {
            std::cout << col.name << "\t";
        }
        std::cout << "\n---------------------------------\n";
        
        // Data rows
        for (size_t i = 0; i < data_table.size(); ++i) {
            const auto& record = data_table.get_record(i);
            for (const auto& col : schema->columns) {
                try {
                    std::cout << record.get_field(col.name) << "\t";
                } catch (const std::out_of_range&) {
                    std::cout << "<missing>\t";
                }
            }
            std::cout << "\n";
        }
        std::cout << "\n\n";
    }
}


void Database::save_to_file(const std::string& filename) const {
    std::ofstream file(filename);
    if (!file) throw std::runtime_error("Cannot open file");

    // Save schemas
    for (const auto& schema : catalog_.schema_catalog.schemas) {
        file << "[SCHEMA:" << schema.name << "]\n";
        for (const auto& col : schema.columns) {
            file << col.name << ":" << col.type << "\n";
        }
    }

    // Save relationships
    for (const auto& rel : catalog_.relationships) {
        file << "[RELATIONSHIP:" << rel.name << "]\n"
             << "Owner:" << rel.owner.table_name << "\n"
             << "Member:" << rel.member.table_name << "\n";
        for (const auto& [owner_ref, member_refs] : rel.owner.links) {
            file << "Link:" << owner_ref.key_value << ":";
            for (size_t i = 0; i < member_refs.size(); ++i) {
                const auto& ref = member_refs[i];
                file << ref.key_value;
                if (i != member_refs.size()-1) file << ",";
            }
            file << "\n";
        }
    }

    // Save data tables
    for (const auto& [tbl_name, tbl] : tables_) {
        file << "[DATA:" << tbl_name << "]\n";
        // Find schema for headers
        const TableSchema* schema = nullptr;
        for (const auto& s : catalog_.schema_catalog.schemas) {
            if (s.name == tbl_name) {
                schema = &s;
                break;
            }
        }
        if (!schema) throw std::runtime_error("Missing schema for table");

        // Write header
        for (const auto& col : schema->columns)
            file << col.name << "\t";
        file << "\n";

        // Write records
        for (size_t i = 0; i < tbl.size(); ++i) {
            const auto& rec = tbl.get_record(i);
            for (const auto& col : schema->columns) {
                file << rec.get_field(col.name) << "\t";
            }
            file << "\n";
        }
    }
}


Database Database::load_from_file(const std::string& filename) {
    std::ifstream file(filename);
    if (!file) throw std::runtime_error("Cannot open file");

    SystemCatalog catalog;
    std::unordered_map<std::string, DataTable> tables;
    std::string line;
    std::string current_section;
    std::unique_ptr<TableSchema> current_schema;
    std::unique_ptr<RelationshipMetadata> current_rel;
    
    // Data parsing state
    std::string current_data_table;
    bool data_header_processed = false;

    while (getline(file, line)) {
        if (line.empty()) continue;

        // Section handling
        if (line[0] == '[') {
            // Finalize current section
            if (current_schema) {
                catalog.add_schema(*current_schema);
                current_schema.reset();
            }
            if (current_rel) {
                catalog.add_relationship(*current_rel);
                current_rel.reset();
            }
            
            current_section = line;
            current_data_table.clear();
            data_header_processed = false;

            // New schema section
            if (current_section.starts_with("[SCHEMA:")) {
                current_schema = std::make_unique<TableSchema>();
                current_schema->name = current_section.substr(8, current_section.size()-9);
            }
            // New relationship section
            else if (current_section.starts_with("[RELATIONSHIP:")) {
                current_rel = std::make_unique<RelationshipMetadata>();
                current_rel->name = current_section.substr(14, current_section.size()-15);
            }
            // New data section
            else if (current_section.starts_with("[DATA:")) {
                current_data_table = current_section.substr(6, current_section.size()-7);
                tables.emplace(current_data_table, DataTable());
            }
            continue;
        }

        // Schema parsing
        if (current_schema) {
            size_t colon = line.find(':');
            if (colon != std::string::npos) {
                current_schema->columns.push_back({
                    line.substr(0, colon),
                    line.substr(colon+1)
                });
            }
        }
        // Relationship parsing
        else if (current_rel) {
            if (line.starts_with("Owner:")) {
                current_rel->owner.table_name = line.substr(6);
            }
            else if (line.starts_with("Member:")) {
                current_rel->member.table_name = line.substr(7);
            }
            else if (line.starts_with("Link:")) {
                std::string link_data = line.substr(5);
                size_t colon = link_data.find(':');
                RecordReference owner_ref{link_data.substr(0, colon)};
                
                std::vector<RecordReference> member_refs;
                std::istringstream iss(link_data.substr(colon+1));
                std::string ref;
                while (getline(iss, ref, ',')) {
                    member_refs.push_back({ref});
                }
                
                current_rel->owner.links.emplace_back(owner_ref, member_refs);
            }
        }
        // Data table parsing
        else if (!current_data_table.empty()) {
            DataTable& tbl = tables[current_data_table];
            
            if (!data_header_processed) {
                // Skip header line
                data_header_processed = true;
                continue;
            }

            // Find matching schema
            const TableSchema* schema = nullptr;
            for (const auto& s : catalog.schema_catalog.schemas) {
                if (s.name == current_data_table) {
                    schema = &s;
                    break;
                }
            }
            if (!schema) throw std::runtime_error("Missing schema for table: " + current_data_table);

            // Parse record
            TableRecord rec;
            std::istringstream iss(line);
            std::string value;
            
            for (const auto& col : schema->columns) {
                if (!getline(iss, value, '\t')) {
                    throw std::runtime_error("Malformed data row in table: " + current_data_table);
                }
                rec.set_field(col.name, value);
            }
            tbl.insert_record(rec);
        }
    }

    // Add any remaining sections
    if (current_schema) catalog.add_schema(*current_schema);
    if (current_rel) catalog.add_relationship(*current_rel);

    // Build database
    Database db(std::move(catalog));
    for (auto& [name, tbl] : tables) {
        db.set_table(name, tbl);
    }
    return db;
}

void Database::DeleteDS(const std::string& table_name) {
    if (tables_.find(table_name) == tables_.end()) {
        std::cerr << "Error: Table '" << table_name << "' not found in database.\n";
        throw std::runtime_error("Table not found");
    }

    // 1. Remove schema from catalog
    auto& schemas = catalog_.schema_catalog.schemas;
    schemas.erase(
        std::remove_if(schemas.begin(), schemas.end(),
            [&](const TableSchema& s) { return s.name == table_name; }),
        schemas.end()
    );

    // 2. Remove related relationships
    auto& relationships = catalog_.relationships;
    relationships.erase(
        std::remove_if(relationships.begin(), relationships.end(),
            [&](const RelationshipMetadata& rel) {
                return rel.owner.table_name == table_name || 
                       rel.member.table_name == table_name;
            }),
        relationships.end()
    );

    // 3. Remove physical data
    tables_.erase(table_name);
}